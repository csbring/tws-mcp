"""Durable, read-only TWS daily-bar collection worker.

The worker is started by the MCP server but runs independently so a collection
can survive an MCP client request ending.  It only writes local CSV/audit files
and connects to TWS with ``readonly=True``.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta
import json
import math
import os
from pathlib import Path
import time
from zoneinfo import ZoneInfo

from ib_insync import IB, Stock


NEW_YORK = ZoneInfo("America/New_York")
FIELDS = ("date", "open", "high", "low", "close", "volume", "wap", "count")


def now_iso() -> str:
    return datetime.now(ZoneInfo("UTC")).isoformat()


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def load_job(path: Path) -> dict:
    return json.loads(path.read_text())


def update_job(path: Path, **changes) -> dict:
    job = load_job(path)
    job.update(changes)
    job["updated_at"] = now_iso()
    atomic_json(path, job)
    return job


def annual_windows(start: date, end: date):
    cursor = start
    while cursor <= end:
        try:
            next_year = cursor.replace(year=cursor.year + 1)
        except ValueError:  # Feb. 29
            next_year = cursor.replace(year=cursor.year + 1, day=28)
        window_end = min(next_year - timedelta(days=1), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def bar_date(value) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return datetime.fromisoformat(text).date().isoformat()


def finite(value, *, positive=False):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or (positive and number <= 0):
        return None
    return number


def clean_bars(bars, start: date, end: date) -> list[dict]:
    rows = []
    for bar in bars:
        row = {
            "date": bar_date(bar.date),
            "open": finite(bar.open, positive=True),
            "high": finite(bar.high, positive=True),
            "low": finite(bar.low, positive=True),
            "close": finite(bar.close, positive=True),
            "volume": finite(bar.volume),
            "wap": finite(bar.average, positive=True),
            "count": finite(bar.barCount),
        }
        current = date.fromisoformat(row["date"])
        if not (start <= current <= end):
            continue
        if any(row[key] is None for key in ("open", "high", "low", "close", "volume")):
            continue
        if row["high"] < max(row["open"], row["low"], row["close"]):
            continue
        if row["low"] > min(row["open"], row["high"], row["close"]) or row["volume"] < 0:
            continue
        rows.append(row)
    return rows


def read_rows(path: Path) -> dict[str, dict]:
    if not path.is_file():
        return {}
    with path.open(newline="") as handle:
        return {row["date"]: row for row in csv.DictReader(handle) if row.get("date")}


def write_rows(path: Path, rows: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for key in sorted(rows):
            writer.writerow({field: rows[key].get(field) for field in FIELDS})
    os.replace(temporary, path)


def collect_symbol(ib: IB, job_path: Path, job: dict, symbol: str) -> None:
    root = Path(job["data_root"])
    output = root / f"{symbol}.csv"
    rows = read_rows(output)
    completed = set(job.get("completed_windows", {}).get(symbol, []))
    contract = Stock(symbol, "SMART", "USD")
    qualified = ib.qualifyContracts(contract)
    if len(qualified) != 1 or not qualified[0].conId:
        raise RuntimeError(f"Could not uniquely qualify {symbol}.")
    start, end = date.fromisoformat(job["start_date"]), date.fromisoformat(job["end_date"])
    for window_start, window_end in annual_windows(start, end):
        label = f"{window_start}:{window_end}"
        if label in completed:
            continue
        response = None
        last_error = None
        for attempt in range(1, 4):
            try:
                response = ib.reqHistoricalData(
                    qualified[0],
                    endDateTime=datetime.combine(window_end, datetime.max.time(), NEW_YORK).strftime(
                        "%Y%m%d %H:%M:%S US/Eastern"),
                    durationStr="1 Y", barSizeSetting="1 day", whatToShow="TRADES",
                    useRTH=True, formatDate=2, keepUpToDate=False, timeout=60,
                )
                if response:
                    break
                last_error = "TWS returned no bars"
            except Exception as exc:  # The audit stores the broker error for later diagnosis.
                last_error = str(exc)
            time.sleep(float(job["pace_seconds"]) * attempt)
        if not response:
            raise RuntimeError(f"{symbol} {label}: {last_error or 'no data'}")
        cleaned = clean_bars(response, window_start, window_end)
        if not cleaned:
            raise RuntimeError(f"{symbol} {label}: no valid daily bars in requested range")
        rows.update({row["date"]: row for row in cleaned})
        write_rows(output, rows)
        refreshed = load_job(job_path)
        completed = set(refreshed.get("completed_windows", {}).get(symbol, []))
        completed.add(label)
        refreshed.setdefault("completed_windows", {})[symbol] = sorted(completed)
        refreshed.setdefault("rows", {})[symbol] = len(rows)
        refreshed["current_symbol"] = symbol
        refreshed["current_window"] = label
        refreshed["status"] = "running"
        refreshed["updated_at"] = now_iso()
        atomic_json(job_path, refreshed)
        time.sleep(float(job["pace_seconds"]))


def run(job_path: Path) -> None:
    job = update_job(job_path, status="running", pid=os.getpid(), started_at=now_iso())
    ib = IB()
    try:
        ib.connect(job["host"], int(job["port"]), clientId=int(job["client_id"]), readonly=True, timeout=20)
        if not ib.isConnected():
            raise RuntimeError("Unable to connect to TWS")
        for symbol in job["symbols"]:
            collect_symbol(ib, job_path, load_job(job_path), symbol)
        update_job(job_path, status="completed", completed_at=now_iso(), current_symbol=None,
                   current_window=None)
    except Exception as exc:
        update_job(job_path, status="failed", error=str(exc), failed_at=now_iso())
        raise
    finally:
        ib.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", required=True, type=Path)
    args = parser.parse_args()
    run(args.job)


if __name__ == "__main__":
    main()
