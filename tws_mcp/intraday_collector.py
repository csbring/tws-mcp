"""Durable, read-only TWS five-minute bar collector.

The worker runs independently of the MCP request and checkpoints each weekly
window. It writes local CSV/audit files only and connects with readonly=True.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, time, timedelta, timezone
import json
import math
import os
from pathlib import Path
import time as time_module
from zoneinfo import ZoneInfo

from ib_insync import IB, Stock


NEW_YORK = ZoneInfo("America/New_York")
UTC = timezone.utc
FIELDS = ("time", "open", "high", "low", "close", "volume", "wap", "count")
# IBKR SMART has no TLT five-minute history before 2016-02-03. The weekly
# window beginning 2016-01-29 straddles that boundary, so use the available
# ARCA history through that whole window and record the source in the audit.
TLT_ARCA_WINDOW_CUTOFF = date(2016, 2, 5)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def weekly_windows(start: date, end: date):
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=6), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def bar_time(value) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        # TWS formatDate=2 supplies UTC epochs; naive values from compatible
        # ib_insync versions are treated as UTC and recorded as such.
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
        stamp = bar_time(bar.date)
        local = stamp.astimezone(NEW_YORK)
        if not (start <= local.date() <= end):
            continue
        if local.weekday() >= 5 or not (time(9, 30) <= local.time().replace(tzinfo=None) <= time(15, 55)):
            continue
        if local.minute % 5 or local.second or local.microsecond:
            continue
        row = {
            "time": stamp.isoformat(timespec="seconds"),
            "open": finite(bar.open, positive=True),
            "high": finite(bar.high, positive=True),
            "low": finite(bar.low, positive=True),
            "close": finite(bar.close, positive=True),
            "volume": finite(bar.volume),
            "wap": finite(bar.average, positive=True),
            "count": finite(bar.barCount),
        }
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
        return {row["time"]: row for row in csv.DictReader(handle) if row.get("time")}


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
    arca_contract = None
    if symbol == "TLT" and date.fromisoformat(job["start_date"]) < TLT_ARCA_WINDOW_CUTOFF:
        arca_qualified = ib.qualifyContracts(Stock(symbol, "ARCA", "USD"))
        if len(arca_qualified) != 1 or arca_qualified[0].conId != qualified[0].conId:
            raise RuntimeError("Could not qualify TLT ARCA history to the same contract ID.")
        arca_contract = arca_qualified[0]
    start, end = date.fromisoformat(job["start_date"]), date.fromisoformat(job["end_date"])
    for window_start, window_end in weekly_windows(start, end):
        label = f"{window_start}:{window_end}"
        if label in completed:
            continue
        request_contract = (arca_contract if arca_contract is not None
                            and window_start < TLT_ARCA_WINDOW_CUTOFF else qualified[0])
        response = None
        last_error = None
        for attempt in range(1, 4):
            try:
                end_time = datetime.combine(window_end, time(23, 59, 59), NEW_YORK)
                response = ib.reqHistoricalData(
                    request_contract, endDateTime=end_time.strftime("%Y%m%d %H:%M:%S US/Eastern"),
                    durationStr="1 W", barSizeSetting="5 mins", whatToShow="TRADES",
                    useRTH=True, formatDate=2, keepUpToDate=False, timeout=90,
                )
                if response:
                    break
                last_error = "TWS returned no bars"
            except Exception as exc:
                last_error = str(exc)
            time_module.sleep(float(job["pace_seconds"]) * attempt)
        if not response:
            raise RuntimeError(f"{symbol} {label}: {last_error or 'no data'}")
        cleaned = clean_bars(response, window_start, window_end)
        if not cleaned:
            raise RuntimeError(f"{symbol} {label}: no valid five-minute TRADES bars")
        rows.update({row["time"]: row for row in cleaned})
        write_rows(output, rows)
        window_audit = {
            "raw_rows": len(response), "clean_rows": len(cleaned),
            "first_time_utc": min(row["time"] for row in cleaned),
            "last_time_utc": max(row["time"] for row in cleaned),
            "source_exchange": request_contract.exchange,
            "source_con_id": request_contract.conId,
        }
        refreshed = load_job(job_path)
        completed = set(refreshed.get("completed_windows", {}).get(symbol, []))
        completed.add(label)
        refreshed.setdefault("completed_windows", {})[symbol] = sorted(completed)
        refreshed.setdefault("window_audits", {}).setdefault(symbol, {})[label] = window_audit
        refreshed.setdefault("rows", {})[symbol] = len(rows)
        refreshed["current_symbol"] = symbol
        refreshed["current_window"] = label
        refreshed["status"] = "running"
        refreshed["updated_at"] = now_iso()
        atomic_json(job_path, refreshed)
        time_module.sleep(float(job["pace_seconds"]))


def run(job_path: Path) -> None:
    job = update_job(job_path, status="running", pid=os.getpid(), started_at=now_iso())
    ib = IB()
    try:
        ib.connect(job["host"], int(job["port"]), clientId=int(job["client_id"]),
                   readonly=True, timeout=30)
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
