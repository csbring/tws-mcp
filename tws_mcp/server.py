"""Read-only TWS API and saved collector data over MCP stdio."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, closing
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo

from ib_insync import IB, Option, Stock
from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field, field_validator

ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = (Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share"))
             / "tws-mcp").expanduser().resolve()
DATABASE = Path(os.environ.get("TWS_OPTIONS_DATABASE", DATA_ROOT / "options_quotes.sqlite")).expanduser().resolve()
HISTORY_ROOT = Path(os.environ.get("TWS_HISTORY_ROOT", DATA_ROOT / "historical_data")).expanduser().resolve()
DEFAULT_HISTORY_SYMBOLS = ("GLD", "UUP", "TLT")
MarketData = Literal["realtime", "frozen", "delayed", "delayed_frozen"]
DATA_TYPES = {"realtime": 1, "frozen": 2, "delayed": 3, "delayed_frozen": 4}
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                            idempotentHint=True, openWorldHint=True)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def symbol_name(value: str):
    value = value.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9. -]{0,14}", value):
        raise ValueError("Use a US stock/ETF ticker, for example SPY or BRK B.")
    return value


def number(value):
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and abs(value) < 1e100 else None


def price(value):
    value = number(value)
    return value if value is not None and value > 0 else None


def contract_data(contract):
    return {"con_id": contract.conId, "symbol": contract.symbol,
            "local_symbol": contract.localSymbol, "security_type": contract.secType,
            "exchange": contract.exchange, "currency": contract.currency,
            "expiry": contract.lastTradeDateOrContractMonth or None,
            "strike": number(contract.strike), "right": contract.right or None,
            "multiplier": contract.multiplier or None,
            "trading_class": contract.tradingClass or None}


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def history_job_path(job_id: str) -> Path:
    if not re.fullmatch(r"[a-f0-9]{16}", job_id):
        raise ValueError("Invalid historical collection job ID.")
    return HISTORY_ROOT / "jobs" / f"{job_id}.json"


def history_job(job_id: str) -> dict[str, Any]:
    path = history_job_path(job_id)
    if not path.is_file():
        raise ValueError("Historical collection job not found.")
    return json.loads(path.read_text())


def history_job_active(job: dict[str, Any]) -> bool:
    if job.get("status") not in {"queued", "running"}:
        return False
    pid = job.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return job.get("status") == "queued"
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def years_before(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:  # Feb. 29
        return value.replace(year=value.year - years, day=28)


def collection_jobs() -> list[dict[str, Any]]:
    directory = HISTORY_ROOT / "jobs"
    if not directory.is_dir():
        return []
    jobs = []
    for path in directory.glob("*.json"):
        try:
            jobs.append(json.loads(path.read_text()))
        except (OSError, json.JSONDecodeError):
            continue
    return jobs


def launch_history_worker(path: Path, job: dict[str, Any]) -> int:
    process = subprocess.Popen(
        [sys.executable, "-m", "tws_mcp.history_collector", "--job", str(path)],
        cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    # The worker replaces this with the same PID when it reaches its running state.
    job["pid"] = process.pid
    atomic_json(path, job)
    return process.pid


def launch_intraday_worker(path: Path, job: dict[str, Any]) -> int:
    process = subprocess.Popen(
        [sys.executable, "-m", "tws_mcp.intraday_collector", "--job", str(path)],
        cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    job["pid"] = process.pid
    atomic_json(path, job)
    return process.pid


def quote_data(ticker):
    bid, ask = price(ticker.bid), price(ticker.ask)
    two_sided = bid is not None and ask is not None and ask >= bid
    mid = (bid + ask) / 2 if two_sided else None
    return {
        "contract": contract_data(ticker.contract),
        "received_at": ticker.time.isoformat() if ticker.time else None,
        "timestamp_note": "Callback receipt time, not necessarily the last trade time.",
        "market_data_type": next((k for k, v in DATA_TYPES.items()
                                   if v == ticker.marketDataType), "unknown"),
        "bid": bid, "ask": ask, "bid_size": number(ticker.bidSize),
        "ask_size": number(ticker.askSize), "last": price(ticker.last),
        "previous_close": price(ticker.close), "mid": mid,
        "spread_pct": (ask - bid) / mid if mid else None,
        "two_sided_quote": two_sided,
        "volume_cumulative": number(ticker.volume),
        "call_open_interest": number(ticker.callOpenInterest),
        "put_open_interest": number(ticker.putOpenInterest),
        "model_greeks": {k: number(v) for k, v in ticker.modelGreeks._asdict().items()}
        if ticker.modelGreeks else None,
    }


class OptionSpec(BaseModel):
    symbol: str
    expiry: str = Field(description="Expiration as YYYY-MM-DD or YYYYMMDD.")
    strike: float = Field(gt=0, allow_inf_nan=False)
    right: Literal["C", "P"]
    trading_class: str = ""
    multiplier: str = "100"

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value):
        return symbol_name(value)

    @field_validator("expiry")
    @classmethod
    def validate_expiry(cls, value):
        return datetime.strptime(value.replace("-", ""), "%Y%m%d").strftime("%Y%m%d")

    def contract(self):
        return Option(self.symbol, self.expiry, self.strike, self.right, "SMART",
                      currency="USD", tradingClass=self.trading_class,
                      multiplier=self.multiplier)


class TwsBackend:
    def __init__(self):
        self.ib = IB()
        self.host = os.environ.get("TWS_API_HOST", "127.0.0.1")
        self.port = int(os.environ.get("TWS_API_PORT", "7496"))
        self.client_id = int(os.environ.get("TWS_MCP_CLIENT_ID", "91"))
        if self.client_id <= 0:
            raise ValueError("TWS_MCP_CLIENT_ID must be positive; client 0 can bind orders.")
        self.lock = asyncio.Lock()

    @asynccontextmanager
    async def session(self):
        async with self.lock:
            try:
                async with asyncio.timeout(45):
                    if not self.ib.isConnected():
                        await self.ib.connectAsync(self.host, self.port, clientId=self.client_id,
                                                   timeout=5, readonly=True)
                    yield self.ib
            except (TimeoutError, ConnectionError, OSError) as exc:
                self.ib.disconnect()
                raise RuntimeError(
                    f"TWS request failed ({type(exc).__name__}). Check login, API socket "
                    f"{self.host}:{self.port}, and whether client ID {self.client_id} is in use.") from None

    async def qualify(self, ib, contracts):
        qualified = await ib.qualifyContractsAsync(*contracts)
        if len(qualified) != len(contracts) or any(not c.conId for c in contracts):
            raise ValueError("A contract is unknown or ambiguous. Check ticker, expiry, strike, and trading class.")
        return qualified

    async def quotes(self, contracts, market_data, wait_seconds):
        async with self.session() as ib:
            contracts = await self.qualify(ib, contracts)
            contracts = list({c.conId: c for c in contracts}.values())
            ib.reqMarketDataType(DATA_TYPES[market_data])
            tickers, subscribed, errors = [], [], []

            def on_error(req_id, code, message, contract):
                if code not in (2104, 2106, 2107, 2108, 2158):
                    errors.append({"code": code, "message": message})

            ib.errorEvent += on_error
            try:
                for contract in contracts:
                    tickers.append(ib.reqMktData(
                        contract, genericTickList="100,101,106" if contract.secType == "OPT" else "",
                        snapshot=False, regulatorySnapshot=False))
                    subscribed.append(contract)
                await asyncio.sleep(wait_seconds)
                return {"retrieved_at": now_iso(), "requested_market_data": market_data,
                        "quotes": [quote_data(t) for t in tickers], "ibkr_messages": errors,
                        "note": "Missing quotes remain null; previous close is not an executable price. "
                                "A realtime request does not guarantee a fresh quote outside market hours."}
            finally:
                for contract in subscribed:
                    ib.cancelMktData(contract)
                ib.errorEvent -= on_error


@asynccontextmanager
async def lifespan(server):
    backend = TwsBackend()
    try:
        yield {"backend": backend}
    finally:
        backend.ib.disconnect()


mcp = FastMCP(
    "tws", lifespan=lifespan, log_level="WARNING",
    instructions="Read-only tools for the user's local TWS API and saved option snapshots. "
    "Inspect quote timestamps, market-data type, spreads, and missing fields. "
    "No order placement, order cancellation, account transfers, or login credentials are exposed.")


def backend(ctx: Context) -> TwsBackend:
    return ctx.request_context.lifespan_context["backend"]


@mcp.tool(annotations=READ_ONLY)
async def tws_status(ctx: Context) -> dict[str, Any]:
    """Check TWS connectivity and broker server time without reading credentials."""
    client = backend(ctx)
    result = {"host": client.host, "port": client.port, "client_id": client.client_id,
              "read_only_tools": True, "checked_at": now_iso()}
    try:
        async with client.session() as ib:
            result.update(connected=True, server_time=(await ib.reqCurrentTimeAsync()).isoformat(),
                          account_count=len(ib.managedAccounts()))
    except RuntimeError as exc:
        result.update(connected=False, error=str(exc))
    return result


@mcp.tool(annotations=READ_ONLY)
async def tws_stock_quotes(
    ctx: Context, symbols: Annotated[list[str], Field(min_length=1, max_length=20)],
    market_data: MarketData = "realtime",
    wait_seconds: Annotated[int, Field(ge=1, le=10)] = 3,
) -> dict[str, Any]:
    """Get bid/ask, last, previous close, and daily volume for up to 20 US stocks/ETFs."""
    contracts = [Stock(symbol_name(s), "SMART", "USD") for s in dict.fromkeys(symbols)]
    return await backend(ctx).quotes(contracts, market_data, wait_seconds)


@mcp.tool(annotations=READ_ONLY)
async def tws_option_chain(ctx: Context, symbol: str) -> dict[str, Any]:
    """List SMART option-chain expirations, strikes, trading classes, and multipliers.

    Not every strike/expiry combination is valid; option quote requests qualify contracts.
    """
    symbol = symbol_name(symbol)
    client = backend(ctx)
    async with client.session() as ib:
        stock = (await client.qualify(ib, [Stock(symbol, "SMART", "USD")]))[0]
        chains = await ib.reqSecDefOptParamsAsync(symbol, "", "STK", stock.conId)
        return {"symbol": symbol, "retrieved_at": now_iso(), "chains": [
            {"exchange": c.exchange, "trading_class": c.tradingClass, "multiplier": c.multiplier,
             "expirations": sorted(c.expirations), "strikes": sorted(c.strikes)}
            for c in chains if c.exchange == "SMART"]}


@mcp.tool(annotations=READ_ONLY)
async def tws_option_quotes(
    ctx: Context, options: Annotated[list[OptionSpec], Field(min_length=1, max_length=20)],
    market_data: MarketData = "realtime",
    wait_seconds: Annotated[int, Field(ge=1, le=10)] = 3,
) -> dict[str, Any]:
    """Quote up to 20 specific options, including model Greeks and OI when IBKR supplies them.

    Volume is cumulative daily volume, not new flow. Missing OI/Greeks are not inferred.
    """
    return await backend(ctx).quotes([spec.contract() for spec in options], market_data, wait_seconds)


@mcp.tool(annotations=READ_ONLY)
async def tws_historical_bars(
    ctx: Context, symbol: str,
    duration_days: Annotated[int, Field(ge=1, le=5)] = 1,
    bar_size: Literal["1 min", "5 mins", "15 mins", "1 hour", "1 day"] = "1 min",
    regular_hours_only: bool = True, end_time: str = "",
    limit: Annotated[int, Field(ge=1, le=2000)] = 500,
) -> dict[str, Any]:
    """Get stock/ETF OHLCV history for up to 5 days. Intraday timestamps are UTC.

    end_time is empty for now or an ISO timestamp with timezone. Returns the latest
    limit bars, with truncation reported. History can be unavailable under IBKR permissions.
    """
    symbol = symbol_name(symbol)
    end = ""
    if end_time:
        end = datetime.fromisoformat(end_time)
        if end.tzinfo is None:
            raise ValueError("end_time must include a timezone, e.g. 2026-09-11T16:00:00-04:00.")
    client = backend(ctx)
    async with client.session() as ib:
        contract = (await client.qualify(ib, [Stock(symbol, "SMART", "USD")]))[0]
        bars = await ib.reqHistoricalDataAsync(
            contract, endDateTime=end, durationStr=f"{duration_days} D",
            barSizeSetting=bar_size, whatToShow="TRADES", useRTH=regular_hours_only,
            formatDate=2, keepUpToDate=False, timeout=30)
        return {"symbol": symbol, "retrieved_at": now_iso(), "bar_size": bar_size,
                "regular_hours_only": regular_hours_only, "total_returned_by_ibkr": len(bars),
                "truncated": len(bars) > limit,
                "note": "The newest bar can be incomplete. Empty history may indicate unavailable data.",
                "bars": [{"time": b.date.isoformat(), "open": number(b.open),
                          "high": number(b.high), "low": number(b.low), "close": number(b.close),
                          "volume": number(b.volume), "wap": number(b.average), "count": b.barCount}
                         for b in bars[-limit:]]}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False,
                                      idempotentHint=True, openWorldHint=False))
def tws_start_historical_collection(
    symbols: Annotated[list[str], Field(min_length=1, max_length=10)] = list(DEFAULT_HISTORY_SYMBOLS),
    start_date: str = "", end_date: str = "",
    pace_seconds: Annotated[float, Field(ge=1.0, le=30.0)] = 1.2,
) -> dict[str, Any]:
    """Start a durable, local daily-bar collection job.

    The worker fetches regular-hours daily TRADES bars in annual TWS requests,
    writes validated CSV files, and can resume completed windows after an error.
    It never changes orders or account state.
    """
    requested_symbols = tuple(dict.fromkeys(symbol_name(value) for value in symbols))
    today = datetime.now(ZoneInfo("America/New_York")).date()
    start = date.fromisoformat(start_date) if start_date else years_before(today, 5)
    end = date.fromisoformat(end_date) if end_date else today
    if start > end:
        raise ValueError("start_date must be on or before end_date.")
    configuration = {
        "symbols": list(requested_symbols), "start_date": start.isoformat(), "end_date": end.isoformat(),
        "bar_size": "1 day", "what_to_show": "TRADES", "regular_hours_only": True,
    }
    for prior in collection_jobs():
        if all(prior.get(key) == value for key, value in configuration.items()):
            prior_id = prior.get("job_id")
            if isinstance(prior_id, str):
                if history_job_active(prior) or prior.get("status") == "completed":
                    return {"job_id": prior_id, "status": prior.get("status"),
                            "active": history_job_active(prior), "reused": True,
                            "data_root": prior.get("data_root")}
                path = history_job_path(prior_id)
                prior["status"] = "queued"
                prior["pace_seconds"] = pace_seconds
                prior.pop("error", None)
                prior["updated_at"] = now_iso()
                atomic_json(path, prior)
                launch_history_worker(path, prior)
                return {"job_id": prior_id, "status": "queued", "active": True,
                        "reused": True, "resumed": True, "data_root": prior.get("data_root")}
    active = [job for job in collection_jobs() if history_job_active(job)]
    if active:
        raise RuntimeError("A historical collection job is already active. Check its status before starting another.")
    fingerprint = json.dumps(configuration, sort_keys=True).encode()
    job_id = hashlib.sha256(fingerprint).hexdigest()[:16]
    path = history_job_path(job_id)
    job = configuration | {
        "job_id": job_id, "status": "queued", "created_at": now_iso(), "updated_at": now_iso(),
        "host": os.environ.get("TWS_API_HOST", "127.0.0.1"),
        "port": int(os.environ.get("TWS_API_PORT", "7496")),
        "client_id": int(os.environ.get("TWS_HISTORICAL_COLLECTOR_CLIENT_ID", "93")),
        "pace_seconds": pace_seconds, "data_root": str(HISTORY_ROOT / "daily"),
        "completed_windows": {}, "rows": {}, "pid": None,
    }
    if job["client_id"] <= 0:
        raise ValueError("TWS_HISTORICAL_COLLECTOR_CLIENT_ID must be positive.")
    atomic_json(path, job)
    launch_history_worker(path, job)
    return {"job_id": job_id, "status": "queued", "active": True, "reused": False,
            "data_root": job["data_root"], "symbols": list(requested_symbols),
            "start_date": start.isoformat(), "end_date": end.isoformat()}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False,
                                      idempotentHint=True, openWorldHint=False))
def tws_start_intraday_collection(
    symbols: Annotated[list[str], Field(min_length=1, max_length=10)] = ["GLD", "UUP", "TLT"],
    start_date: str = "", end_date: str = "",
    pace_seconds: Annotated[float, Field(ge=1.0, le=30.0)] = 1.5,
) -> dict[str, Any]:
    """Start a durable, local five-minute RTH TRADES-bar collection job.

    The worker fetches one-week TWS requests sequentially, checkpoints each
    symbol/window, and writes validated CSVs under historical_data/intraday_5m.
    It writes local research data only and never changes account or order state.
    The end date defaults to yesterday so incomplete current-session bars are excluded.
    """
    requested_symbols = tuple(dict.fromkeys(symbol_name(value) for value in symbols))
    today = datetime.now(ZoneInfo("America/New_York")).date()
    start = date.fromisoformat(start_date) if start_date else years_before(today, 5)
    end = date.fromisoformat(end_date) if end_date else today - timedelta(days=1)
    if start > end:
        raise ValueError("start_date must be on or before end_date.")
    if end >= today:
        raise ValueError("end_date must be before today; incomplete current-session bars are excluded.")
    configuration = {
        "symbols": list(requested_symbols), "start_date": start.isoformat(), "end_date": end.isoformat(),
        "bar_size": "5 mins", "what_to_show": "TRADES", "regular_hours_only": True,
    }
    for prior in collection_jobs():
        if all(prior.get(key) == value for key, value in configuration.items()):
            prior_id = prior.get("job_id")
            if isinstance(prior_id, str) and (history_job_active(prior) or prior.get("status") == "completed"):
                return {"job_id": prior_id, "status": prior.get("status"),
                        "active": history_job_active(prior), "reused": True,
                        "data_root": prior.get("data_root")}
            if isinstance(prior_id, str):
                path = history_job_path(prior_id)
                prior.update(status="queued", pace_seconds=pace_seconds, updated_at=now_iso())
                prior.pop("error", None)
                atomic_json(path, prior)
                launch_intraday_worker(path, prior)
                return {"job_id": prior_id, "status": "queued", "active": True,
                        "reused": True, "resumed": True, "data_root": prior.get("data_root")}
    active = [job for job in collection_jobs() if history_job_active(job)]
    if active:
        raise RuntimeError("A historical collection job is already active. Check its status before starting another.")
    fingerprint = json.dumps(configuration, sort_keys=True).encode()
    job_id = hashlib.sha256(fingerprint).hexdigest()[:16]
    path = history_job_path(job_id)
    job = configuration | {
        "job_id": job_id, "status": "queued", "created_at": now_iso(), "updated_at": now_iso(),
        "host": os.environ.get("TWS_API_HOST", "127.0.0.1"),
        "port": int(os.environ.get("TWS_API_PORT", "7496")),
        "client_id": int(os.environ.get("TWS_INTRADAY_COLLECTOR_CLIENT_ID", "94")),
        "pace_seconds": pace_seconds, "data_root": str(HISTORY_ROOT / "intraday_5m"),
        "completed_windows": {}, "window_audits": {}, "rows": {}, "pid": None,
    }
    if job["client_id"] <= 0:
        raise ValueError("TWS_INTRADAY_COLLECTOR_CLIENT_ID must be positive.")
    atomic_json(path, job)
    launch_intraday_worker(path, job)
    return {"job_id": job_id, "status": "queued", "active": True, "reused": False,
            "data_root": job["data_root"], "symbols": list(requested_symbols),
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "bar_size": "5 mins", "window_size": "1 W"}


@mcp.tool(annotations=READ_ONLY)
def tws_historical_collection_status(job_id: str) -> dict[str, Any]:
    """Read a durable historical collection job's current progress and audit state."""
    job = history_job(job_id)
    return job | {"active": history_job_active(job)}


@mcp.tool(annotations=READ_ONLY)
def tws_collected_historical_bars(
    symbol: str, start_date: str = "", end_date: str = "",
    limit: Annotated[int, Field(ge=1, le=2000)] = 1500,
) -> dict[str, Any]:
    """Read bounded daily bars from the locally collected TWS history."""
    symbol = symbol_name(symbol)
    start = date.fromisoformat(start_date).isoformat() if start_date else "0001-01-01"
    end = date.fromisoformat(end_date).isoformat() if end_date else "9999-12-31"
    if end < start:
        raise ValueError("end_date must be on or after start_date.")
    path = HISTORY_ROOT / "daily" / f"{symbol}.csv"
    if not path.is_file():
        raise RuntimeError(f"No collected daily history exists for {symbol}.")
    import csv
    with path.open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if start <= row.get("date", "") <= end]
    total_matching_rows = len(rows)
    truncated = total_matching_rows > limit
    rows = rows[-limit:]
    converted = []
    for row in rows:
        converted.append({"time": row["date"], "open": number(row["open"]),
                          "high": number(row["high"]), "low": number(row["low"]),
                          "close": number(row["close"]), "volume": number(row["volume"]),
                          "wap": number(row["wap"]), "count": number(row["count"])})
    return {"symbol": symbol, "bar_size": "1 day", "regular_hours_only": True,
            "what_to_show": "TRADES", "path": str(path), "start_date": start_date or None,
            "end_date": end_date or None, "total_matching_rows": total_matching_rows,
            "truncated": truncated, "bars": converted}


@mcp.tool(annotations=READ_ONLY)
def tws_collected_intraday_bars(
    symbol: str, start_date: str = "", end_date: str = "",
    limit: Annotated[int, Field(ge=1, le=2000)] = 1500,
) -> dict[str, Any]:
    """Read bounded five-minute RTH TRADES bars from locally collected history.

    Date bounds use America/New_York session dates. Returned timestamps are UTC.
    """
    import csv

    symbol = symbol_name(symbol)
    start = date.fromisoformat(start_date).isoformat() if start_date else "0001-01-01"
    end = date.fromisoformat(end_date).isoformat() if end_date else "9999-12-31"
    if end < start:
        raise ValueError("end_date must be on or after start_date.")
    path = HISTORY_ROOT / "intraday_5m" / f"{symbol}.csv"
    if not path.is_file():
        raise RuntimeError(f"No collected five-minute history exists for {symbol}.")
    rows = []
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            stamp = datetime.fromisoformat(row["time"])
            session_date = stamp.astimezone(ZoneInfo("America/New_York")).date().isoformat()
            if start <= session_date <= end:
                rows.append(row)
    total_matching_rows = len(rows)
    truncated = total_matching_rows > limit
    rows = rows[-limit:]
    converted = []
    for row in rows:
        converted.append({"time": row["time"], "open": number(row["open"]),
                          "high": number(row["high"]), "low": number(row["low"]),
                          "close": number(row["close"]), "volume": number(row["volume"]),
                          "wap": number(row["wap"]), "count": number(row["count"])})
    return {"symbol": symbol, "bar_size": "5 mins", "regular_hours_only": True,
            "what_to_show": "TRADES", "timestamp_timezone": "UTC", "path": str(path),
            "start_date": start_date or None, "end_date": end_date or None,
            "total_matching_rows": total_matching_rows, "truncated": truncated,
            "bars": converted}


@mcp.tool(annotations=READ_ONLY)
async def tws_positions(ctx: Context) -> dict[str, Any]:
    """Read current positions and average cost; masks account identifiers."""
    async with backend(ctx).session() as ib:
        positions = await ib.reqPositionsAsync()
        return {"retrieved_at": now_iso(), "positions": [
            {"account": "***" + p.account[-4:], "contract": contract_data(p.contract),
             "quantity": number(p.position), "average_cost": number(p.avgCost)} for p in positions]}


@mcp.tool(annotations=READ_ONLY)
async def tws_account_summary(ctx: Context) -> dict[str, Any]:
    """Read cash, net liquidation, buying power, and margin; masks account identifiers."""
    async with backend(ctx).session() as ib:
        rows = await ib.accountSummaryAsync()
        return {"retrieved_at": now_iso(), "values": [
            {"account": "***" + row.account[-4:], "tag": row.tag,
             "value": row.value, "currency": row.currency} for row in rows]}


@mcp.tool(annotations=READ_ONLY)
async def tws_open_orders(ctx: Context) -> dict[str, Any]:
    """Read open orders across API clients. Does not bind, modify, or cancel orders."""
    async with backend(ctx).session() as ib:
        trades = await ib.reqAllOpenOrdersAsync()
        return {"retrieved_at": now_iso(), "orders": [
            {"order_id": t.order.orderId, "client_id": t.order.clientId,
             "contract": contract_data(t.contract), "action": t.order.action,
             "order_type": t.order.orderType, "quantity": number(t.order.totalQuantity),
             "limit_price": number(t.order.lmtPrice), "stop_price": number(t.order.auxPrice),
             "status": t.orderStatus.status} for t in trades]}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                                    idempotentHint=True, openWorldHint=False))
def collector_history(start_date: str = "", end_date: str = "") -> dict[str, Any]:
    """List saved option-data dates, ticker counts, raw/clean row counts, and run status.

    Dates are YYYY-MM-DD in New York time. Works even when TWS is disconnected.
    Counts are from the collection run ledger. No arbitrary SQL is accepted.
    """
    start = date.fromisoformat(start_date).isoformat() if start_date else "0001-01-01"
    end = date.fromisoformat(end_date).isoformat() if end_date else "9999-12-31"
    if end < start:
        raise ValueError("end_date must be on or after start_date.")
    if not DATABASE.is_file():
        return {"available": False, "dates": [],
                "note": "No options database configured. Set TWS_OPTIONS_DATABASE to an existing collection ledger."}
    with closing(sqlite3.connect(DATABASE.as_uri() + "?mode=ro", uri=True, timeout=5)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT substr(started_at,1,10) AS date, COUNT(*) AS runs, "
            "COUNT(DISTINCT symbol) AS tickers, SUM(COALESCE(raw_rows,0)) AS raw_rows, "
            "SUM(COALESCE(clean_rows,0)) AS clean_rows, "
            "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS successful_runs "
            "FROM collection_runs WHERE substr(started_at,1,10) BETWEEN ? AND ? "
            "GROUP BY substr(started_at,1,10) ORDER BY date", (start, end)).fetchall()
    return {"available": True, "database": str(DATABASE), "dates": [dict(row) for row in rows]}


def main():
    logging.basicConfig(level=logging.WARNING)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
