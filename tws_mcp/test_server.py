import asyncio
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from ib_insync import Stock, Ticker
from ib_insync.objects import OptionComputation
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from tws_mcp import intraday_collector
from tws_mcp import server


class DataTests(unittest.TestCase):
    def test_intraday_windows_cover_inclusive_dates_without_overlap(self):
        windows = list(intraday_collector.weekly_windows(
            datetime(2026, 1, 1).date(), datetime(2026, 1, 17).date()))
        self.assertEqual(windows, [
            (datetime(2026, 1, 1).date(), datetime(2026, 1, 7).date()),
            (datetime(2026, 1, 8).date(), datetime(2026, 1, 14).date()),
            (datetime(2026, 1, 15).date(), datetime(2026, 1, 17).date()),
        ])

    def test_intraday_cleaner_keeps_only_valid_ny_rth_five_minute_bars(self):
        from types import SimpleNamespace

        bars = [
            SimpleNamespace(date=datetime(2026, 10, 8, 13, 30, tzinfo=timezone.utc),
                            open=100, high=101, low=99, close=100.5, volume=10,
                            average=100.25, barCount=4),
            SimpleNamespace(date=datetime(2026, 10, 8, 13, 32, tzinfo=timezone.utc),
                            open=100, high=101, low=99, close=100.5, volume=10,
                            average=100.25, barCount=4),
            SimpleNamespace(date=datetime(2026, 10, 8, 20, 0, tzinfo=timezone.utc),
                            open=100, high=101, low=99, close=100.5, volume=10,
                            average=100.25, barCount=4),
            SimpleNamespace(date=datetime(2026, 10, 8, 13, 35, tzinfo=timezone.utc),
                            open=100, high=99, low=99.5, close=100.5, volume=10,
                            average=100.25, barCount=4),
        ]
        rows = intraday_collector.clean_bars(
            bars, datetime(2026, 10, 8).date(), datetime(2026, 10, 8).date())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["time"], "2026-10-08T13:30:00+00:00")

    def test_intraday_csv_is_idempotent_by_utc_timestamp(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "GLD.csv"
            row = {"time": "2026-10-08T13:30:00+00:00", "open": 100, "high": 101,
                   "low": 99, "close": 100.5, "volume": 10, "wap": 100.25, "count": 4}
            intraday_collector.write_rows(path, {row["time"]: row})
            intraday_collector.write_rows(path, {row["time"]: row | {"close": 100.6}})
            loaded = intraday_collector.read_rows(path)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[row["time"]]["close"], "100.6")

    def test_quotes_keep_missing_prices_missing(self):
        ticker = Ticker(contract=Stock("SPY", "SMART", "USD"), bid=-1,
                        ask=math.nan, close=100, time=datetime.now(timezone.utc))
        row = server.quote_data(ticker)
        self.assertIsNone(row["mid"])
        self.assertIsNone(row["bid"])
        self.assertFalse(row["two_sided_quote"])
        self.assertEqual(row["previous_close"], 100)
        json.dumps(row, allow_nan=False)

    def test_crossed_quote_is_not_a_valid_midpoint(self):
        ticker = Ticker(contract=Stock("SPY", "SMART", "USD"), bid=101, ask=100)
        self.assertIsNone(server.quote_data(ticker)["mid"])

    def test_greeks_namedtuple_is_serialized_with_missing_values_as_null(self):
        ticker = Ticker(contract=Stock("SPY", "SMART", "USD"),
                        modelGreeks=OptionComputation(0, 0.2, 0.5, 4, 0, math.nan, 0.1, -0.2, 600))
        row = server.quote_data(ticker)
        self.assertEqual(row["model_greeks"]["delta"], 0.5)
        self.assertIsNone(row["model_greeks"]["gamma"])
        json.dumps(row, allow_nan=False)

    def test_option_spec_rejects_invalid_contract_parameters(self):
        valid = dict(symbol="spy", expiry="2026-09-18", strike=600, right="C")
        self.assertEqual(server.OptionSpec(**valid).expiry, "20260918")
        for replacement in ({"strike": -1}, {"expiry": "2026-02-30"},
                            {"right": "BUY"}, {"symbol": "SPY; DROP TABLE"}):
            with self.assertRaises(ValueError):
                server.OptionSpec(**(valid | replacement))

    def test_collector_history_is_read_only_and_date_filtered(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "quotes.sqlite"
            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE collection_runs (started_at TEXT, symbol TEXT, raw_rows INT, clean_rows INT, status TEXT)")
                conn.executemany("INSERT INTO collection_runs VALUES (?,?,?,?,?)", [
                    ("2026-09-10T09:45:00-04:00", "SPY", 10, 5, "success"),
                    ("2026-09-11T09:45:00-04:00", "IWM", 20, 10, "success")])
            with patch.object(server, "DATABASE", path):
                rows = server.collector_history("2026-09-11", "2026-09-11")["dates"]
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["clean_rows"], 10)
                with self.assertRaises(ValueError):
                    server.collector_history("2026-09-12", "2026-09-11")


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_market_data_cancelled_when_request_is_cancelled(self):
        client = server.TwsBackend()
        client.ib = MagicMock()
        client.ib.isConnected.return_value = True
        stock = Stock("SPY", "SMART", "USD", conId=123)
        client.ib.qualifyContractsAsync = AsyncMock(return_value=[stock])
        client.ib.reqMktData.return_value = Ticker(contract=stock)
        with patch.object(server.asyncio, "sleep", side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await client.quotes([stock], "realtime", 3)
        client.ib.cancelMktData.assert_called_once_with(stock)

    async def test_duplicate_contracts_use_one_subscription(self):
        client = server.TwsBackend()
        client.ib = MagicMock()
        client.ib.isConnected.return_value = True
        stock = Stock("SPY", "SMART", "USD", conId=123)
        client.ib.qualifyContractsAsync = AsyncMock(return_value=[stock, stock])
        client.ib.reqMktData.return_value = Ticker(contract=stock)
        with patch.object(server.asyncio, "sleep", new=AsyncMock()):
            result = await client.quotes([stock, stock], "frozen", 1)
        self.assertEqual(len(result["quotes"]), 1)
        client.ib.reqMarketDataType.assert_called_once_with(2)
        client.ib.reqMktData.assert_called_once()
        client.ib.cancelMktData.assert_called_once()

    async def test_failed_qualification_does_not_subscribe(self):
        client = server.TwsBackend()
        client.ib = MagicMock()
        client.ib.isConnected.return_value = True
        client.ib.qualifyContractsAsync = AsyncMock(return_value=[])
        with self.assertRaises(ValueError):
            await client.quotes([Stock("BAD", "SMART", "USD")], "realtime", 1)
        client.ib.reqMktData.assert_not_called()

    async def test_disconnected_client_reconnects_readonly(self):
        client = server.TwsBackend()
        client.ib = MagicMock()
        client.ib.isConnected.return_value = False
        client.ib.connectAsync = AsyncMock()
        async with client.session():
            pass
        self.assertTrue(client.ib.connectAsync.call_args.kwargs["readonly"])
        self.assertNotEqual(client.ib.connectAsync.call_args.kwargs["clientId"], 0)

    async def test_mcp_stdio_discovery_validation_and_offline_status(self):
        params = StdioServerParameters(
            command=sys.executable, args=["-m", "tws_mcp.server"],
            env=os.environ | {"TWS_API_HOST": "127.0.0.1", "TWS_API_PORT": "1", "TWS_MCP_CLIENT_ID": "92"})
        async with stdio_client(params) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                await session.initialize()
                tools = (await session.list_tools()).tools
                self.assertEqual(len(tools), 14)
                annotations = {tool.name: tool.annotations for tool in tools}
                self.assertFalse(annotations["tws_start_historical_collection"].readOnlyHint)
                self.assertFalse(annotations["tws_start_intraday_collection"].readOnlyHint)
                self.assertTrue(all(value.readOnlyHint for name, value in annotations.items()
                                    if name not in {"tws_start_historical_collection",
                                                    "tws_start_intraday_collection"}))
                self.assertFalse(any("place" in t.name or "cancel" in t.name for t in tools))
                invalid = await session.call_tool("tws_stock_quotes", {"symbols": []})
                self.assertTrue(invalid.isError)
                status = await session.call_tool("tws_status", {})
                self.assertFalse(status.isError)
                self.assertFalse(status.structuredContent["connected"])


if __name__ == "__main__":
    unittest.main()
