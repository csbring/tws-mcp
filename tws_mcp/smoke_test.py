"""Exercise the server over MCP; --live also requests a TWS quote and bars."""

import argparse
import asyncio
from datetime import datetime
from pathlib import Path
import os
import sys
import tempfile
from zoneinfo import ZoneInfo

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def run(live):
    # The offline check uses empty temporary storage and cannot reach a broker.
    with tempfile.TemporaryDirectory() as tmp:
        environment = os.environ | {"TWS_MCP_CLIENT_ID": "95",
                                    "TWS_OPTIONS_DATABASE": str(Path(tmp) / "missing.sqlite"),
                                    "TWS_HISTORY_ROOT": str(Path(tmp) / "history")}
        if not live:
            environment["TWS_API_HOST"] = "127.0.0.1"
            environment["TWS_API_PORT"] = "1"
        await check(live, environment)


async def check(live, environment):
    params = StdioServerParameters(command=sys.executable, args=["-m", "tws_mcp.server"],
                                  env=environment)
    async with stdio_client(params) as (reader, writer):
        async with ClientSession(reader, writer) as session:
            await session.initialize()
            print("MCP tools:", ", ".join(t.name for t in (await session.list_tools()).tools))
            history = await session.call_tool("collector_history", {})
            assert not history.isError, history
            print("Saved dates:", len(history.structuredContent["dates"]))
            if not live:
                return
            status = await session.call_tool("tws_status", {})
            assert not status.isError and status.structuredContent["connected"], status
            print("TWS connected:", status.structuredContent["server_time"])
            chain = await session.call_tool("tws_option_chain", {"symbol": "SPY"})
            assert not chain.isError and chain.structuredContent["chains"], chain
            print("SPY SMART option chains:", len(chain.structuredContent["chains"]))
            quotes = await session.call_tool("tws_stock_quotes", {"symbols": ["SPY"], "market_data": "frozen"})
            assert not quotes.isError, quotes
            q = quotes.structuredContent["quotes"][0]
            print("SPY quote:", {k: q[k] for k in ("bid", "ask", "last", "market_data_type")})
            spot = q["last"] or q["previous_close"]
            today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
            available = [c for c in chain.structuredContent["chains"]
                         if c["multiplier"] == "100" and any(e > today for e in c["expirations"])]
            if spot and available:
                selected = next((c for c in available if c["trading_class"] == "SPY"), available[0])
                expiry = min(e for e in selected["expirations"] if e > today)
                strike = min(selected["strikes"], key=lambda x: abs(x - spot))
                option = await session.call_tool("tws_option_quotes", {
                    "options": [{"symbol": "SPY", "expiry": expiry, "strike": strike,
                                 "right": "C", "trading_class": selected["trading_class"]}],
                    "market_data": "frozen"})
                assert not option.isError, option
                print("Option quote contract:", option.structuredContent["quotes"][0]["contract"]["local_symbol"])
            bars = await session.call_tool("tws_historical_bars", {"symbol": "SPY", "limit": 5})
            assert not bars.isError, bars
            print("SPY historical bars:", len(bars.structuredContent["bars"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    asyncio.run(run(parser.parse_args().live))
