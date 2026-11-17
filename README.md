# TWS MCP

[![Tests](https://github.com/csbring/tws-mcp/actions/workflows/tests.yml/badge.svg)](https://github.com/csbring/tws-mcp/actions/workflows/tests.yml)

A local Model Context Protocol server for Interactive Brokers Trader Workstation.
Give an MCP client stock and option quotes, option chains, historical bars, and
resumable data collection through a logged-in TWS session.

The server exposes **14 tools**, with no order placement, cancellation, transfer,
or login tools. Two tools start local collection workers and write CSV/audit
files; the others are read-only. This repository contains source code and
synthetic tests, **no account records, credentials, market datasets, or saved jobs**.

## Quick start

Requires Python 3.11–3.13. Live requests also require TWS, an enabled API socket,
and the relevant IBKR market-data permissions. Offline tests need no broker account.

```bash
git clone https://github.com/csbring/tws-mcp.git
cd tws-mcp
python3 -m venv .venv
source .venv/bin/activate
python -m pip install .
python -m unittest discover -s tws_mcp -p 'test_*.py' -v
python -m tws_mcp.smoke_test
```

In TWS API settings, enable socket clients and **Read-Only API**. Match the port
below to your TWS configuration. The examples use `7497` for a paper session;
the server's inherited default is `7496`. Use a distinct positive client ID for
each connection. Client ID zero is rejected.

```bash
export TWS_API_HOST=127.0.0.1
export TWS_API_PORT=7497
export TWS_MCP_CLIENT_ID=91
tws-mcp
```

The last command starts a stdio server, so it waits for an MCP client rather than
opening a webpage. Quit with Ctrl-C when running it manually.

## Connect an MCP client

Use the absolute path to the installed executable and pass environment variables
through your client's configuration. For clients using `mcpServers` JSON:

```json
{
  "mcpServers": {
    "tws": {
      "command": "/absolute/path/to/tws-mcp/.venv/bin/tws-mcp",
      "env": {
        "TWS_API_HOST": "127.0.0.1",
        "TWS_API_PORT": "7497",
        "TWS_MCP_CLIENT_ID": "91"
      }
    }
  }
}
```

Use the corresponding `Scripts` executable path on Windows. A checkout launcher,
`./run_server.sh`, is also available on systems with Bash after installing into
the checkout's `.venv`. For development, install with `python -m pip install -e .`.

Example requests: “Show SPY's bid/ask and quote timestamp”, “List the SPY option
chain”, or “Get the last five regular-session five-minute bars for IWM.”

## Tools

| Tool | Purpose |
| --- | --- |
| `tws_status` | Connectivity, broker time, and account count |
| `tws_stock_quotes` | Up to 20 stock/ETF quotes |
| `tws_option_chain` | SMART expirations, strikes, and trading classes |
| `tws_option_quotes` | Up to 20 qualified options, Greeks and OI when supplied |
| `tws_historical_bars` | Up to five days of stock/ETF bars |
| `tws_start_historical_collection` | Start/resume a local daily-bar collection |
| `tws_start_intraday_collection` | Start/resume a local five-minute collection |
| `tws_historical_collection_status` | Read job progress and audit state |
| `tws_collected_historical_bars` | Read bounded local daily history |
| `tws_collected_intraday_bars` | Read bounded local five-minute history |
| `tws_positions` | Positions and average cost; account IDs partially masked |
| `tws_account_summary` | Cash, equity, buying power, and margin; IDs partially masked |
| `tws_open_orders` | Existing open orders across API clients |
| `collector_history` | Date/count summaries from an optional options collection ledger |

Account tools return the connected user's private information to their MCP client.
Partial ID masking is not anonymization: positions, balances, and order details
remain sensitive. Use a trusted client and review what it retains or sends to a
model provider. There is no bundled account data or automatic account export.

## Configuration and storage

| Environment variable | Default |
| --- | --- |
| `TWS_API_HOST` | `127.0.0.1` |
| `TWS_API_PORT` | `7496` |
| `TWS_MCP_CLIENT_ID` | `91` |
| `TWS_HISTORICAL_COLLECTOR_CLIENT_ID` | `93` |
| `TWS_INTRADAY_COLLECTOR_CLIENT_ID` | `94` |
| `TWS_HISTORY_ROOT` | `$XDG_DATA_HOME/tws-mcp/historical_data` |
| `TWS_OPTIONS_DATABASE` | `$XDG_DATA_HOME/tws-mcp/options_quotes.sqlite` |

When `XDG_DATA_HOME` is unset, it defaults to `~/.local/share`. These paths are
outside the checkout. Overrides accept absolute paths or `~`. The example
`.env.example` is documentation; the server does not load `.env` files itself.

History workers save daily/five-minute CSVs and JSON job checkpoints under
`TWS_HISTORY_ROOT`. They run separately from the MCP process, retry failed
requests, and skip completed windows when resuming a failed job. They require
broker data permissions and remain subject to pacing limits. Do not launch
multiple server processes against the same history directory: job coordination
is not protected by a cross-process lock. Keep outputs private.

`collector_history` is an optional compatibility tool, not an options collector.
It opens an existing SQLite database read-only and expects a `collection_runs`
table with `started_at`, `symbol`, `raw_rows`, `clean_rows`, and `status` columns.
Without a database it returns `available: false` and an empty date list.

## Data quality and boundaries

- Missing prices, Greeks, and open interest stay null. Crossed quotes have no midpoint.
- Quote timestamps are callback receipt times, not necessarily last-trade times.
  Requested realtime data does not guarantee a fresh or executable quote.
- Frozen and delayed data require an explicit request; responses report the
  data type returned by IBKR. Chain combinations are qualified before quoting.
- Requests are serialized and have a 45-second timeout after acquiring the lock.
  Quote subscriptions are cancelled when requests finish or are cancelled.
- Connections request `readonly=True`; **TWS's Read-Only API setting** supplies
  broker-side enforcement. The client flag alone is not a permission boundary.
- Daily history may include an incomplete current bar. The five-minute collector
  defaults to yesterday, uses UTC timestamps, and filters dates in New York time.
- OHLC checks and weekday/session-clock filtering do not prove complete exchange
  sessions. Validate missing bars, holidays, and early closes before research.
- A legacy TLT backfill rule uses ARCA for weekly windows starting before
  2016-02-05, recording the venue in each audit. Venue volume can differ from SMART;
  this preserves a historical source workaround, not a universal availability claim.

## Verification

```bash
python -m unittest discover -s tws_mcp -p 'test_*.py' -v
python -m tws_mcp.smoke_test
```

Tests use synthetic data, temporary directories and mocked broker calls, plus a
real MCP stdio handshake. The offline smoke test uses empty temporary storage and
an unreachable local broker port. GitHub Actions runs on Python 3.11, 3.12 and 3.13.

An explicitly requested live check is available after configuring TWS:

```bash
python -m tws_mcp.smoke_test --live
```

It checks connectivity, SPY chains, quotes, and a few historical bars. It does not
request balances/positions, place orders, or start a collection job. Live broker
access and quote availability are separate from passing the offline tests.

The implementation uses the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
and [ib_insync](https://github.com/erdewit/ib_insync). Dependency versions are pinned
where inherited from the working implementation; no IBKR affiliation is implied.
