"""Regression checks for standalone storage and broker-free installation."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from tws_mcp import server


class PackagingTests(unittest.TestCase):
    def test_missing_optional_database_does_not_create_a_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "missing.sqlite"
            with patch.object(server, "DATABASE", path):
                result = server.collector_history()
            self.assertFalse(result["available"])
            self.assertEqual(result["dates"], [])
            self.assertFalse(path.exists())

    def test_storage_defaults_are_outside_the_package(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {key: value for key, value in os.environ.items()
                   if key not in {"TWS_OPTIONS_DATABASE", "TWS_HISTORY_ROOT"}}
            env["XDG_DATA_HOME"] = tmp
            code = ("import json; from tws_mcp import server; "
                    "print(json.dumps([str(server.DATABASE), str(server.HISTORY_ROOT)]))")
            output = subprocess.check_output([sys.executable, "-c", code], env=env, text=True)
            self.assertEqual(json.loads(output), [
                str(Path(tmp) / "tws-mcp/options_quotes.sqlite"),
                str(Path(tmp) / "tws-mcp/historical_data"),
            ])
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_storage_overrides_are_resolved(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ | {"TWS_OPTIONS_DATABASE": str(Path(tmp) / "ledger.sqlite"),
                                "TWS_HISTORY_ROOT": str(Path(tmp) / "history")}
            code = ("import json; from tws_mcp import server; "
                    "print(json.dumps([str(server.DATABASE), str(server.HISTORY_ROOT)]))")
            output = subprocess.check_output([sys.executable, "-c", code], env=env, text=True)
            self.assertEqual(json.loads(output), [env["TWS_OPTIONS_DATABASE"], env["TWS_HISTORY_ROOT"]])
            self.assertEqual(list(Path(tmp).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
