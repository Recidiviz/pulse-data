# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Tests for rekey_tables_to_cmek."""
import datetime
import unittest
from typing import Any
from unittest.mock import MagicMock

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from recidiviz.big_query.rekey_tables_to_cmek import (
    RekeyAction,
    rekey_table,
    rekey_tables,
)

_PROJECT = "test-project"
_KEY = "projects/kms-proj/locations/us/keyRings/ring/cryptoKeys/the-key"
_NOW = datetime.datetime.now(datetime.timezone.utc)


def _fake_table(
    *,
    table_type: str = "TABLE",
    kms_key: str | None = None,
    streaming_buffer: Any = None,
    modified_days_ago: float = 400,
    num_bytes: int = 1000,
    num_rows: int = 10,
) -> MagicMock:
    table = MagicMock()
    table.table_type = table_type
    table.encryption_configuration = (
        bigquery.EncryptionConfiguration(kms_key_name=kms_key) if kms_key else None
    )
    table.streaming_buffer = streaming_buffer
    table.modified = _NOW - datetime.timedelta(days=modified_days_ago)
    table.num_bytes = num_bytes
    table.num_rows = num_rows
    return table


def _query_result(value: str) -> MagicMock:
    job = MagicMock()
    job.result.return_value = iter([(value,)])
    return job


def _rekey_table_defaults(client: MagicMock, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "client": client,
        "project_id": _PROJECT,
        "dataset_id": "ds",
        "table_id": "tb",
        "kms_key": _KEY,
        "apply": True,
        "min_quiet_hours": 24,
        "streamed_tables": set(),
        "checksum_max_gb": 100,
        "keep_backups": False,
    }
    kwargs.update(overrides)
    return rekey_table(**kwargs)


class RekeyTableTest(unittest.TestCase):
    """Tests for the single-table guards and the apply path."""

    def test_dry_run_reports_and_changes_nothing(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table()

        result = _rekey_table_defaults(client, apply=False)

        self.assertEqual(RekeyAction.WOULD_REKEY, result.action)
        client.copy_table.assert_not_called()
        client.query.assert_not_called()
        client.delete_table.assert_not_called()

    def test_skip_missing(self) -> None:
        client = MagicMock()
        client.get_table.side_effect = NotFound("gone")

        self.assertEqual(RekeyAction.SKIP_MISSING, _rekey_table_defaults(client).action)

    def test_skip_not_base_table(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table(table_type="VIEW")

        self.assertEqual(
            RekeyAction.SKIP_NOT_BASE, _rekey_table_defaults(client).action
        )

    def test_skip_already_cmek(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table(kms_key=f"{_KEY}/version/1")

        result = _rekey_table_defaults(client)

        self.assertEqual(RekeyAction.SKIP_ALREADY_CMEK, result.action)
        client.copy_table.assert_not_called()

    def test_skip_other_key(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table(kms_key="projects/other/k")

        self.assertEqual(
            RekeyAction.SKIP_OTHER_KEY, _rekey_table_defaults(client).action
        )

    def test_skip_streaming_buffer(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table(streaming_buffer=object())

        self.assertEqual(RekeyAction.SKIP_BUFFER, _rekey_table_defaults(client).action)

    def test_skip_recently_streamed(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table()

        result = _rekey_table_defaults(client, streamed_tables={("ds", "tb")})

        self.assertEqual(RekeyAction.SKIP_STREAMED, result.action)

    def test_skip_not_quiet(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table(modified_days_ago=0.1)

        self.assertEqual(
            RekeyAction.SKIP_NOT_QUIET, _rekey_table_defaults(client).action
        )

    def test_apply_happy_path(self) -> None:
        client = MagicMock()
        client.get_table.side_effect = [
            _fake_table(),  # gates
            _fake_table(kms_key=f"{_KEY}/version/1"),  # verify
        ]
        client.query.side_effect = [_query_result("10|abc"), _query_result("10|abc")]

        result = _rekey_table_defaults(client)

        self.assertEqual(RekeyAction.REKEYED_VERIFIED, result.action)
        table_ref = f"{_PROJECT}.ds.tb"
        backup_ref = f"{table_ref}_rekey_bak"
        # The backup copy must come before the in-place re-key.
        self.assertEqual(
            [(table_ref, backup_ref), (table_ref, table_ref)],
            [c.args for c in client.copy_table.call_args_list],
        )
        rekey_config = client.copy_table.call_args_list[1].kwargs["job_config"]
        self.assertEqual(
            _KEY, rekey_config.destination_encryption_configuration.kms_key_name
        )
        client.delete_table.assert_called_once_with(backup_ref, not_found_ok=True)

    def test_mismatch_keeps_backup(self) -> None:
        client = MagicMock()
        client.get_table.side_effect = [
            _fake_table(),
            _fake_table(kms_key=f"{_KEY}/version/1"),
        ]
        client.query.side_effect = [
            _query_result("10|abc"),
            _query_result("11|zzz"),  # content changed under us
        ]

        result = _rekey_table_defaults(client)

        self.assertEqual(RekeyAction.CRITICAL_MISMATCH, result.action)
        client.delete_table.assert_not_called()

    def test_rejects_sql_unsafe_identifiers(self) -> None:
        client = MagicMock()
        for bad_table_id in ("t`; DROP", "t.other", "t x", ""):
            with self.assertRaisesRegex(ValueError, "Invalid table_id"):
                _rekey_table_defaults(client, table_id=bad_table_id)
        with self.assertRaisesRegex(ValueError, "Invalid dataset_id"):
            _rekey_table_defaults(client, dataset_id="ds-with-dash")
        client.get_table.assert_not_called()

    def test_large_table_uses_count_only_checksum(self) -> None:
        client = MagicMock()
        client.get_table.return_value = _fake_table(num_bytes=int(200e9))

        result = _rekey_table_defaults(client, apply=False)

        self.assertEqual(RekeyAction.WOULD_REKEY, result.action)
        self.assertIn("checksum=count", result.detail)


class RekeyTablesTest(unittest.TestCase):
    """Tests for the sweep loop."""

    @staticmethod
    def _ledger_result() -> MagicMock:
        job = MagicMock()
        job.result.return_value = iter([])
        return job

    def test_one_failure_does_not_stop_the_sweep(self) -> None:
        client = MagicMock()
        client.query.return_value = self._ledger_result()
        client.get_table.side_effect = [
            RuntimeError("policy tag 403"),
            _fake_table(kms_key=f"{_KEY}/version/1"),
        ]

        results = rekey_tables(
            client=client,
            project_id=_PROJECT,
            tables=[("ds", "bad"), ("ds", "good")],
            kms_key=_KEY,
            apply=True,
        )

        self.assertEqual(
            [RekeyAction.FAILED, RekeyAction.SKIP_ALREADY_CMEK],
            [r.action for r in results],
        )

    def test_mismatch_halts_the_sweep(self) -> None:
        client = MagicMock()
        ledger = self._ledger_result()
        client.query.side_effect = [
            ledger,
            _query_result("10|abc"),
            _query_result("11|zzz"),
        ]
        client.get_table.side_effect = [
            _fake_table(),
            _fake_table(kms_key=f"{_KEY}/version/1"),
        ]

        results = rekey_tables(
            client=client,
            project_id=_PROJECT,
            tables=[("ds", "racy"), ("ds", "never_reached")],
            kms_key=_KEY,
            apply=True,
        )

        self.assertEqual([RekeyAction.CRITICAL_MISMATCH], [r.action for r in results])

    def test_dry_run_is_default(self) -> None:
        client = MagicMock()
        client.query.return_value = self._ledger_result()
        client.get_table.return_value = _fake_table()

        results = rekey_tables(
            client=client,
            project_id=_PROJECT,
            tables=[("ds", "tb")],
            kms_key=_KEY,
        )

        self.assertEqual([RekeyAction.WOULD_REKEY], [r.action for r in results])
        client.copy_table.assert_not_called()


if __name__ == "__main__":
    unittest.main()
