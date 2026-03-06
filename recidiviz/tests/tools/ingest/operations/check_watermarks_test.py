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
"""Tests for check_watermarks.py."""
import datetime
import unittest
from io import StringIO
from unittest.mock import patch

import pytest
import sqlalchemy.orm

from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.ingest.operations.check_watermarks import (
    _build_tag_rows,
    _get_fix_watermark_query,
    _get_max_update_datetimes,
    _print_check_results,
    check_watermarks,
    get_watermarks,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_REGION_CODE = "US_XX"
_INGEST_INSTANCE = "PRIMARY"
_JOB_ID_1 = "2024-01-01_00_00_00-1111111111111111111"
_JOB_ID_2 = "2024-02-01_00_00_00-2222222222222222222"

_UTC = datetime.timezone.utc


def _DT(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
) -> datetime.datetime:
    return datetime.datetime(year, month, day, hour, minute, second, tzinfo=_UTC)


@pytest.mark.uses_db
class TestQueryFunctions(unittest.TestCase):
    """Integration tests for the DB query functions against a real PostgresSQL instance."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @staticmethod
    def _add_bq_file_metadata(
        session: sqlalchemy.orm.Session,
        *,
        file_id: int,
        file_tag: str,
        update_datetime: datetime.datetime,
        is_invalidated: bool = False,
        file_processed_time: datetime.datetime | None = datetime.datetime(2024, 1, 1),
    ) -> None:
        session.add(
            schema.DirectIngestRawBigQueryFileMetadata(
                file_id=file_id,
                region_code=_REGION_CODE,
                raw_data_instance=_INGEST_INSTANCE,
                file_tag=file_tag,
                update_datetime=update_datetime,
                is_invalidated=is_invalidated,
                file_processed_time=file_processed_time,
            )
        )

    @staticmethod
    def _add_job_and_watermarks(
        session: sqlalchemy.orm.Session,
        *,
        job_id: str,
        is_invalidated: bool = False,
        watermarks: dict[str, datetime.datetime],
    ) -> None:
        session.add(
            schema.DirectIngestDataflowJob(
                job_id=job_id,
                region_code=_REGION_CODE,
                ingest_instance=_INGEST_INSTANCE,
                completion_time=datetime.datetime(2024, 1, 1),
                is_invalidated=is_invalidated,
            )
        )
        for tag, dt in watermarks.items():
            session.add(
                schema.DirectIngestDataflowRawTableUpperBounds(
                    region_code=_REGION_CODE,
                    job_id=job_id,
                    raw_data_file_tag=tag,
                    watermark_datetime=dt,
                )
            )

    # --- _get_max_update_datetimes ---

    def test_get_max_update_datetimes_basic(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._add_bq_file_metadata(
                session, file_id=1, file_tag="tag_a", update_datetime=_DT(2024, 1, 5)
            )
            self._add_bq_file_metadata(
                session, file_id=2, file_tag="tag_a", update_datetime=_DT(2024, 1, 10)
            )
            self._add_bq_file_metadata(
                session, file_id=3, file_tag="tag_b", update_datetime=_DT(2024, 2, 1)
            )
            session.commit()

            result = _get_max_update_datetimes(session, _REGION_CODE, _INGEST_INSTANCE)

        self.assertEqual(result["tag_a"], _DT(2024, 1, 10))
        self.assertEqual(result["tag_b"], _DT(2024, 2, 1))

    def test_get_max_update_datetimes_excludes_invalidated(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._add_bq_file_metadata(
                session, file_id=1, file_tag="tag_a", update_datetime=_DT(2024, 1, 5)
            )
            self._add_bq_file_metadata(
                session,
                file_id=2,
                file_tag="tag_a",
                update_datetime=_DT(2024, 1, 10),
                is_invalidated=True,
            )
            session.commit()

            result = _get_max_update_datetimes(session, _REGION_CODE, _INGEST_INSTANCE)

        self.assertEqual(result["tag_a"], _DT(2024, 1, 5))

    def test_get_max_update_datetimes_excludes_unprocessed(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._add_bq_file_metadata(
                session, file_id=1, file_tag="tag_a", update_datetime=_DT(2024, 1, 5)
            )
            self._add_bq_file_metadata(
                session,
                file_id=2,
                file_tag="tag_a",
                update_datetime=_DT(2024, 1, 10),
                file_processed_time=None,
            )
            session.commit()

            result = _get_max_update_datetimes(session, _REGION_CODE, _INGEST_INSTANCE)

        self.assertEqual(result["tag_a"], _DT(2024, 1, 5))

    def test_get_max_update_datetimes_empty(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            result = _get_max_update_datetimes(session, _REGION_CODE, _INGEST_INSTANCE)
        self.assertEqual(result, {})

    # --- _get_watermarks ---

    def test_get_watermarks_basic(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._add_job_and_watermarks(
                session,
                job_id=_JOB_ID_1,
                watermarks={"tag_a": _DT(2024, 1, 5), "tag_b": _DT(2024, 2, 1)},
            )
            session.commit()

            watermarks, job_id = get_watermarks(session, _REGION_CODE, _INGEST_INSTANCE)

        self.assertEqual(job_id, _JOB_ID_1)
        self.assertEqual(watermarks["tag_a"], _DT(2024, 1, 5))
        self.assertEqual(watermarks["tag_b"], _DT(2024, 2, 1))

    def test_get_watermarks_uses_latest_non_invalidated_job(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._add_job_and_watermarks(
                session,
                job_id=_JOB_ID_1,
                watermarks={"tag_a": _DT(2024, 1, 1)},
            )
            self._add_job_and_watermarks(
                session,
                job_id=_JOB_ID_2,
                watermarks={"tag_a": _DT(2024, 2, 1)},
            )
            session.commit()

            watermarks, job_id = get_watermarks(session, _REGION_CODE, _INGEST_INSTANCE)

        # Should pick the max job_id (_JOB_ID_2 sorts higher lexicographically)
        self.assertEqual(job_id, _JOB_ID_2)
        self.assertEqual(watermarks["tag_a"], _DT(2024, 2, 1))

    def test_get_watermarks_skips_invalidated_job(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self._add_job_and_watermarks(
                session,
                job_id=_JOB_ID_1,
                watermarks={"tag_a": _DT(2024, 1, 1)},
            )
            self._add_job_and_watermarks(
                session,
                job_id=_JOB_ID_2,
                is_invalidated=True,
                watermarks={"tag_a": _DT(2024, 2, 1)},
            )
            session.commit()

            watermarks, job_id = get_watermarks(session, _REGION_CODE, _INGEST_INSTANCE)

        self.assertEqual(job_id, _JOB_ID_1)
        self.assertEqual(watermarks["tag_a"], _DT(2024, 1, 1))

    def test_get_watermarks_empty_when_no_jobs(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            watermarks, job_id = get_watermarks(session, _REGION_CODE, _INGEST_INSTANCE)

        self.assertEqual(watermarks, {})
        self.assertIsNone(job_id)

    def test_check_watermarks_raises_when_no_watermarks(self) -> None:
        with patch(
            "recidiviz.tools.ingest.operations.check_watermarks._get_data_needed_for_checking",
            return_value=({}, {}, None),
        ):
            with self.assertRaisesRegex(ValueError, "No watermarks found for US_XX"):
                check_watermarks(
                    self.database_key, _REGION_CODE, _INGEST_INSTANCE, list_mode=False
                )


class TestBuildTagRows(unittest.TestCase):
    """Tests for _build_tag_rows."""

    def test_all_ok(self) -> None:
        watermarks = {"tag_a": _DT(2024, 1, 2), "tag_b": _DT(2024, 1, 2)}
        current = {"tag_a": _DT(2024, 1, 2), "tag_b": _DT(2024, 1, 3)}
        statuses = {r[0]: r[3] for r in _build_tag_rows(watermarks, current)}
        self.assertEqual(statuses, {"tag_a": "OK", "tag_b": "OK"})

    def test_missing(self) -> None:
        watermarks = {"tag_a": _DT(2024, 1, 2), "tag_b": _DT(2024, 1, 2)}
        current = {"tag_a": _DT(2024, 1, 2)}
        statuses = {r[0]: r[3] for r in _build_tag_rows(watermarks, current)}
        self.assertEqual(statuses["tag_a"], "OK")
        self.assertEqual(statuses["tag_b"], "MISSING")

    def test_stale(self) -> None:
        watermarks = {"tag_a": _DT(2024, 1, 10), "tag_b": _DT(2024, 1, 2)}
        current = {"tag_a": _DT(2024, 1, 5), "tag_b": _DT(2024, 1, 5)}
        statuses = {r[0]: r[3] for r in _build_tag_rows(watermarks, current)}
        self.assertEqual(statuses["tag_a"], "STALE")
        self.assertEqual(statuses["tag_b"], "OK")

    def test_multiple_missing(self) -> None:
        watermarks = {
            "tag_a": _DT(2024, 1, 10),
            "tag_b": _DT(2024, 1, 2),
            "tag_c": _DT(2024, 1, 1),
        }
        current = {"tag_b": _DT(2024, 1, 5)}
        statuses = {r[0]: r[3] for r in _build_tag_rows(watermarks, current)}
        self.assertEqual(statuses["tag_a"], "MISSING")
        self.assertEqual(statuses["tag_b"], "OK")
        self.assertEqual(statuses["tag_c"], "MISSING")

    def test_no_watermark(self) -> None:
        watermarks: dict[str, datetime.datetime] = {}
        current = {"tag_a": _DT(2024, 1, 5)}
        statuses = {r[0]: r[3] for r in _build_tag_rows(watermarks, current)}
        self.assertEqual(statuses["tag_a"], "NO_WATERMARK")

    def test_current_equal_to_watermark_is_ok(self) -> None:
        dt = _DT(2024, 6, 15, 12, 0, 0)
        watermarks = {"tag_a": dt}
        current = {"tag_a": dt}
        self.assertEqual(_build_tag_rows(watermarks, current)[0][3], "OK")

    def test_problematic_tags_sorted_first(self) -> None:
        watermarks = {
            "ok_tag": _DT(2024, 1, 1),
            "stale_tag": _DT(2024, 1, 10),
            "missing_tag": _DT(2024, 1, 1),
        }
        current = {
            "ok_tag": _DT(2024, 1, 5),
            "stale_tag": _DT(2024, 1, 5),
        }
        rows = _build_tag_rows(watermarks, current)
        statuses = [r[3] for r in rows]
        self.assertIn(statuses[0], ("MISSING", "STALE"))
        self.assertIn(statuses[1], ("MISSING", "STALE"))
        self.assertEqual(statuses[2], "OK")


class TestGetFixWatermarkQuery(unittest.TestCase):
    """Tests for _get_fix_watermark_query."""

    def test_query_targets_correct_table_and_params(self) -> None:
        query = _get_fix_watermark_query()
        self.assertIn("direct_ingest_dataflow_raw_table_upper_bounds", query.text)
        self.assertIn(":job_id", query.text)
        self.assertIn(":tags", query.text)


class TestPrintCheckResults(unittest.TestCase):
    """Tests for _print_check_results."""

    @staticmethod
    def _run(
        watermarks: dict[str, datetime.datetime],
        current: dict[str, datetime.datetime],
        job_id: str = _JOB_ID_1,
        list_mode: bool = False,
    ) -> str:
        rows = _build_tag_rows(watermarks, current)
        with patch("sys.stdout", new_callable=StringIO) as mock_out:
            _print_check_results(rows, len(watermarks), job_id, list_mode)
            return mock_out.getvalue()

    def test_pass(self) -> None:
        watermarks = {"tag_a": _DT(2024, 1, 1), "tag_b": _DT(2024, 1, 1)}
        current = {"tag_a": _DT(2024, 1, 2), "tag_b": _DT(2024, 1, 1)}
        output = self._run(watermarks, current)
        self.assertIn("PASS", output)
        self.assertIn("2", output)  # "All 2 watermarks"

    def test_stale_prints_fail_and_suggested_sql(self) -> None:
        watermarks = {"tag_a": _DT(2024, 1, 10)}
        current = {"tag_a": _DT(2024, 1, 5)}
        output = self._run(watermarks, current)
        self.assertIn("FAIL", output)
        self.assertIn("tag_a", output)
        self.assertIn(
            "DELETE FROM direct_ingest_dataflow_raw_table_upper_bounds", output
        )
        self.assertIn(_JOB_ID_1, output)

    def test_missing_prints_fail_and_suggested_sql(self) -> None:
        watermarks = {"tag_a": _DT(2024, 1, 1)}
        current: dict[str, datetime.datetime] = {}
        output = self._run(watermarks, current)
        self.assertIn("FAIL", output)
        self.assertIn("tag_a", output)
        self.assertIn(
            "DELETE FROM direct_ingest_dataflow_raw_table_upper_bounds", output
        )

    def test_list_mode_shows_all_statuses(self) -> None:
        watermarks = {
            "stale_tag": _DT(2024, 1, 10),
            "ok_tag": _DT(2024, 1, 1),
            "missing_tag": _DT(2024, 1, 1),
        }
        current = {
            "stale_tag": _DT(2024, 1, 5),
            "ok_tag": _DT(2024, 1, 5),
            "no_wm_tag": _DT(2024, 1, 5),
        }
        output = self._run(watermarks, current, list_mode=True)
        self.assertIn("STALE", output)
        self.assertIn("OK", output)
        self.assertIn("MISSING", output)
        self.assertIn("NO_WATERMARK", output)
        self.assertIn("stale_tag", output)
        self.assertIn("ok_tag", output)
        self.assertIn("missing_tag", output)
        self.assertIn("no_wm_tag", output)
