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
"""Tests for StateIngestViewAndMappingTestCase.resolve_raw_data_datetime_upper_bound."""
import datetime
import os
import tempfile
import unittest
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct.regions.ingest_view_query_test_case import (
    StateIngestViewAndMappingTestCase,
)

# tagBasicData has a per-test fixture; tagPipeSeparatedNonUTF8 is a code file, so it
# has a single fixture shared by every test in the state.
_VIEW_WITH_UPDATE_TIMESTAMP = DirectIngestViewQueryBuilder(
    region="us_xx",
    ingest_view_name="basic_with_update_timestamp",
    view_query_template=(
        "SELECT * FROM {tagBasicData} "
        "JOIN {tagPipeSeparatedNonUTF8} ON COL1 = PRIMARY_COL1 "
        "WHERE COL3 <= @update_timestamp"
    ),
    region_module=fake_regions_module,
)

_VIEW_WITHOUT_UPDATE_TIMESTAMP = DirectIngestViewQueryBuilder(
    region="us_xx",
    ingest_view_name="basic_without_update_timestamp",
    view_query_template=(
        "SELECT * FROM {tagBasicData} "
        "JOIN {tagPipeSeparatedNonUTF8} ON COL1 = PRIMARY_COL1"
    ),
    region_module=fake_regions_module,
)


def _test_case_for_view(
    *,
    ingest_view_builder: DirectIngestViewQueryBuilder,
    class_raw_data_datetime_upper_bound: datetime.datetime | None,
) -> type[StateIngestViewAndMappingTestCase]:
    """Returns a test case class for |ingest_view_builder| with |class_raw_data_datetime_upper_bound|
    as its class-level raw_data_datetime_upper_bound. Defined inside a function so pytest does not
    collect it and run the emulator tests it inherits.
    """

    class _FakeIngestViewTest(StateIngestViewAndMappingTestCase):
        __test__ = True

        raw_data_datetime_upper_bound = class_raw_data_datetime_upper_bound

        @classmethod
        def state_code(cls) -> StateCode:
            return StateCode.US_XX

        @classmethod
        def ingest_view_builder(cls) -> DirectIngestViewQueryBuilder:
            return ingest_view_builder

    return _FakeIngestViewTest


class ResolveRawDataUpperBoundDatetimeTest(unittest.TestCase):
    """Tests for StateIngestViewAndMappingTestCase.resolve_raw_data_datetime_upper_bound."""

    def setUp(self) -> None:
        self.temp_dir = self.enterContext(
            tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        )
        self.enterContext(
            patch(
                "recidiviz.tests.ingest.direct.raw_data_fixture."
                "fixture_path_for_raw_file_config",
                side_effect=self._fixture_path,
            )
        )

    def _fixture_path(
        self,
        _state_code: StateCode,
        raw_file_config: DirectIngestRawFileConfig,
        _file_name: str,
    ) -> str:
        return os.path.join(self.temp_dir, f"{raw_file_config.file_tag}.csv")

    def _write_fixtures(self, *, basic_data: str, code_file: str) -> None:
        for file_tag, contents in [
            ("tagBasicData", basic_data),
            ("tagPipeSeparatedNonUTF8", code_file),
        ]:
            with open(
                os.path.join(self.temp_dir, f"{file_tag}.csv"), "w", encoding="utf-8"
            ) as f:
                f.write(contents)

    def test_class_upper_bound_is_used_without_test_upper_bound(
        self,
    ) -> None:
        self._write_fixtures(
            basic_data=(
                "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
                "A100,Main St,Active,1,2025-01-15,False\n"
            ),
            code_file=(
                "PRIMARY_COL1,COL2,COL3,COL4,file_id,update_datetime,is_deleted\n"
                "A100,North,Region 1,Open,7,2024-06-01,False\n"
            ),
        )
        self.assertEqual(
            datetime.datetime(2025, 3, 1),
            _test_case_for_view(
                ingest_view_builder=_VIEW_WITH_UPDATE_TIMESTAMP,
                class_raw_data_datetime_upper_bound=datetime.datetime(2025, 3, 1),
            ).resolve_raw_data_datetime_upper_bound(
                characteristic="basic", test_raw_data_datetime_upper_bound=None
            ),
        )

    def test_test_upper_bound_overrides_class_upper_bound(
        self,
    ) -> None:
        self._write_fixtures(
            basic_data=(
                "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
                "A100,Main St,Active,1,2025-01-15,False\n"
            ),
            code_file=(
                "PRIMARY_COL1,COL2,COL3,COL4,file_id,update_datetime,is_deleted\n"
                "A100,North,Region 1,Open,7,2024-06-01,False\n"
            ),
        )
        self.assertEqual(
            datetime.datetime(2025, 6, 30),
            _test_case_for_view(
                ingest_view_builder=_VIEW_WITH_UPDATE_TIMESTAMP,
                class_raw_data_datetime_upper_bound=datetime.datetime(2025, 3, 1),
            ).resolve_raw_data_datetime_upper_bound(
                characteristic="basic",
                test_raw_data_datetime_upper_bound=datetime.datetime(2025, 6, 30),
            ),
        )

    def test_upper_bound_on_latest_test_fixture_row_is_allowed(
        self,
    ) -> None:
        self._write_fixtures(
            basic_data=(
                "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
                "A100,Main St,Active,1,2025-01-15,False\n"
                "A100,Main St,Active,2,2025-02-11 21:00:14,False\n"
            ),
            code_file=(
                "PRIMARY_COL1,COL2,COL3,COL4,file_id,update_datetime,is_deleted\n"
                "A100,North,Region 1,Open,7,2024-06-01,False\n"
            ),
        )
        self.assertEqual(
            datetime.datetime(2025, 2, 11, 21, 0, 14),
            _test_case_for_view(
                ingest_view_builder=_VIEW_WITH_UPDATE_TIMESTAMP,
                class_raw_data_datetime_upper_bound=None,
            ).resolve_raw_data_datetime_upper_bound(
                characteristic="basic",
                test_raw_data_datetime_upper_bound=datetime.datetime(
                    2025, 2, 11, 21, 0, 14
                ),
            ),
        )

    def test_upper_bound_before_latest_test_fixture_row_raises(
        self,
    ) -> None:
        self._write_fixtures(
            basic_data=(
                "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
                "A100,Main St,Active,1,2025-01-15,False\n"
                "A100,Main St,Active,2,2025-02-11 21:00:14,False\n"
            ),
            code_file=(
                "PRIMARY_COL1,COL2,COL3,COL4,file_id,update_datetime,is_deleted\n"
                "A100,North,Region 1,Open,7,2024-06-01,False\n"
            ),
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^raw_data_datetime_upper_bound \[2025-02-01T00:00:00\] for the "
            r"\[basic_with_update_timestamp\] test for characteristic \[basic\] is "
            r"earlier than the latest row in raw data fixture \[tagBasicData\] "
            r"\(update_datetime \[2025-02-11T21:00:14\]\), so the query would drop "
            r"rows the test sets up\. Choose a raw_data_datetime_upper_bound on or after "
            r"\[2025-02-11T21:00:14\]\.$",
        ):
            _test_case_for_view(
                ingest_view_builder=_VIEW_WITH_UPDATE_TIMESTAMP,
                class_raw_data_datetime_upper_bound=None,
            ).resolve_raw_data_datetime_upper_bound(
                characteristic="basic",
                test_raw_data_datetime_upper_bound=datetime.datetime(2025, 2, 1),
            )

    def test_upper_bound_may_select_older_code_file_snapshot(
        self,
    ) -> None:
        self._write_fixtures(
            basic_data=(
                "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
                "A100,Main St,Active,1,2025-01-15,False\n"
            ),
            code_file=(
                "PRIMARY_COL1,COL2,COL3,COL4,file_id,update_datetime,is_deleted\n"
                "A100,North,Region 1,Open,7,2024-06-01,False\n"
                "A100,North,Region 1,Open,8,2025-03-01,False\n"
            ),
        )
        self.assertEqual(
            datetime.datetime(2025, 2, 1),
            _test_case_for_view(
                ingest_view_builder=_VIEW_WITH_UPDATE_TIMESTAMP,
                class_raw_data_datetime_upper_bound=None,
            ).resolve_raw_data_datetime_upper_bound(
                characteristic="basic",
                test_raw_data_datetime_upper_bound=datetime.datetime(2025, 2, 1),
            ),
        )

    def test_upper_bound_before_every_code_file_row_raises(
        self,
    ) -> None:
        self._write_fixtures(
            basic_data=(
                "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
                "A100,Main St,Active,1,2025-01-15,False\n"
            ),
            code_file=(
                "PRIMARY_COL1,COL2,COL3,COL4,file_id,update_datetime,is_deleted\n"
                "A100,North,Region 1,Open,7,2025-03-01,False\n"
            ),
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^raw_data_datetime_upper_bound \[2025-02-01T00:00:00\] for the "
            r"\[basic_with_update_timestamp\] test for characteristic \[basic\] is "
            r"earlier than every row in shared code file fixture "
            r"\[tagPipeSeparatedNonUTF8\] \(earliest update_datetime "
            r"\[2025-03-01T00:00:00\]\), so the query would read none of that "
            r"table\. Choose a raw_data_datetime_upper_bound on or after \[2025-03-01T00:00:00\]\.$",
        ):
            _test_case_for_view(
                ingest_view_builder=_VIEW_WITH_UPDATE_TIMESTAMP,
                class_raw_data_datetime_upper_bound=None,
            ).resolve_raw_data_datetime_upper_bound(
                characteristic="basic",
                test_raw_data_datetime_upper_bound=datetime.datetime(2025, 2, 1),
            )

    def test_view_without_update_timestamp_defaults_to_latest_row_including_code_files(
        self,
    ) -> None:
        self._write_fixtures(
            basic_data=(
                "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
                "A100,Main St,Active,1,2025-01-15,False\n"
            ),
            code_file=(
                "PRIMARY_COL1,COL2,COL3,COL4,file_id,update_datetime,is_deleted\n"
                "A100,North,Region 1,Open,7,2024-06-01,False\n"
                "A100,North,Region 1,Open,8,2025-03-01,False\n"
            ),
        )
        self.assertEqual(
            datetime.datetime(2025, 3, 1),
            _test_case_for_view(
                ingest_view_builder=_VIEW_WITHOUT_UPDATE_TIMESTAMP,
                class_raw_data_datetime_upper_bound=None,
            ).resolve_raw_data_datetime_upper_bound(
                characteristic="basic", test_raw_data_datetime_upper_bound=None
            ),
        )
