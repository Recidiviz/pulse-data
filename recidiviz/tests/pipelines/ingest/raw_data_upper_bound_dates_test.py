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
"""Tests for the validate_and_backfill_raw_data_upper_bound_dates helper."""
import unittest
from unittest import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.raw_data_upper_bound_dates import (
    validate_and_backfill_raw_data_upper_bound_dates,
)

_HELPER_MODULE = "recidiviz.pipelines.ingest.raw_data_upper_bound_dates"


class TestValidateAndBackfillRawDataUpperBoundDates(unittest.TestCase):
    """Unit tests for `validate_and_backfill_raw_data_upper_bound_dates`."""

    def setUp(self) -> None:
        # All raw file tags considered valid for the (mocked) region.
        self.raw_file_tags = {
            "file_a",
            "file_b",
            "file_c",
            "file_d",
        }
        raw_file_config_patcher = mock.patch(
            f"{_HELPER_MODULE}.DirectIngestRegionRawFileConfig"
        )
        mock_raw_file_config_cls = raw_file_config_patcher.start()
        self.addCleanup(raw_file_config_patcher.stop)
        mock_config = mock.MagicMock()
        mock_config.raw_file_tags = self.raw_file_tags
        mock_raw_file_config_cls.return_value = mock_config

        self.region = mock.MagicMock()
        self.state_code = StateCode.US_XX

    def _view_collector(self, view_deps: dict[str, set[str]]) -> mock.MagicMock:
        """Returns a mock view_collector whose `get_query_builder_by_view_name`
        emits a builder with the dependencies declared in `view_deps`."""
        collector = mock.MagicMock()

        def get_query_builder_by_view_name(name: str) -> mock.MagicMock:
            builder = mock.MagicMock()
            builder.raw_data_table_dependency_file_tags = view_deps[name]
            return builder

        collector.get_query_builder_by_view_name.side_effect = (
            get_query_builder_by_view_name
        )
        return collector

    def test_happy_path_all_deps_hydrated(self) -> None:
        result = validate_and_backfill_raw_data_upper_bound_dates(
            state_code=self.state_code,
            region=self.region,
            view_collector=self._view_collector({"view_1": {"file_a", "file_b"}}),
            view_names=["view_1"],
            raw_data_upper_bound_dates={
                "file_a": "2024-01-01T00:00:00",
                "file_b": "2024-01-02T00:00:00",
            },
        )
        self.assertEqual(
            result,
            {
                "file_a": "2024-01-01T00:00:00",
                "file_b": "2024-01-02T00:00:00",
            },
        )

    def test_unexpected_file_tag_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            (
                r"Found unexpected file tags in raw_data_upper_bound_dates\. "
                r"These are not valid raw file tags for \[US_XX\]"
            ),
        ):
            validate_and_backfill_raw_data_upper_bound_dates(
                state_code=self.state_code,
                region=self.region,
                view_collector=self._view_collector({}),
                view_names=[],
                raw_data_upper_bound_dates={
                    "file_a": "2024-01-01T00:00:00",
                    "not_a_real_tag": "2024-01-01T00:00:00",
                },
            )

    def test_missing_dependency_not_in_allowlist_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found dependency table\(s\) of ingest view \[view_1\] with no data: ",
        ):
            validate_and_backfill_raw_data_upper_bound_dates(
                state_code=self.state_code,
                region=self.region,
                view_collector=self._view_collector({"view_1": {"file_a", "file_b"}}),
                view_names=["view_1"],
                raw_data_upper_bound_dates={"file_a": "2024-01-01T00:00:00"},
            )

    @mock.patch.dict(
        f"{_HELPER_MODULE}.RAW_DATA_TABLES_ALLOWED_EMPTY_BY_INGEST_VIEW",
        {StateCode.US_XX: {"view_1": {"file_b"}}},
    )
    def test_missing_dependency_in_allowlist_is_backfilled(self) -> None:
        result = validate_and_backfill_raw_data_upper_bound_dates(
            state_code=self.state_code,
            region=self.region,
            view_collector=self._view_collector({"view_1": {"file_a", "file_b"}}),
            view_names=["view_1"],
            raw_data_upper_bound_dates={"file_a": "2024-07-04T00:00:00"},
        )
        self.assertEqual(
            result,
            {
                "file_a": "2024-07-04T00:00:00",
                "file_b": "2024-07-04T00:00:00",
            },
        )

    @mock.patch.dict(
        f"{_HELPER_MODULE}.RAW_DATA_TABLES_ALLOWED_EMPTY_BY_INGEST_VIEW",
        {StateCode.US_XX: {"view_1": {"file_b"}}},
    )
    def test_backfill_uses_max_peer_date(self) -> None:
        result = validate_and_backfill_raw_data_upper_bound_dates(
            state_code=self.state_code,
            region=self.region,
            view_collector=self._view_collector(
                {"view_1": {"file_a", "file_b", "file_c"}}
            ),
            view_names=["view_1"],
            raw_data_upper_bound_dates={
                "file_a": "2024-01-01T00:00:00",
                "file_c": "2024-05-01T00:00:00",
            },
        )
        # file_b is backfilled with the max of its populated peers (file_a, file_c).
        self.assertEqual(result["file_b"], "2024-05-01T00:00:00")

    @mock.patch.dict(
        f"{_HELPER_MODULE}.RAW_DATA_TABLES_ALLOWED_EMPTY_BY_INGEST_VIEW",
        {StateCode.US_XX: {"view_1": {"file_a", "file_b"}}},
    )
    def test_all_dependencies_allowed_empty_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            (
                r"At least one raw data dependency of view \[view_1\] must "
                r"be hydrated"
            ),
        ):
            validate_and_backfill_raw_data_upper_bound_dates(
                state_code=self.state_code,
                region=self.region,
                view_collector=self._view_collector({"view_1": {"file_a", "file_b"}}),
                view_names=["view_1"],
                raw_data_upper_bound_dates={},
            )

    @mock.patch.dict(
        f"{_HELPER_MODULE}.RAW_DATA_TABLES_ALLOWED_EMPTY_BY_INGEST_VIEW",
        {StateCode.US_XX: {"view_1": {"file_b"}, "view_2": {"file_b"}}},
    )
    def test_later_view_sees_earlier_backfilled_date(self) -> None:
        result = validate_and_backfill_raw_data_upper_bound_dates(
            state_code=self.state_code,
            region=self.region,
            view_collector=self._view_collector(
                {
                    "view_1": {"file_a", "file_b"},
                    "view_2": {"file_b", "file_c"},
                }
            ),
            view_names=["view_1", "view_2"],
            raw_data_upper_bound_dates={
                "file_a": "2024-01-01T00:00:00",
                "file_c": "2024-05-01T00:00:00",
            },
        )
        # view_1 backfills file_b from its only populated peer (file_a). view_2
        # then sees file_b as already populated and does not re-backfill it
        # from file_c.
        self.assertEqual(result["file_b"], "2024-01-01T00:00:00")

    def test_does_not_mutate_input_dict(self) -> None:
        input_dates = {
            "file_a": "2024-01-01T00:00:00",
            "file_b": "2024-01-02T00:00:00",
        }
        validate_and_backfill_raw_data_upper_bound_dates(
            state_code=self.state_code,
            region=self.region,
            view_collector=self._view_collector({"view_1": {"file_a", "file_b"}}),
            view_names=["view_1"],
            raw_data_upper_bound_dates=input_dates,
        )
        self.assertEqual(
            input_dates,
            {
                "file_a": "2024-01-01T00:00:00",
                "file_b": "2024-01-02T00:00:00",
            },
        )

    def test_empty_view_names_returns_input_unchanged(self) -> None:
        result = validate_and_backfill_raw_data_upper_bound_dates(
            state_code=self.state_code,
            region=self.region,
            view_collector=self._view_collector({}),
            view_names=[],
            raw_data_upper_bound_dates={"file_a": "2024-01-01T00:00:00"},
        )
        self.assertEqual(result, {"file_a": "2024-01-01T00:00:00"})
