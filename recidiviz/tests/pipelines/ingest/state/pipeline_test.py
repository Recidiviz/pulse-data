# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests the state ingest pipeline."""
import unittest
from typing import Any, Dict, List

from mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.pipelines.ingest.utils.ingest_view_query_helpers import (
    LOWER_BOUND_DATETIME_COL_NAME,
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadAllFromBigQuery,
    FakeReadFromBigQuery,
    FakeWriteExactOutputToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import run_test_pipeline
from recidiviz.tests.utils.fake_region import fake_region


class TestStateIngestPipeline(unittest.TestCase):
    """Tests the state ingest pipeline."""

    def setUp(self) -> None:
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteExactOutputToBigQuery
        )

        self.pipeline_class = pipeline.StateIngestPipeline

    # TODO(#22059): Update unittesting to read raw data for the fake region from fixture
    # CSV files rather than from in-memory structures.
    # TODO(#22059): Improve unittesting to check if actual output is returned based on
    # expected input.
    def run_test_pipeline(
        self,
        state_code: str,
        raw_table_date_values: List[Dict[str, Any]],
        ingest_view_results: List[Dict[str, Any]],
    ) -> None:
        """Runs a test version of the state ingest pipeline."""
        project = "recidiviz-staging"
        dataset = "dataset"

        # pylint: disable=unused-argument
        def create_fake_bq_source_constructor(
            query: str,
        ) -> FakeReadFromBigQuery:
            return FakeReadFromBigQuery(table_values=raw_table_date_values)

        def create_fake_bq_read_all_source_constructor() -> FakeReadAllFromBigQuery:
            return FakeReadAllFromBigQuery(table_values=ingest_view_results)

        read_from_bq_constructor = create_fake_bq_source_constructor
        read_all_from_bq_constructor = create_fake_bq_read_all_source_constructor
        write_to_bq_constructor = (
            self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
                expected_dataset=dataset,
                expected_output_tags=[],
                expected_output=ingest_view_results,
            )
        )

        run_test_pipeline(
            pipeline_cls=self.pipeline_class,
            state_code=state_code,
            project_id=project,
            dataset_id=dataset,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            read_all_from_bq_constructor=read_all_from_bq_constructor,
        )

    @patch("recidiviz.utils.metadata.project_id", return_value="recidiviz-staging")
    @patch(
        "recidiviz.ingest.direct.views.direct_ingest_view_query_builder"
        ".get_region_raw_file_config"
    )
    @patch("recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region")
    def test_state_ingest_pipeline(
        self,
        mock_region: MagicMock,
        mock_raw_file_config: MagicMock,
        _mock_project_id: MagicMock,
    ) -> None:
        mock_region.return_value = fake_region(
            region_code="us_xx", environment="staging", region_module=fake_regions
        )
        mock_raw_file_config.return_value = DirectIngestRegionRawFileConfig(
            region_code=StateCode.US_XX.value, region_module=fake_regions
        )
        self.run_test_pipeline(
            "US_XX",
            [
                {
                    UPPER_BOUND_DATETIME_COL_NAME: "2023-07-07",
                    LOWER_BOUND_DATETIME_COL_NAME: "2023-07-06",
                }
            ],
            [
                {
                    "COL1": "value1",
                    "COL2": "value2",
                    "COL3": "value3",
                    UPPER_BOUND_DATETIME_COL_NAME: "2023-07-07",
                    LOWER_BOUND_DATETIME_COL_NAME: "2023-07-06",
                    MATERIALIZATION_TIME_COL_NAME: "2023-07-07 00:00:00",
                }
            ],
        )
