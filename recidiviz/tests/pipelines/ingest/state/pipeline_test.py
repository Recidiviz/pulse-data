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
import json
import re
from types import ModuleType
from typing import Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.pipelines.ingest.state.normalization import (
    normalize_state_person,
    normalize_state_staff,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)
from recidiviz.tests.pipelines.ingest.state.pipeline_test_case import (
    StateIngestPipelineTestCase,
)

INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME = "ingest_integration"


class TestStateIngestPipeline(StateIngestPipelineTestCase):
    """Tests the state ingest pipeline all the way through using state code US_DD."""

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_DD

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return fake_regions

    def setUp(self) -> None:
        super().setUp()
        self.us_dd_upper_date_bound_overrides = json.dumps(
            {
                # Fake upper bound dates for US_DD region
                "table1": "2022-07-04T00:00:00.000000",
                "table2": "2022-07-04T00:00:00.000000",
                "table3": "2022-07-04T00:00:00.000000",
                "table4": "2022-07-04T00:00:00.000000",
                "table5": "2023-07-05T00:00:00.000000",
                "table6": None,
                "table7": "2023-07-04T00:00:00.000000",
            }
        )
        self.person_delegate_patchers = start_pipeline_delegate_getter_patchers(
            normalize_state_person
        )
        self.staff_delegate_patchers = start_pipeline_delegate_getter_patchers(
            normalize_state_staff
        )
        self.generic_pipeline_delegate_patchers = (
            start_pipeline_delegate_getter_patchers(pipeline)
        )

    def tearDown(self) -> None:
        super().tearDown()
        for patcher in self.person_delegate_patchers:
            patcher.stop()
        for patcher in self.staff_delegate_patchers:
            patcher.stop()
        for patcher in self.generic_pipeline_delegate_patchers:
            patcher.stop()

    def test_state_ingest_pipeline(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )
        self.run_test_ingest_pipeline(
            test_name="integration_simple",
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
        )

    def test_state_ingest_pipeline_ingest_view_results_only(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )
        self.run_test_ingest_pipeline(
            test_name="integration_ingest_view_results_only",
            ingest_view_results_only=True,
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
        )

    def test_state_ingest_pipeline_pre_normalization_only(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )
        self.run_test_ingest_pipeline(
            test_name="integration_pre_normalization_only",
            pre_normalization_only=True,
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
        )

    def test_state_ingest_pipeline_ingest_views_to_run_subset(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )
        subset_of_ingest_views = ["ingest12"]
        self.run_test_ingest_pipeline(
            test_name="integration_ingest_views_to_run_subset",
            ingest_views_to_run=" ".join(subset_of_ingest_views),
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
        )

    def test_missing_raw_data_upper_bound_dates(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found dependency table(s) of ingest view "
                "[ingestMultipleRootExternalIds] with no data: {'table3'}"
            ),
        ):
            self.run_test_ingest_pipeline(
                test_name="missing_upper_bounds",
                raw_data_upper_bound_dates_json_override=json.dumps(
                    {
                        # Fake upper bound dates for US_DD region, missing table3
                        "table1": "2022-07-04T00:00:00.000000",
                        "table2": "2022-07-04T00:00:00.000000",
                        "table4": "2022-07-04T00:00:00.000000",
                        "table5": "2023-07-05T00:00:00.000000",
                        "table6": None,
                        "table7": "2023-07-04T00:00:00.000000",
                    }
                ),
            )

    def test_state_ingest_pipeline_overwrites_all_tables_each_time(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )

        # First run a pipeline with ALL views enabled. We should get the same results
        # as the basic pipeline integration test.
        self.run_test_ingest_pipeline(
            test_name="integration_simple",
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
        )

        # Next, run with a subset of ingest views. The result should match the result
        # if we had just run with this ingest view without running another pipeline
        # first, i.e. we should fully overwrite the output dataset.
        subset_of_ingest_views = ["ingest12"]
        self.run_test_ingest_pipeline(
            test_name="integration_ingest_views_to_run_subset",
            ingest_views_to_run=" ".join(subset_of_ingest_views),
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
        )
