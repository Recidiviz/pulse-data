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

    def tearDown(self) -> None:
        super().tearDown()
        for patcher in self.person_delegate_patchers:
            patcher.stop()
        for patcher in self.staff_delegate_patchers:
            patcher.stop()

    def test_state_ingest_pipeline(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )
        self.run_test_ingest_pipeline(
            test_name="integration_simple",
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
            run_normalization_override=True,
        )

    # TODO(#29517): We should be able to delete this test once we've enabled
    #  normalization in ingest for all states.
    def test_state_ingest_pipeline_no_normalization(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )
        self.run_test_ingest_pipeline(
            test_name="integration_simple_no_normalization",
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
            run_normalization_override=False,
        )

    def test_state_ingest_pipeline_ingest_view_results_only(self) -> None:
        self.setup_region_raw_data_bq_tables(
            test_name=INTEGRATION_RAW_DATA_INPUTS_FIXTURES_NAME
        )
        self.run_test_ingest_pipeline(
            test_name="integration_ingest_view_results_only",
            ingest_view_results_only=True,
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_date_bound_overrides,
            # Even if the run_normalization flag is True, we still should only produce
            # ingest view results!
            run_normalization_override=True,
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
            run_normalization_override=True,
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
                run_normalization_override=True,
            )
