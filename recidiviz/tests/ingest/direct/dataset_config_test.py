#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for raw_data/dataset_config.py."""
import unittest

from recidiviz.calculator.query.state.dataset_config import state_dataset_for_state_code
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataset,
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class TestDatasetConfig(unittest.TestCase):
    """Tests for dataset_config.py."""

    def test_raw_tables_dataset_for_region_primary(self) -> None:
        self.assertEqual(
            "us_xx_raw_data",
            raw_tables_dataset_for_region(
                state_code=StateCode.US_XX, instance=DirectIngestInstance.PRIMARY
            ),
        )

    def test_raw_tables_dataset_for_region_secondary(self) -> None:
        self.assertEqual(
            "us_xx_raw_data_secondary",
            raw_tables_dataset_for_region(
                state_code=StateCode.US_XX, instance=DirectIngestInstance.SECONDARY
            ),
        )

    def test_latest_views_dataset_for_region_secondary(self) -> None:
        self.assertEqual(
            "us_xx_raw_data_up_to_date_views_secondary",
            raw_latest_views_dataset_for_region(
                state_code=StateCode.US_XX, instance=DirectIngestInstance.SECONDARY
            ),
        )

    def test_latest_views_dataset_for_region_primary(self) -> None:
        self.assertEqual(
            "us_xx_raw_data_up_to_date_views",
            raw_latest_views_dataset_for_region(
                state_code=StateCode.US_XX, instance=DirectIngestInstance.PRIMARY
            ),
        )

    def test_ingest_view_materialization_results_dataset_primary(self) -> None:
        self.assertEqual(
            "us_xx_dataflow_ingest_view_results_primary",
            ingest_view_materialization_results_dataset(
                state_code=StateCode.US_XX, instance=DirectIngestInstance.PRIMARY
            ),
        )

    def test_ingest_view_materialization_results_dataset_secondary(
        self,
    ) -> None:
        self.assertEqual(
            "us_xx_dataflow_ingest_view_results_secondary",
            ingest_view_materialization_results_dataset(
                state_code=StateCode.US_XX, instance=DirectIngestInstance.SECONDARY
            ),
        )

    def test_state_dataset_for_state_code(
        self,
    ) -> None:
        self.assertEqual(
            "us_xx_state_secondary",
            state_dataset_for_state_code(
                state_code=StateCode.US_XX, instance=DirectIngestInstance.SECONDARY
            ),
        )
        self.assertEqual(
            "us_yy_state_primary",
            state_dataset_for_state_code(
                state_code=StateCode.US_YY, instance=DirectIngestInstance.PRIMARY
            ),
        )
