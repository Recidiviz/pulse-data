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
"""Tests for find_direct_raw_data_references.py."""
import unittest
from typing import Optional
from unittest.mock import patch

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    RAW_DATA_LATEST_VIEW_ID_SUFFIX,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.tools.find_direct_raw_data_references import (
    find_direct_raw_data_references,
)

RAW_DATASET_XX = "us_xx_raw_data"
RAW_DATASET_XX_LATEST = "us_xx_raw_data_up_to_date_views"
RAW_DATASET_YY = "us_yy_raw_data"
SOURCE_TABLE_1 = "source_table"
SOURCE_TABLE_2 = "source_table_2"


class TestFindDirectRawDataReferences(unittest.TestCase):
    """Tests for finding views that reference raw data tables/views directly."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        ingest_states = [StateCode.US_XX, StateCode.US_YY]

        self.mock_get_existing_direct_ingest_states = patch(
            "recidiviz.ingest.direct.dataset_helpers.get_existing_direct_ingest_states"
        )
        self.mock_get_existing_direct_ingest_states.start().return_value = ingest_states

        self.mock_get_existing_direct_ingest_states_2 = patch(
            "recidiviz.ingest.direct.regions.direct_ingest_region_utils.get_existing_direct_ingest_states"
        )
        self.mock_get_existing_direct_ingest_states_2.start().return_value = (
            ingest_states
        )

        regions_by_state = {}
        for state in ingest_states:
            regions_by_state[state.value.lower()] = fake_region(
                region_code=state.value.lower(), region_module=fake_regions
            )

        self.mock_fake_regions = patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region",
            new=lambda region_code: regions_by_state[region_code.lower()],
        )
        self.mock_fake_regions.start()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.mock_get_existing_direct_ingest_states.stop()
        self.mock_get_existing_direct_ingest_states_2.stop()
        self.mock_fake_regions.stop()

    def test_downstream_view_is_excluded(self) -> None:
        view1 = self._create_view_builder(
            "dataset_1",
            "table_1",
            self._build_view_address_string(RAW_DATASET_XX, SOURCE_TABLE_1),
        )
        view2 = self._create_view_builder(
            "dataset_2",
            "table_2",
            self._build_view_address_string(RAW_DATASET_XX, SOURCE_TABLE_2),
        )
        view3 = self._create_view_builder(
            "dataset_3",
            "table_3",
            self._build_view_address_string("dataset_1", "table_1"),
        )

        view_list = [view1, view2, view3]
        references = find_direct_raw_data_references(view_list)

        expected_references = {
            StateCode.US_XX: {
                SOURCE_TABLE_1: {view1.address},
                SOURCE_TABLE_2: {view2.address},
            }
        }

        self.assertEqual(references, expected_references)

    def test_view_queries_different_states(self) -> None:
        view = self._create_view_builder(
            "dataset_1",
            "table_1",
            self._build_view_address_string(RAW_DATASET_XX, SOURCE_TABLE_1),
            self._build_view_address_string(RAW_DATASET_YY, SOURCE_TABLE_2),
        )

        view_list = [view]
        references = find_direct_raw_data_references(view_list)

        expected_references = {
            StateCode.US_XX: {
                SOURCE_TABLE_1: {view.address},
            },
            StateCode.US_YY: {
                SOURCE_TABLE_2: {view.address},
            },
        }

        self.assertEqual(references, expected_references)

    def test_view_queries_different_tables_same_state(self) -> None:
        view = self._create_view_builder(
            "dataset_1",
            "table_1",
            self._build_view_address_string(RAW_DATASET_XX, SOURCE_TABLE_1),
            self._build_view_address_string(RAW_DATASET_XX, SOURCE_TABLE_2),
        )

        view_list = [view]
        references = find_direct_raw_data_references(view_list)

        expected_references = {
            StateCode.US_XX: {
                SOURCE_TABLE_1: {view.address},
                SOURCE_TABLE_2: {view.address},
            }
        }

        self.assertEqual(references, expected_references)

    def test_no_distinction_between_latest_and_raw_datasets(self) -> None:
        view1 = self._create_view_builder(
            "dataset_1",
            "table_1",
            self._build_view_address_string(RAW_DATASET_XX, SOURCE_TABLE_1),
            self._build_view_address_string(
                RAW_DATASET_XX_LATEST,
                f"{SOURCE_TABLE_1}{RAW_DATA_LATEST_VIEW_ID_SUFFIX}",
            ),
        )
        view2 = self._create_view_builder(
            "dataset_2",
            "table_2",
            self._build_view_address_string(
                RAW_DATASET_XX_LATEST,
                f"{SOURCE_TABLE_1}{RAW_DATA_LATEST_VIEW_ID_SUFFIX}",
            ),
        )

        view_list = [view1, view2]
        references = find_direct_raw_data_references(view_list)

        expected_references = {
            StateCode.US_XX: {
                SOURCE_TABLE_1: {view1.address, view2.address},
            }
        }

        self.assertEqual(references, expected_references)

    def _build_view_address_string(self, dataset_id: str, table_id: str) -> str:
        return f"{dataset_id}.{table_id}"

    def _create_view_builder(
        self,
        dataset_id: str,
        view_id: str,
        source_address: str,
        source_address_2: Optional[str] = None,
    ) -> BigQueryViewBuilder:
        query_template = f"SELECT * FROM `{{project_id}}.{source_address}`"
        query_template += (
            f"JOIN `{{project_id}}.{source_address_2}` USING (col)"
            if source_address_2
            else ""
        )

        return SimpleBigQueryViewBuilder(
            dataset_id=dataset_id,
            view_id=view_id,
            description=f"{view_id} description",
            view_query_template=query_template,
        )
