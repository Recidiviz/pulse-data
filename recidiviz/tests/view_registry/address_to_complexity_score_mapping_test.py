# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for recidiviz/view_registry/address_to_complexity_score_mapping_test.py"""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.address_to_complexity_score_mapping import (
    ParentAddressComplexityScoreMapper,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders

_RAW_DATA_LATEST_VIEW_ADDRESS = BigQueryAddress.from_str(
    "us_nd_raw_data_up_to_date_views.elite_offenders_latest"
)
_RAW_DATA_SOURCE_TABLE_ADDRESS = BigQueryAddress.from_str(
    "us_nd_raw_data.elite_offenders"
)
_STATE_SPECIFIC_VIEW_MATERIALIZED_TABLE_ADDRESS = BigQueryAddress.from_str(
    "reference_views.us_ix_location_metadata_materialized"
)
_STATE_AGNOSTIC_VIEW_ADDRESS = BigQueryAddress.from_str("sessions.dataflow_sessions")
_STATE_AGNOSTIC_VIEW_2_MATERIALIZED_TABLE_ADDRESS = BigQueryAddress.from_str(
    "sessions.session_location_names_materialized"
)
_STATE_AGNOSTIC_UNION_ALL_VIEW_ADDRESS = BigQueryAddress.from_str(
    "normalized_state_views.state_person_view"
)


class TestParentAddressComplexityScoreMapper(unittest.TestCase):
    """Tests for ParentAddressComplexityScoreMapper"""

    mapper: ParentAddressComplexityScoreMapper

    @classmethod
    def setUpClass(cls) -> None:
        project_id = "recidiviz-staging"
        with local_project_id_override(project_id):
            source_table_repository = (
                build_source_table_repository_for_collected_schemata(
                    project_id=project_id
                )
            )
            all_view_builders = deployed_view_builders()

        cls.mapper = ParentAddressComplexityScoreMapper(
            source_table_repository=source_table_repository,
            all_view_builders=all_view_builders,
        )

    def test_get_parent_complexity_map_for_view_2025_regular(self) -> None:
        regular_view_map = self.mapper.get_parent_complexity_map_for_view_2025(
            _STATE_AGNOSTIC_VIEW_ADDRESS
        )

        # Referencing a raw data table / view directly in a normal view would cost 10
        # complexity points.
        self.assertEqual(10, regular_view_map[_RAW_DATA_LATEST_VIEW_ADDRESS])

        # Referencing a non-raw data, state-specific view would cost 5 complexity
        # points.
        self.assertEqual(
            5, regular_view_map[_STATE_SPECIFIC_VIEW_MATERIALIZED_TABLE_ADDRESS]
        )

        # Referencing a state-agnostic table is one point.
        self.assertEqual(
            1, regular_view_map[_STATE_AGNOSTIC_VIEW_2_MATERIALIZED_TABLE_ADDRESS]
        )

    def test_get_parent_complexity_map_for_view_2025_union_all(self) -> None:

        union_all_view_map = self.mapper.get_parent_complexity_map_for_view_2025(
            _STATE_AGNOSTIC_UNION_ALL_VIEW_ADDRESS
        )

        # Do not penalize referencing a state-specific view in the context of a
        # UnionAllBigQueryViewBuilder view.
        self.assertEqual(
            1, union_all_view_map[_STATE_SPECIFIC_VIEW_MATERIALIZED_TABLE_ADDRESS]
        )

        # Referencing a raw data table is still very expensive
        self.assertEqual(10, union_all_view_map[_RAW_DATA_LATEST_VIEW_ADDRESS])

        # Referencing a state-agnostic table is still one point.
        self.assertEqual(
            1, union_all_view_map[_STATE_AGNOSTIC_VIEW_2_MATERIALIZED_TABLE_ADDRESS]
        )

    def test_get_parent_complexity_map_for_view_2025_latest_view(self) -> None:
        raw_data_ok_view_map = self.mapper.get_parent_complexity_map_for_view_2025(
            _RAW_DATA_LATEST_VIEW_ADDRESS
        )

        # Do not penalize for referencing a raw data view inside a raw data latest view
        self.assertEqual(1, raw_data_ok_view_map[_RAW_DATA_SOURCE_TABLE_ADDRESS])

        # Other views are normal points
        self.assertEqual(
            5, raw_data_ok_view_map[_STATE_SPECIFIC_VIEW_MATERIALIZED_TABLE_ADDRESS]
        )
        self.assertEqual(
            1,
            raw_data_ok_view_map[_STATE_AGNOSTIC_VIEW_2_MATERIALIZED_TABLE_ADDRESS],
        )
