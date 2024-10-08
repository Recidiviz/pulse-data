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
"""Tests for deployed_address_schema_utils.py"""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.deployed_address_schema_utils import (
    STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS,
    get_deployed_addresses_without_state_code_column,
    state_agnostic_deployed_views_without_state_code_column,
    state_specific_deployed_views_without_state_code_columns,
)
from recidiviz.view_registry.deployed_views import all_deployed_view_builders


class TestDeployedAddressesWithoutStateCodeColumn(unittest.TestCase):
    """Tests for deployed_address_schema_utils.py"""

    view_builders_by_address: dict[BigQueryAddress, BigQueryViewBuilder]
    all_source_tables_to_configs: dict[BigQueryAddress, SourceTableConfig]

    @classmethod
    def setUpClass(cls) -> None:
        cls.view_builders_by_address = {
            vb.address: vb for vb in all_deployed_view_builders()
        }
        cls.all_source_tables_to_configs = {}

        for project_id in GCP_PROJECTS:
            with local_project_id_override(project_id):
                cls.all_source_tables_to_configs = {
                    **cls.all_source_tables_to_configs,
                    **build_source_table_repository_for_collected_schemata(
                        project_id=project_id
                    ).source_tables,
                }

    def test_get_deployed_addresses_without_state_code_column(self) -> None:
        for project_id in GCP_PROJECTS:
            with local_project_id_override(project_id):
                no_state_code_addresses = (
                    get_deployed_addresses_without_state_code_column(project_id)
                )
                for a in no_state_code_addresses:
                    self.assertIsInstance(a, BigQueryAddress)

    def test_state_agnostic_mapping_tables(self) -> None:
        for address in STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS:
            view_builder = self.view_builders_by_address[address]

            with local_project_id_override("recidiviz-456"):
                view: BigQueryView = view_builder.build()

            # Mappings tables should generally have no parent tables, i.e. they are
            # usually tables that build data from an UNNEST statement
            if not view.parent_tables:
                continue

            for parent_table in view.parent_tables:
                if (
                    parent_table not in self.all_source_tables_to_configs
                    and parent_table
                    not in STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS
                ):
                    raise ValueError(
                        f"Found view [{view_builder.address.to_str()}] that is listed "
                        f"in STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS, "
                        f"but has parent [{parent_table.to_str()}] that is neither a "
                        f"source table nor another view in "
                        f"STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS. "
                        f"This view does not qualify as a 'mappings view'."
                    )

                parent_has_state_code = self.all_source_tables_to_configs[
                    parent_table
                ].has_column("state_code")
                if parent_has_state_code:
                    raise ValueError(
                        f"Found view [{view_builder.address.to_str()}] that is listed "
                        f"in STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS, "
                        f"but has parent [{parent_table.to_str()}] that has a "
                        f"state_code column. This view should have a state_code column."
                    )

    def test_state_agnostic_deployed_views_without_state_code_column(self) -> None:
        for address in state_agnostic_deployed_views_without_state_code_column(
            self.view_builders_by_address
        ):
            self.assertFalse(
                address.is_state_specific_address(),
                f"Found address in state-agnostic views list that is state-specific: "
                f"{address.to_str()}",
            )

    def test_state_specific_deployed_views_without_state_code_columns(self) -> None:
        for address in state_specific_deployed_views_without_state_code_columns(
            self.view_builders_by_address
        ):
            self.assertTrue(
                address.is_state_specific_address(),
                f"Found address in state-specific views list that is state-agnostic: "
                f"{address.to_str()}",
            )
