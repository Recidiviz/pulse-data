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
"""Tests for reference_query_providers.py"""
import unittest
from unittest.mock import patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.utils.reference_query_providers import (
    RootEntityIdFilteredQueryProvider,
    view_builders_as_state_filtered_query_providers,
)


class TestRootEntityIdFilteredQueryProvider(unittest.TestCase):
    """Tests for RootEntityIdFilteredQueryProvider"""

    def test_root_entity_id_filter(self) -> None:
        original_query = "SELECT * FROM `recidiviz-test.dataset.table`;"
        query_provider = RootEntityIdFilteredQueryProvider(
            original_query=original_query,
            root_entity_cls=entities.StatePerson,
            root_entity_id_filter_set={1, 4},
        )

        expected_query = """SELECT * FROM (
SELECT * FROM `recidiviz-test.dataset.table`
) WHERE person_id IN ('1', '4');
"""
        self.assertEqual(expected_query, query_provider.get_query())

    def test_root_entity_id_composed(self) -> None:
        original_query = StateFilteredQueryProvider(
            original_query="SELECT * FROM `recidiviz-test.dataset.table`;",
            state_code_filter=StateCode.US_XX,
        )
        query_provider = RootEntityIdFilteredQueryProvider(
            original_query=original_query,
            root_entity_cls=entities.StateStaff,
            root_entity_id_filter_set={1, 4},
        )
        expected_query = """SELECT * FROM (
SELECT * FROM (
SELECT * FROM `recidiviz-test.dataset.table`
) WHERE state_code = 'US_XX'
) WHERE staff_id IN ('1', '4');
"""
        self.assertEqual(expected_query, query_provider.get_query())


class TestViewBuildersAsProviders(unittest.TestCase):
    """Tests for view_builders_as_state_filtered_query_providers()"""

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_view_builders_as_state_filtered_query_providers(self) -> None:
        builder = SimpleBigQueryViewBuilder(
            dataset_id="dataset",
            view_id="my_view",
            description="description",
            view_query_template="SELECT * FROM `{project_id}.dataset.table`;",
        )
        providers_map = view_builders_as_state_filtered_query_providers(
            view_builders=[builder], state_code=StateCode.US_XX, address_overrides=None
        )

        self.assertEqual({"my_view"}, providers_map.keys())

        expected_query = """SELECT * FROM (
SELECT * FROM `recidiviz-456.dataset.table`
) WHERE state_code = 'US_XX';
"""
        self.assertEqual(expected_query, providers_map["my_view"].get_query())

    def test_view_builders_as_state_filtered_query_providers_with_overrides(
        self,
    ) -> None:
        builder = SimpleBigQueryViewBuilder(
            dataset_id="dataset",
            view_id="my_view",
            description="description",
            view_query_template="SELECT * FROM `{project_id}.dataset.table`;",
        )
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("dataset")
            .build()
        )
        providers_map = view_builders_as_state_filtered_query_providers(
            view_builders=[builder],
            state_code=StateCode.US_XX,
            address_overrides=overrides,
        )

        self.assertEqual({"my_view"}, providers_map.keys())

        expected_query = """SELECT * FROM (
SELECT * FROM `recidiviz-456.my_prefix_dataset.table`
) WHERE state_code = 'US_XX';
"""
        self.assertEqual(expected_query, providers_map["my_view"].get_query())
