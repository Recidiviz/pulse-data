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

from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.utils.reference_query_providers import (
    RootEntityIdFilteredQueryProvider,
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
