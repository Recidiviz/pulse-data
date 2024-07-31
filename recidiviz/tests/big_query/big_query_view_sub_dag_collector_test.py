# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for BigQueryViewSubDagCollector"""
import unittest
from unittest.mock import Mock, patch

from recidiviz.big_query.big_query_view_sub_dag_collector import (
    BigQueryViewSubDagCollector,
)
from recidiviz.tests.big_query.big_query_view_dag_walker_test import (
    DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class BigQueryViewSubDagCollectorTest(unittest.TestCase):
    """Tests for BigQueryViewSubDagCollector"""

    def test_single_view_sub_dag(self) -> None:
        collector = BigQueryViewSubDagCollector(
            view_builders_in_full_dag=DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            view_addresses_in_sub_dag={
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[0].address
            },
            include_ancestors=False,
            include_descendants=False,
            datasets_to_exclude=set(),
        )

        self.assertCountEqual(
            [DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[0]],
            collector.collect_view_builders(),
        )

    def test_single_dataset_in_sub_dag(self) -> None:
        collector = BigQueryViewSubDagCollector(
            view_builders_in_full_dag=DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            view_addresses_in_sub_dag={
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[3].address
            },
            include_ancestors=False,
            include_descendants=False,
            datasets_to_exclude=set(),
        )

        self.assertCountEqual(
            [DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[3]],
            collector.collect_view_builders(),
        )

    def test_multiple_addresses_specified(self) -> None:
        collector = BigQueryViewSubDagCollector(
            view_builders_in_full_dag=DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            view_addresses_in_sub_dag={
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[0].address,
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[3].address,
            },
            include_ancestors=False,
            include_descendants=False,
            datasets_to_exclude=set(),
        )

        self.assertCountEqual(
            [
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[0],
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[3],
            ],
            collector.collect_view_builders(),
        )

    def test_include_ancestors(self) -> None:
        collector = BigQueryViewSubDagCollector(
            view_builders_in_full_dag=DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            view_addresses_in_sub_dag={
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[2].address
            },
            include_ancestors=True,
            include_descendants=False,
            datasets_to_exclude=set(),
        )

        self.assertCountEqual(
            [
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[0],
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[1],
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[2],
            ],
            collector.collect_view_builders(),
        )

    def test_include_descendants(self) -> None:
        collector = BigQueryViewSubDagCollector(
            view_builders_in_full_dag=DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            view_addresses_in_sub_dag={
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[2].address
            },
            include_ancestors=False,
            include_descendants=True,
            datasets_to_exclude=set(),
        )

        self.assertCountEqual(
            [
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[2],
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[3],
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[4],
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[5],
            ],
            collector.collect_view_builders(),
        )

    def test_include_ancestors_and_descendants(self) -> None:
        collector = BigQueryViewSubDagCollector(
            view_builders_in_full_dag=DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            view_addresses_in_sub_dag={
                DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[2].address
            },
            include_ancestors=True,
            include_descendants=True,
            datasets_to_exclude=set(),
        )

        self.assertCountEqual(
            DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            collector.collect_view_builders(),
        )
