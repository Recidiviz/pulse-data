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
"""Tests for the UnionAllBigQueryViewBuilder."""
import unittest
from unittest.mock import Mock, patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestUnionAllBigQueryViewBuilder(unittest.TestCase):
    """Tests for the UnionAllBigQueryViewBuilder."""

    def setUp(self) -> None:
        self.view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="parent_dataset_1",
                view_id="parent_table_1",
                description="parent_table_1 description",
                view_query_template="SELECT * FROM `{project_id}.my_dataset.table_foo`",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="parent_dataset_2",
                view_id="parent_table_2",
                description="parent_table_2 description",
                view_query_template="SELECT * FROM `{project_id}.my_dataset.table_bar`",
                should_materialize=True,
            ),
        ]

    def test_one_view(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parent_view_builders=self.view_builders[0:1],
        )

        view = builder.build()

        self.assertEqual(
            "SELECT * FROM `recidiviz-456.parent_dataset_1.parent_table_1_materialized`",
            view.view_query,
        )

    def test_multiple_views(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parent_view_builders=self.view_builders[0:2],
        )

        view = builder.build()

        expected_view_query = """SELECT * FROM `recidiviz-456.parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

    def test_build_with_overrides(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parent_view_builders=self.view_builders[0:2],
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset_1")
            .build()
        )
        view = builder.build(address_overrides=address_overrides)

        expected_view_query = """SELECT * FROM `recidiviz-456.my_prefix_parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_union_dataset", table_id="my_union_all_view"
            ),
            view.address,
        )

    def test_build_with_overrides_union_all_dataset(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parent_view_builders=self.view_builders[0:2],
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset_1")
            # Override the dataset this builder is in
            .register_sandbox_override_for_entire_dataset("my_union_dataset")
            .build()
        )
        view = builder.build(address_overrides=address_overrides)

        expected_view_query = """SELECT * FROM `recidiviz-456.my_prefix_parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )
        # View address for the union all view is overridden
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_my_union_dataset", table_id="my_union_all_view"
            ),
            view.address,
        )

    def test_build_with_parent_filter(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parent_view_builders=self.view_builders[0:2],
        )

        builder.set_parent_address_filter(
            parent_address_filter={self.view_builders[0].address}
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset_1")
            .build()
        )
        view = builder.build(address_overrides=address_overrides)

        expected_view_query = """SELECT * FROM `recidiviz-456.my_prefix_parent_dataset_1.parent_table_1_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

    def test_build_with_parent_filter_no_overlap(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parent_view_builders=self.view_builders[0:2],
        )

        builder.set_parent_address_filter(
            parent_address_filter={
                BigQueryAddress(dataset_id="some_other_dataset", table_id="some_view"),
            }
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset_1")
            .build()
        )
        view = builder.build(address_overrides=address_overrides)

        # If the filter does not overlap with any of the queried views, we default to
        # querying all views.
        expected_view_query = """SELECT * FROM `recidiviz-456.my_prefix_parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

    def test_build_with_parent_filter_no_overrides(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parent_view_builders=self.view_builders[0:2],
        )

        builder.set_parent_address_filter(
            parent_address_filter={self.view_builders[0].address}
        )

        with self.assertRaisesRegex(
            ValueError,
            "Cannot set a UNION ALL query filter unless loading views into a sandbox.",
        ):
            _ = builder.build()
