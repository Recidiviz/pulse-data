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

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.metadata import local_project_id_override


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
            SimpleBigQueryViewBuilder(
                dataset_id="parent_dataset_3",
                view_id="parent_table_3",
                description="parent_table_3 description",
                view_query_template="SELECT * FROM `{project_id}.my_dataset.table_baz`",
                should_materialize=True,
                projects_to_deploy={"recidiviz-789"},
            ),
        ]

    def test_one_view(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=self.view_builders[0:1],
            clustering_fields=["state_code"],
        )

        with local_project_id_override("recidiviz-456"):
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
            parents=self.view_builders[0:2],
            clustering_fields=["state_code"],
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build()

        self.assertEqual(["state_code"], view.clustering_fields)

        expected_view_query = """SELECT * FROM `recidiviz-456.parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

    def test_source_table_parents(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=[
                BigQueryAddress.from_str("source_table_dataset1.table_1"),
                BigQueryAddress.from_str("source_table_dataset2.table_2"),
            ],
            clustering_fields=["state_code"],
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build()

        expected_view_query = """SELECT * FROM `recidiviz-456.source_table_dataset1.table_1`
UNION ALL
SELECT * FROM `recidiviz-456.source_table_dataset2.table_2`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

    def test_custom_select_statement(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=[
                BigQueryAddress.from_str("source_table_dataset1.table_1"),
                BigQueryAddress.from_str("source_table_dataset2.table_2"),
            ],
            custom_select_statement="SELECT a, b",
            clustering_fields=["state_code"],
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build()

        expected_view_query = """SELECT a, b FROM `recidiviz-456.source_table_dataset1.table_1`
UNION ALL
SELECT a, b FROM `recidiviz-456.source_table_dataset2.table_2`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

    def test_materialized_address_override(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=[
                BigQueryAddress.from_str("source_table_dataset1.table_1"),
                BigQueryAddress.from_str("source_table_dataset2.table_2"),
            ],
            materialized_address_override=BigQueryAddress.from_str(
                "another_dataset.another_table"
            ),
            clustering_fields=["state_code"],
        )
        self.assertEqual(
            builder.table_for_query,
            BigQueryAddress.from_str("another_dataset.another_table"),
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build()

        self.assertEqual(
            view.table_for_query,
            BigQueryAddress.from_str("another_dataset.another_table"),
        )

        builder_no_override = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=[
                BigQueryAddress.from_str("source_table_dataset1.table_1"),
                BigQueryAddress.from_str("source_table_dataset2.table_2"),
            ],
            clustering_fields=["state_code"],
        )
        self.assertEqual(
            builder_no_override.table_for_query,
            BigQueryAddress.from_str("my_union_dataset.my_union_all_view_materialized"),
        )

    def test_multiple_views_one_should_not_deploy(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=self.view_builders[0:3],
            clustering_fields=["state_code"],
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build()

        # The third view shouldn't be deployed in recidiviz-456 test project, so it
        # doesn't get pulled into the UNION.
        expected_view_query = """SELECT * FROM `recidiviz-456.parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

        with local_project_id_override("recidiviz-789"):
            view = builder.build()

        # ... however it is deployed in recidiviz-789 so it gets included when building
        #  for that project.
        expected_view_query = """SELECT * FROM `recidiviz-789.parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-789.parent_dataset_2.parent_table_2_materialized`
UNION ALL
SELECT * FROM `recidiviz-789.parent_dataset_3.parent_table_3_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )

    def test_build_with_overrides(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=self.view_builders[0:2],
            clustering_fields=["state_code"],
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset_1")
            .build()
        )
        sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix="my_prefix",
            state_code_filter=None,
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build(sandbox_context=sandbox_context)

        expected_view_query = """SELECT * FROM `recidiviz-456.my_prefix_parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_my_union_dataset", table_id="my_union_all_view"
            ),
            view.address,
        )

    def test_build_with_overrides_and_state_code_filter_state_agnostic_parents(
        self,
    ) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=self.view_builders[0:2],
            clustering_fields=["state_code"],
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset_1")
            .build()
        )
        sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix="my_prefix",
            state_code_filter=StateCode.US_XX,
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build(sandbox_context=sandbox_context)

        # We keep both parents because they are state-angostic tables
        expected_view_query = """SELECT * FROM `recidiviz-456.my_prefix_parent_dataset_1.parent_table_1_materialized`
UNION ALL
SELECT * FROM `recidiviz-456.parent_dataset_2.parent_table_2_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_my_union_dataset", table_id="my_union_all_view"
            ),
            view.address,
        )

    def test_build_with_overrides_and_state_code_filter_state_specific_parents(
        self,
    ) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=[
                SimpleBigQueryViewBuilder(
                    dataset_id="us_xx_parent_dataset",
                    view_id="parent_table_1",
                    description="parent_table_1 description",
                    view_query_template="SELECT * FROM `{project_id}.my_dataset.table_foo`",
                    should_materialize=True,
                ),
                SimpleBigQueryViewBuilder(
                    dataset_id="us_yy_parent_dataset",
                    view_id="parent_table_2",
                    description="parent_table_2 description",
                    view_query_template="SELECT * FROM `{project_id}.my_dataset.table_bar`",
                    should_materialize=True,
                ),
            ],
            clustering_fields=["state_code"],
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("us_xx_parent_dataset")
            .register_sandbox_override_for_entire_dataset("us_yy_parent_dataset")
            .build()
        )
        sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix="my_prefix",
            state_code_filter=StateCode.US_XX,
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build(sandbox_context=sandbox_context)

        # We keep only the US_XX parent table
        expected_view_query = """SELECT * FROM `recidiviz-456.my_prefix_us_xx_parent_dataset.parent_table_1_materialized`"""
        self.assertEqual(
            expected_view_query,
            view.view_query,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_my_union_dataset", table_id="my_union_all_view"
            ),
            view.address,
        )

    def test_build_with_overrides_union_all_dataset(self) -> None:
        builder = UnionAllBigQueryViewBuilder(
            dataset_id="my_union_dataset",
            view_id="my_union_all_view",
            description="All data together",
            parents=self.view_builders[0:2],
            clustering_fields=["state_code"],
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset_1")
            # Override the dataset this builder is in
            .register_sandbox_override_for_entire_dataset("my_union_dataset")
            .build()
        )

        sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix="my_prefix",
            state_code_filter=None,
        )

        with local_project_id_override("recidiviz-456"):
            view = builder.build(sandbox_context=sandbox_context)

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
