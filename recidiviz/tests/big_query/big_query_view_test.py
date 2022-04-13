# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for BigQueryView"""
import copy
import unittest

from mock import patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class BigQueryViewTest(unittest.TestCase):
    """Tests for BigQueryView"""

    PROJECT_ID = "recidiviz-project-id"

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.PROJECT_ID

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_simple_view_no_extra_args(self) -> None:
        fake_clustering_fields = ["clustering_field_1", "clustering_field_2"]
        view = BigQueryView(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
            clustering_fields=fake_clustering_fields,
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual("view_dataset", view.dataset_id)
        self.assertEqual("my_view", view.table_id)
        self.assertEqual("my_view", view.view_id)
        self.assertEqual(
            f"SELECT * FROM `{self.PROJECT_ID}.some_dataset.table`", view.view_query
        )
        self.assertEqual(
            f"SELECT * FROM `{self.PROJECT_ID}.view_dataset.my_view`", view.select_query
        )
        self.assertEqual(fake_clustering_fields, view.clustering_fields)

    def test_simple_view_overwrite_project_id(self) -> None:
        view = BigQueryView(
            project_id="other-project",
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
        )

        self.assertEqual("other-project", view.project)
        self.assertEqual("view_dataset", view.dataset_id)
        self.assertEqual("my_view", view.table_id)
        self.assertEqual("my_view", view.view_id)
        self.assertEqual(
            "SELECT * FROM `other-project.some_dataset.table`", view.view_query
        )
        self.assertEqual(
            "SELECT * FROM `other-project.view_dataset.my_view`", view.select_query
        )

    def test_extra_format_args(self) -> None:
        view = BigQueryView(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT {select_col_1}, {select_col_2} FROM `{project_id}.{some_dataset}.table`",
            some_dataset="a_dataset",
            select_col_1="name",
            select_col_2="date",
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual("view_dataset", view.dataset_id)
        self.assertEqual("my_view", view.table_id)
        self.assertEqual("my_view", view.view_id)
        self.assertEqual(
            f"SELECT name, date FROM `{self.PROJECT_ID}.a_dataset.table`",
            view.view_query,
        )
        self.assertEqual(
            f"SELECT * FROM `{self.PROJECT_ID}.view_dataset.my_view`", view.select_query
        )

    def test_missing_format_arg_throws_on_instantiation(self) -> None:
        with self.assertRaises(KeyError):
            _ = BigQueryView(
                dataset_id="view_dataset",
                view_id="my_view",
                description="my_view description",
                view_query_template="SELECT {select_col_1}, {select_col_2} FROM `{project_id}.{some_dataset}.table`",
                some_dataset="a_dataset",
                select_col_2="date",
            )

    def test_materialized_address_override_same_as_view_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^Materialized address "
            r"\[BigQueryAddress\(dataset_id='view_dataset', table_id='my_view'\)\] cannot be "
            "same as view itself.$",
        ):
            _ = SimpleBigQueryViewBuilder(
                dataset_id="view_dataset",
                view_id="my_view",
                description="my_view description",
                should_materialize=True,
                materialized_address_override=BigQueryAddress(
                    dataset_id="view_dataset", table_id="my_view"
                ),
                view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
                some_dataset="a_dataset",
            ).build()

    def test_materialized_address_override_no_should_materialize_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found nonnull materialized_address_override \["
            r"BigQueryAddress\(dataset_id='view_dataset_materialized', table_id='my_view_table'\)\] "
            "when `should_materialize` is not True",
        ):
            _ = SimpleBigQueryViewBuilder(
                dataset_id="view_dataset",
                view_id="my_view",
                description="my_view description",
                materialized_address_override=BigQueryAddress(
                    dataset_id="view_dataset_materialized",
                    table_id="my_view_table",
                ),
                view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
                some_dataset="a_dataset",
            ).build()

    def test_materialized_address(self) -> None:
        view_materialized_no_override = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            should_materialize=True,
            view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
            some_dataset="a_dataset",
        ).build()

        self.assertEqual(
            BigQueryAddress(dataset_id="view_dataset", table_id="my_view_materialized"),
            view_materialized_no_override.materialized_address,
        )
        self.assertEqual(
            BigQueryAddress(dataset_id="view_dataset", table_id="my_view_materialized"),
            view_materialized_no_override.table_for_query,
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-project-id.view_dataset.my_view_materialized`",
            view_materialized_no_override.select_query,
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-project-id.view_dataset.my_view`",
            view_materialized_no_override.direct_select_query,
        )

        view_with_override = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset",
                table_id="my_view_table",
            ),
            view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
            some_dataset="a_dataset",
        ).build()

        self.assertEqual(
            BigQueryAddress(dataset_id="other_dataset", table_id="my_view_table"),
            view_with_override.materialized_address,
        )
        self.assertEqual(
            BigQueryAddress(dataset_id="other_dataset", table_id="my_view_table"),
            view_with_override.table_for_query,
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-project-id.other_dataset.my_view_table`",
            view_with_override.select_query,
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-project-id.view_dataset.my_view`",
            view_with_override.direct_select_query,
        )

        view_not_materialized = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
            some_dataset="a_dataset",
        ).build()

        self.assertIsNone(view_not_materialized.materialized_address)
        self.assertEqual(
            BigQueryAddress(dataset_id="view_dataset", table_id="my_view"),
            view_not_materialized.table_for_query,
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-project-id.view_dataset.my_view`",
            view_not_materialized.select_query,
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-project-id.view_dataset.my_view`",
            view_not_materialized.direct_select_query,
        )

    def test_materialized_address_overrides(self) -> None:
        address_overrides_builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_override"
        )
        for dataset in ["view_dataset", "other_dataset"]:
            address_overrides_builder.register_sandbox_override_for_entire_dataset(
                dataset
            )
        address_overrides = address_overrides_builder.build()

        view_materialized_no_override = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            should_materialize=True,
            view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
            some_dataset="a_dataset",
        ).build(address_overrides=address_overrides)

        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_override_view_dataset", table_id="my_view_materialized"
            ),
            view_materialized_no_override.materialized_address,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_override_view_dataset", table_id="my_view_materialized"
            ),
            view_materialized_no_override.table_for_query,
        )

        view_with_override = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset",
                table_id="my_view_table",
            ),
            view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
            some_dataset="a_dataset",
        ).build(address_overrides=address_overrides)

        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_override_other_dataset", table_id="my_view_table"
            ),
            view_with_override.materialized_address,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_override_other_dataset", table_id="my_view_table"
            ),
            view_with_override.table_for_query,
        )

        view_not_materialized = BigQueryView(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            address_overrides=address_overrides,
            view_query_template="SELECT * FROM `{project_id}.{some_dataset}.table`",
            some_dataset="a_dataset",
        )

        self.assertIsNone(view_not_materialized.materialized_address)
        self.assertEqual(
            BigQueryAddress(dataset_id="my_override_view_dataset", table_id="my_view"),
            view_not_materialized.table_for_query,
        )

    def test_view_equality(self) -> None:
        v = BigQueryView(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
        )

        v_copy = copy.copy(v)

        self.assertTrue(v == v_copy)
        self.assertTrue(v in {v_copy})

        v_deep_copy = copy.deepcopy(v)
        self.assertTrue(v == v_deep_copy)
        self.assertTrue(v in {v_deep_copy})

        view_set = {v}
        view_set.add(v_copy)
        view_set.add(v_deep_copy)

        self.assertEqual(1, len(view_set))

    def test_deploy_in_all(self) -> None:
        v = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
        )

        self.assertTrue(v.should_deploy_in_project("test-project"))
        self.assertTrue(v.should_deploy_in_project(GCP_PROJECT_STAGING))
        self.assertTrue(v.should_deploy_in_project(GCP_PROJECT_PRODUCTION))

    def test_deploy_in_staging(self) -> None:
        v = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
            projects_to_deploy={GCP_PROJECT_STAGING},
        )

        self.assertFalse(v.should_deploy_in_project("test-project"))
        self.assertTrue(v.should_deploy_in_project(GCP_PROJECT_STAGING))
        self.assertFalse(v.should_deploy_in_project(GCP_PROJECT_PRODUCTION))

    def test_deploy_in_multiple(self) -> None:
        v = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
            projects_to_deploy={GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION},
        )

        self.assertFalse(v.should_deploy_in_project("test-project"))
        self.assertTrue(v.should_deploy_in_project(GCP_PROJECT_STAGING))
        self.assertTrue(v.should_deploy_in_project(GCP_PROJECT_PRODUCTION))
