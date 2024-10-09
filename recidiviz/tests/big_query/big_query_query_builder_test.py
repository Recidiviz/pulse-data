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
"""Tests for the BigQueryQueryBuilder."""
import unittest

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_address_formatter import (
    BigQueryAddressFormatter,
    BigQueryAddressFormatterProvider,
)
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder

_DATASET_1 = "dataset_1"
_DATASET_2 = "dataset_2"

_TABLE_1 = "table_1"
_TABLE_2 = "table_2"


class _LimitOneFormatter(BigQueryAddressFormatter):
    def format_address(self, address: ProjectSpecificBigQueryAddress) -> str:
        return f"(SELECT * FROM {address.format_address_for_query()} LIMIT 1)"


@attr.define
class _LimitOneFormatterProvider(BigQueryAddressFormatterProvider):
    def get_formatter(self, address: BigQueryAddress) -> BigQueryAddressFormatter:
        return _LimitOneFormatter()


class BigQueryQueryBuilderTest(unittest.TestCase):
    """Tests for the BigQueryQueryBuilder."""

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.builder_no_overrides = BigQueryQueryBuilder(
            parent_address_overrides=None, parent_address_formatter_provider=None
        )

        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_address(
                BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
            )
            .register_sandbox_override_for_address(
                BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_2)
            )
            .build()
        )
        self.builder_with_overrides = BigQueryQueryBuilder(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=None,
        )
        self.builder_with_overrides_and_formatter = BigQueryQueryBuilder(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=_LimitOneFormatterProvider(),
        )

    def test_build_no_table_no_args(self) -> None:
        template = "SELECT * FROM UNNEST([1, 2]);"

        result = self.builder_no_overrides.build_query(
            project_id=self.project_id, query_template=template, query_format_kwargs={}
        )
        self.assertEqual(result, template)

        result = self.builder_with_overrides.build_query(
            project_id=self.project_id, query_template=template, query_format_kwargs={}
        )
        self.assertEqual(result, template)

        result = self.builder_with_overrides_and_formatter.build_query(
            project_id=self.project_id, query_template=template, query_format_kwargs={}
        )
        self.assertEqual(result, template)

    def test_build_no_table(self) -> None:
        template = "SELECT {column_arg} FROM UNNEST([1, 2]);"

        query_args = {"column_arg": "my_column"}

        expected_result = "SELECT my_column FROM UNNEST([1, 2]);"

        result = self.builder_no_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(result, expected_result)

        result = self.builder_with_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(result, expected_result)

        result = self.builder_with_overrides_and_formatter.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(result, expected_result)

    def test_build_simple(self) -> None:
        template = "SELECT {column_arg} FROM `{project_id}.dataset_1.table_1`;"

        query_args = {"column_arg": "my_column"}

        result = self.builder_no_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result, "SELECT my_column FROM `recidiviz-456.dataset_1.table_1`;"
        )

        result = self.builder_with_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result, "SELECT my_column FROM `recidiviz-456.my_prefix_dataset_1.table_1`;"
        )

        result = self.builder_with_overrides_and_formatter.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result,
            "SELECT my_column FROM (SELECT * FROM `recidiviz-456.my_prefix_dataset_1.table_1` LIMIT 1);",
        )

    def test_build_simple_inject_dataset_and_table(self) -> None:
        template = "SELECT {column_arg} FROM `{project_id}.{dataset_arg}.{table_arg}`;"

        query_args = {
            "column_arg": "my_column",
            "dataset_arg": _DATASET_1,
            "table_arg": _TABLE_1,
        }

        result = self.builder_no_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result, "SELECT my_column FROM `recidiviz-456.dataset_1.table_1`;"
        )

        result = self.builder_with_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result, "SELECT my_column FROM `recidiviz-456.my_prefix_dataset_1.table_1`;"
        )

        result = self.builder_with_overrides_and_formatter.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result,
            "SELECT my_column FROM (SELECT * FROM `recidiviz-456.my_prefix_dataset_1.table_1` LIMIT 1);",
        )

    def test_build_no_overrides_apply_to_view(self) -> None:
        template = "SELECT {column_arg} FROM `{project_id}.{dataset_arg}.{table_arg}`;"

        query_args = {
            "column_arg": "my_column",
            "dataset_arg": "other_dataset",
            "table_arg": "other_table",
        }

        result = self.builder_with_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result, "SELECT my_column FROM `recidiviz-456.other_dataset.other_table`;"
        )

        result = self.builder_with_overrides_and_formatter.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result,
            "SELECT my_column FROM (SELECT * FROM `recidiviz-456.other_dataset.other_table` LIMIT 1);",
        )

    def test_argument_has_project_id_format_arg(self) -> None:
        template = "SELECT {column_arg} FROM `{table_clause}`;"
        query_args = {
            "column_arg": "my_column",
            "table_clause": "{project_id}.dataset_1.table_1",
        }

        result = self.builder_no_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result, "SELECT my_column FROM `recidiviz-456.dataset_1.table_1`;"
        )

        result = self.builder_with_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result, "SELECT my_column FROM `recidiviz-456.my_prefix_dataset_1.table_1`;"
        )

        result = self.builder_with_overrides_and_formatter.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result,
            "SELECT my_column FROM (SELECT * FROM `recidiviz-456.my_prefix_dataset_1.table_1` LIMIT 1);",
        )

    def test_argument_has_format_args_test(self) -> None:
        template = "SELECT {column_arg} FROM `{table_clause}`;"
        query_args = {
            "column_arg": "my_column",
            "table_clause": "{project_id}.{dataset}.{table}",
        }

        expected_error_message = (
            r"Query format arg \[table_clause\] is a template with arguments "
            r"other than project_id: \"{project_id}.{dataset}.{table}\""
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            _ = self.builder_no_overrides.build_query(
                project_id=self.project_id,
                query_template=template,
                query_format_kwargs=query_args,
            )

    def test_build_view_in_another_project_referenced(self) -> None:
        template = (
            "SELECT {column_arg} "
            "FROM `{project_id}.dataset_1.table_1` "
            "JOIN `{another_project_id}.dataset_1.table_1` "
            "ON {column_arg};"
        )

        query_args = {"column_arg": "my_column", "another_project_id": "recidiviz-789"}

        result = self.builder_no_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result,
            (
                "SELECT my_column "
                "FROM `recidiviz-456.dataset_1.table_1` "
                "JOIN `recidiviz-789.dataset_1.table_1` "
                "ON my_column;"
            ),
        )

        result = self.builder_with_overrides.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result,
            (
                "SELECT my_column "
                "FROM `recidiviz-456.my_prefix_dataset_1.table_1` "
                "JOIN `recidiviz-789.dataset_1.table_1` "  # NOTE: No override here
                "ON my_column;"
            ),
        )

        result = self.builder_with_overrides_and_formatter.build_query(
            project_id=self.project_id,
            query_template=template,
            query_format_kwargs=query_args,
        )
        self.assertEqual(
            result,
            (
                "SELECT my_column "
                "FROM (SELECT * FROM `recidiviz-456.my_prefix_dataset_1.table_1` LIMIT 1) "
                "JOIN (SELECT * FROM `recidiviz-789.dataset_1.table_1` LIMIT 1) "  # NOTE: No override here
                "ON my_column;"
            ),
        )
