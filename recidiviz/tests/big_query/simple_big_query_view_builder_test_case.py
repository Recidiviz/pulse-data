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
"""This file provides a test case to test unit test SimpleBigQueryViewBuilder views."""
from typing import Any

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

TableRow = dict[str, Any]


class SimpleBigQueryViewBuilderTestCase(BigQueryEmulatorTestCase):
    """
    This test case allows testing input and output on
    SimpleBigQueryViewBuilder using the BigQueryEmulator.
    """

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        raise NotImplementedError(
            "Subclasses should have this property return the SimpleBigQueryViewBuilder you'd like to test!"
        )

    # TODO(#18306) Update view builder to have more knowledge of parents' schema information
    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        """This dictionary has the BQ schema for each direct parent view or table of the view_builder associated
        with this test.
        """
        return {}

    # TODO(#33288) Support using fixture files
    def run_simple_view_builder_query_test_from_fixtures(self) -> None:
        return None

    def run_simple_view_builder_query_test_from_data(
        self,
        input_data: dict[BigQueryAddress, list[TableRow]],
        expected_result: list[TableRow],
        enforce_order: bool = True,
    ) -> None:
        view = self.view_builder.build()
        try:
            self.assertSetEqual(view.parent_tables, set(self.parent_schemas))
        except Exception as e:
            raise ValueError(
                "The schemas defined for in `parent_schemas` do not match the parent tables for this view."
            ) from e

        for address, schema in self.parent_schemas.items():
            self.create_mock_table(address, schema)
            self.load_rows_into_table(address, input_data[address])
        self.run_query_test(
            view.view_query, expected_result, enforce_order=enforce_order
        )


_ExampleVB = SimpleBigQueryViewBuilder(
    dataset_id="example",
    view_id="example_view",
    description="This is a lil fake view for an example.",
    view_query_template="SELECT col FROM `{project_id}.example.ex_input`",
)


class ExampleTest(SimpleBigQueryViewBuilderTestCase):
    """A small example to show how to use the SimpleBigQueryViewBuilderTestCase."""

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return _ExampleVB

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            BigQueryAddress.from_str("example.ex_input"): [
                bigquery.SchemaField("col", "STRING"),
            ]
        }

    def test_returns_a(self) -> None:
        output_data = [{"col": "a"}]
        input_data = {BigQueryAddress.from_str("example.ex_input"): output_data}
        self.run_simple_view_builder_query_test_from_data(input_data, output_data)

    def test_returns_b(self) -> None:
        output_data = [{"col": "b"}]
        input_data = {BigQueryAddress.from_str("example.ex_input"): output_data}
        self.run_simple_view_builder_query_test_from_data(input_data, output_data)
