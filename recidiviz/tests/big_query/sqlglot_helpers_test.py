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
"""Tests our sqlglot_helpers utility functions."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.tests.big_query.sqlglot_helpers import (
    check_query_is_not_ordered_outside_of_windows,
    check_query_selects_output_columns,
    check_view_has_no_state_specific_logic,
)


class TestSqlglotHelpers(unittest.TestCase):
    """Tests all of our helper functions using sqlglot."""

    def test_check_query_is_not_ordered_outside_of_windows(self) -> None:

        qualified = "SELECT * FROM table_a QUALIFY ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) = 1"
        check_query_is_not_ordered_outside_of_windows(qualified)

        invalid = "SELECT * FROM table_a ORDER BY b"
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY b'",
        ):
            check_query_is_not_ordered_outside_of_windows(invalid)

        invalid_multiple_cols = "SELECT * FROM table_a ORDER BY a, b"
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY a, b'",
        ):
            check_query_is_not_ordered_outside_of_windows(invalid_multiple_cols)

        invalid_qualified = """
            SELECT * 
            FROM table_a 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) = 1
            ORDER BY invalid
        """
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY invalid'",
        ):
            check_query_is_not_ordered_outside_of_windows(invalid_qualified)

        multiple_windows = """
            SELECT 
                ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) AS row_num,
                FIRST_VALUE(a) OVER(PARTITION BY a ORDER BY b, c) AS first_val,
                MAX(a) OVER(PARTITION BY a ORDER BY b) AS max_Val
            FROM table_a 
        """
        check_query_is_not_ordered_outside_of_windows(multiple_windows)

        multiple_windows_invalid = """
            SELECT 
                ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) AS row_num,
                FIRST_VALUE(a) OVER(PARTITION BY a ORDER BY b, c) AS first_val,
                MAX(a) OVER(PARTITION BY a ORDER BY b) AS max_Val
            FROM table_a 
            ORDER BY row_num DESC
        """
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY row_num DESC'",
        ):
            check_query_is_not_ordered_outside_of_windows(multiple_windows_invalid)

        string_agg = """
            SELECT 
                STRING_AGG(DISTINCT control_number, ',' ORDER BY control_number) AS control_numbers,
            FROM table
        """
        check_query_is_not_ordered_outside_of_windows(string_agg)

        array_agg_structs = """
        SELECT
          ARRAY_AGG(STRUCT<outcome_type string, outcome_data string>(VIOLATIONOUTCOME,OUTCOMEDETAILS) ORDER BY VIOLATIONOUTCOME, OUTCOMEDETAILS)
        FROM
            table
        """
        check_query_is_not_ordered_outside_of_windows(array_agg_structs)

        json_from_structs = """
        WITH tmp AS (
            SELECT 1 AS a, 'b' AS b UNION ALL
            SELECT 2 AS a, 'c' AS b UNION ALL
            SELECT 3 AS a, 'd' AS b
        )
            SELECT
              TO_JSON(
                ARRAY_AGG(
                  STRUCT<a int64, b string>(a, b)
                  ORDER BY a DESC
                )
              )
            FROM tmp
        """
        check_query_is_not_ordered_outside_of_windows(json_from_structs)

        order_struct_outside_of_aggregation_invalid = """
        WITH tmp AS (
          SELECT 1 AS a, 'b' AS b UNION ALL
          SELECT 2 AS a, 'c' AS b UNION ALL
          SELECT 3 AS a, 'd' AS b
        )
        SELECT
          STRUCT<a int64, b string>(a, b) AS rec
        FROM tmp
          ORDER BY rec.a DESC
        """
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY rec.a DESC'",
        ):
            check_query_is_not_ordered_outside_of_windows(
                order_struct_outside_of_aggregation_invalid
            )

    def test_check_query_selects_output_columns(self) -> None:
        valid_query_simple = "SELECT a, b FROM table_a"
        check_query_selects_output_columns(valid_query_simple, {"a", "b"})

        valid_query_reverse_order = "SELECT b, a FROM table_a"
        check_query_selects_output_columns(valid_query_reverse_order, {"a", "b"})

        valid_query_with_alias = "SELECT x AS b, a FROM table_a"
        check_query_selects_output_columns(valid_query_with_alias, {"a", "b"})

        valid_query_with_cte = """
        WITH cte AS (SELECT * FROM table_a) SELECT a, b FROM cte
        """
        check_query_selects_output_columns(valid_query_with_cte, {"a", "b"})

        valid_query_with_cte_2 = """
        WITH cte AS (SELECT a, b, c FROM table_a) SELECT a, b FROM cte
        """
        check_query_selects_output_columns(valid_query_with_cte_2, {"a", "b"})

        invalid_query_select_star = "SELECT * FROM table_a"
        with self.assertRaisesRegex(
            ValueError, r"May not use a wildcard \(\*\) in the query SELECT statement."
        ):
            check_query_selects_output_columns(invalid_query_select_star, {"a", "b"})

        invalid_query_select_star_except = "SELECT * EXCEPT(a, b) FROM table_a"
        with self.assertRaisesRegex(
            ValueError, r"May not use a wildcard \(\*\) in the query SELECT statement."
        ):
            check_query_selects_output_columns(
                invalid_query_select_star_except, {"a", "b"}
            )

        invalid_query_select_star_except_2 = "SELECT a, b, * EXCEPT(a, b) FROM table_a"
        with self.assertRaisesRegex(
            ValueError, r"May not use a wildcard \(\*\) in the query SELECT statement."
        ):
            check_query_selects_output_columns(
                invalid_query_select_star_except_2, {"a", "b"}
            )

        invalid_query_select_star_and_cols = "SELECT a, b, * FROM table_a"
        with self.assertRaisesRegex(
            ValueError, r"May not use a wildcard \(\*\) in the query SELECT statement."
        ):
            check_query_selects_output_columns(
                invalid_query_select_star_and_cols, {"a", "b"}
            )

        invalid_query_missing_col = "SELECT a, b FROM table_a"
        with self.assertRaisesRegex(
            ValueError, r"Missing expected top-level selected columns: \['c'\]"
        ):
            check_query_selects_output_columns(
                invalid_query_missing_col, {"a", "b", "c"}
            )

        invalid_query_extra_cols = "SELECT a, b, c, d FROM table_a"
        with self.assertRaisesRegex(
            ValueError, r"Found unexpected top-level selected columns: \['c', 'd'\]"
        ):
            check_query_selects_output_columns(invalid_query_extra_cols, {"a", "b"})

    def _builder_for_query_template(
        self, query_template: str
    ) -> SimpleBigQueryViewBuilder:
        return SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="my_view",
            description="Description for my_view",
            view_query_template=query_template,
        )

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    def test_check_view_has_no_state_specific_logic(self) -> None:
        valid_view_no_state_logic = self._builder_for_query_template(
            "SELECT a, b FROM `{project_id}.dataset.table`"
        )
        check_view_has_no_state_specific_logic(valid_view_no_state_logic)

        invalid_view_state_specific_table = self._builder_for_query_template(
            "SELECT a, b FROM `{project_id}.us_xx_dataset.table`"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot query from state-specific tables in this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_state_specific_table)

        invalid_view_state_specific_table_2 = self._builder_for_query_template(
            "SELECT a, b FROM `{project_id}.dataset.us_xx_table`"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot query from state-specific tables in this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_state_specific_table_2)

        invalid_view_invalid_table_in_cte = self._builder_for_query_template(
            "WITH cte AS (SELECT * FROM `{project_id}.dataset.us_xx_table`) "
            "SELECT a, b FROM cte"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot query from state-specific tables in this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_invalid_table_in_cte)

        invalid_view_invalid_table_in_subquery = self._builder_for_query_template(
            "SELECT a, b FROM (SELECT * FROM `{project_id}.dataset.us_xx_table`) a"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot query from state-specific tables in this view."
        ):
            check_view_has_no_state_specific_logic(
                invalid_view_invalid_table_in_subquery
            )

        invalid_view_filters_by_state_code = self._builder_for_query_template(
            "SELECT a, b FROM `{project_id}.dataset.table` WHERE state = 'US_XX'"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot include state-specific logic this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_filters_by_state_code)

        invalid_view_filters_by_state_code_2 = self._builder_for_query_template(
            "SELECT a, b FROM `{project_id}.dataset.table` "
            "WHERE state IN ('US_XX', 'US_YY')"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot include state-specific logic this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_filters_by_state_code_2)

        invalid_view_state_code_in_column = self._builder_for_query_template(
            "SELECT 'US_XX' AS a, b FROM `{project_id}.dataset.table`"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot include state-specific logic this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_state_code_in_column)

        invalid_view_state_code_case = self._builder_for_query_template(
            "SELECT CASE WHEN state = 'US_XX' THEN x END AS a, b "
            "FROM `{project_id}.dataset.table`"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot include state-specific logic this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_state_code_case)

        invalid_view_state_code_cte = self._builder_for_query_template(
            "WITH cte AS ("
            "  SELECT * FROM `{project_id}.dataset.table` WHERE state = 'US_XX'"
            ") SELECT a, b FROM cte"
        )
        with self.assertRaisesRegex(
            ValueError, r"Cannot include state-specific logic this view."
        ):
            check_view_has_no_state_specific_logic(invalid_view_state_code_cte)
