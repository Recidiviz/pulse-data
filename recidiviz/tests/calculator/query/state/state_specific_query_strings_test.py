# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for state_specific_query_strings helpers."""

import unittest

from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_dynamic_filter_options,
    get_pathways_incarceration_last_updated_date,
    get_pathways_supervision_last_updated_date,
)


class GetPathwaysDynamicFilterOptionsTests(unittest.TestCase):
    """Tests for the get_pathways_dynamic_filter_options helper function."""

    def test_returns_non_empty_string(self) -> None:
        result = get_pathways_dynamic_filter_options()
        self.assertIsInstance(result, str)
        self.assertTrue(len(result.strip()) > 0)

    def test_contains_all_cte_names(self) -> None:
        result = get_pathways_dynamic_filter_options()
        expected_ctes = [
            "facilities",
            "genders",
            "races",
            "ethnicities",
            "sentence_length_mins",
            "sentence_length_maxs",
            "charge_county",
            "offense_type",
            "date_in_population",
        ]
        for cte in expected_ctes:
            self.assertIn(cte, result, f"Missing CTE: {cte}")

    def test_contains_all_id_name_map_columns(self) -> None:
        result = get_pathways_dynamic_filter_options()
        expected_columns = [
            "gender_id_name_map",
            "facility_id_name_map",
            "race_id_name_map",
            "ethnicity_id_name_map",
            "sentence_length_min_id_name_map",
            "sentence_length_max_id_name_map",
            "charge_county_id_name_map",
            "offense_type_id_name_map",
            "date_in_population_id_name_map",
        ]
        for col in expected_columns:
            self.assertIn(col, result, f"Missing column: {col}")

    def test_selects_state_code_and_dynamic_filter_options(self) -> None:
        result = get_pathways_dynamic_filter_options()
        self.assertIn("state_code", result)
        self.assertIn("dynamic_filter_options", result)

    def test_uses_to_json_string_struct(self) -> None:
        result = get_pathways_dynamic_filter_options()
        self.assertIn("TO_JSON_STRING(STRUCT(", result)

    def test_joins_all_ctes_on_state_code(self) -> None:
        result = get_pathways_dynamic_filter_options()
        expected_joins = [
            "LEFT JOIN facilities",
            "LEFT JOIN races",
            "LEFT JOIN ethnicities",
            "LEFT JOIN sentence_length_mins",
            "LEFT JOIN sentence_length_maxs",
            "LEFT JOIN charge_county",
            "LEFT JOIN offense_type",
            "LEFT JOIN date_in_population",
        ]
        for join in expected_joins:
            self.assertIn(join, result, f"Missing join: {join}")
        # genders is the base table (FROM), not a LEFT JOIN
        self.assertIn("FROM genders", result)

    def test_all_joins_use_state_code(self) -> None:
        result = get_pathways_dynamic_filter_options()
        # Count USING (state_code): 8 outer LEFT JOINs between top-level CTEs +
        # 1 inner INNER JOIN against UNDER_CUSTODY_all inside the
        # date_in_population CTE = 9.
        using_count = result.count("USING (state_code)") + result.count(
            "USING(state_code)"
        )
        self.assertEqual(using_count, 11)


class GetPathwaysLastUpdatedDateTests(unittest.TestCase):
    """Tests for the get_pathways_*_last_updated_date helper functions."""

    def test_supervision_single_table_state_no_subquery(self) -> None:
        result = get_pathways_supervision_last_updated_date()
        # US_ND uses a single table — should reference it directly, not nested
        self.assertIn("us_nd_raw_data_views.docstars_offendercasestable_all", result)
        # Should not wrap single-table states in a subquery FROM (...)
        self.assertNotIn(
            "SELECT update_datetime FROM `{project_id}.us_nd_raw_data_views", result
        )

    def test_supervision_us_tn_uses_both_tables(self) -> None:
        result = get_pathways_supervision_last_updated_date()
        self.assertIn("us_tn_raw_data_views.SupervisionPlan_all", result)
        self.assertIn("us_tn_raw_data_views.AssignedStaff_all", result)

    def test_supervision_us_tn_subquery_takes_max_across_tables(self) -> None:
        result = get_pathways_supervision_last_updated_date()
        # Both US_TN tables appear as flat inner SELECTs in the UNION ALL
        self.assertIn(
            "SELECT 'US_TN' AS state_code, update_datetime"
            " FROM `{project_id}.us_tn_raw_data_views.SupervisionPlan_all`",
            result,
        )
        self.assertIn(
            "SELECT 'US_TN' AS state_code, update_datetime"
            " FROM `{project_id}.us_tn_raw_data_views.AssignedStaff_all`",
            result,
        )
        # The outer GROUP BY drives the MAX across all tables for each state
        self.assertIn("date(max(update_datetime))", result)
        self.assertIn("GROUP BY state_code", result)

    def test_supervision_all_states_present(self) -> None:
        result = get_pathways_supervision_last_updated_date()
        for state_code in ("US_IX", "US_ND", "US_TN", "US_ME"):
            self.assertIn(state_code, result, f"Missing state: {state_code}")

    def test_incarceration_single_table_state_no_subquery(self) -> None:
        result = get_pathways_incarceration_last_updated_date()
        self.assertIn("us_tn_raw_data_views.OffenderMovement_all", result)
        self.assertNotIn(
            "SELECT update_datetime FROM `{project_id}.us_tn_raw_data_views.OffenderMovement_all`",
            result,
        )

    def test_incarceration_all_states_present(self) -> None:
        result = get_pathways_incarceration_last_updated_date()
        for state_code in (
            "US_IX",
            "US_ME",
            "US_ND",
            "US_TN",
            "US_MI",
            "US_CO",
            "US_NY",
        ):
            self.assertIn(state_code, result, f"Missing state: {state_code}")
