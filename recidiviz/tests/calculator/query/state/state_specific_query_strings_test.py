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
"""Tests for get_pathways_dynamic_filter_options."""

import unittest

from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_dynamic_filter_options,
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
        # Count USING (state_code) — should match the number of LEFT JOINs (8)
        using_count = result.count("USING (state_code)") + result.count(
            "USING(state_code)"
        )
        self.assertEqual(using_count, 8)
