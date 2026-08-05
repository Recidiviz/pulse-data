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
"""Tests for individual_level_export.py to ensure no PII columns are included and
that raw internal codes are translated to display labels matching the dashboard."""

import json
import unittest

from recidiviz.public_pathways.individual_level_export import (
    INCLUDED_INDIVIDUAL_LEVEL_COLUMNS,
    _translate_row,
    _translate_value,
    build_label_maps,
)


class TestIncludedIndividualLevelColumnsNoPII(unittest.TestCase):
    """Tests that INCLUDED_INDIVIDUAL_LEVEL_COLUMNS does not contain PII identifiers."""

    def test_included_individual_level_columns_no_pii_columns(self) -> None:
        """Test that INCLUDED_INDIVIDUAL_LEVEL_COLUMNS does not contain PII columns.

        Ensures that no columns containing 'name', 'email', 'address', 'ind', or
        'id' are present in the included columns list. Unlike the analogous
        allowed-columns lists for the aggregate Public Pathways views, no id
        column (including person_id) is ever allowed here: this export must
        never give an external consumer a way to key off of any identifier.
        """
        pii_keywords = ["name", "email", "address", "ind"]

        # Adding "kid" to ensure that our id logic does not trip a false positive
        column_names = list(INCLUDED_INDIVIDUAL_LEVEL_COLUMNS) + ["kid"]
        for column in column_names:
            column_lower = column.lower()

            for keyword in pii_keywords:
                self.assertNotIn(
                    keyword,
                    column_lower,
                    f"Column '{column}' contains PII keyword '{keyword}' and should not be in included columns",
                )

            if "id" in column_lower.split("_"):
                self.fail(
                    f"Column '{column}' contains 'id' and should not be in included columns"
                )


class TestTranslateValue(unittest.TestCase):
    """Tests for _translate_value."""

    def setUp(self) -> None:
        self.label_map = {
            "WHITE": "White",
            "UNKNOWN": "Not Coded",
        }

    def test_translates_known_value(self) -> None:
        self.assertEqual("White", _translate_value("WHITE", self.label_map))

    def test_translates_none_to_unknown_entry(self) -> None:
        self.assertEqual("Not Coded", _translate_value(None, self.label_map))

    def test_translates_unknown_sentinel_to_unknown_entry(self) -> None:
        self.assertEqual(
            "Not Coded", _translate_value("EXTERNAL_UNKNOWN", self.label_map)
        )
        self.assertEqual(
            "Not Coded", _translate_value("INTERNAL_UNKNOWN", self.label_map)
        )
        self.assertEqual(
            "Not Coded", _translate_value("PRESENT_WITHOUT_INFO", self.label_map)
        )
        self.assertEqual("Not Coded", _translate_value("", self.label_map))

    def test_falls_back_to_raw_value_when_missing_from_map(self) -> None:
        self.assertEqual("NEW_CODE", _translate_value("NEW_CODE", self.label_map))

    def test_falls_back_to_none_when_none_and_no_unknown_entry(self) -> None:
        self.assertIsNone(_translate_value(None, {"WHITE": "White"}))


class TestBuildLabelMaps(unittest.TestCase):
    """Tests for build_label_maps."""

    def test_parses_dynamic_filter_options_and_includes_time_period(self) -> None:
        dynamic_filter_options_json = json.dumps(
            {
                "facility_id_name_map": json.dumps(
                    [{"value": "FACILITY_A", "label": "Facility A"}]
                ),
                "race_id_name_map": json.dumps(
                    [
                        {"value": "WHITE", "label": "White"},
                        {
                            "value": "AMERICAN_INDIAN_ALASKAN_NATIVE",
                            "label": "Native American",
                        },
                    ]
                ),
                "ethnicity_id_name_map": None,
                "months_at_facility_id_name_map": None,
                "sentence_length_min_id_name_map": None,
                "sentence_length_max_id_name_map": None,
                "charge_county_id_name_map": None,
                "offense_type_id_name_map": None,
                "charge_description_id_name_map": None,
                "admission_reason_id_name_map": None,
            }
        )

        label_maps = build_label_maps(dynamic_filter_options_json)

        self.assertEqual(
            {
                "months_0_6": "6 months",
                "months_7_12": "1 year",
                "months_13_24": "2 years",
                "months_25_60": "5 years",
            },
            label_maps["time_period"],
        )
        self.assertEqual({"FACILITY_A": "Facility A"}, label_maps["facility"])
        self.assertNotIn("ethnicity", label_maps)

    def test_accepts_already_parsed_dict(self) -> None:
        """Local dev fixtures (tools/shared_pathways/load_fixtures.py) insert
        dynamic_filter_options via a raw SQL INSERT, which Postgres parses directly
        into a jsonb object; SQLAlchemy then returns it as a dict rather than a JSON
        string. build_label_maps must handle this shape too."""
        dynamic_filter_options = {
            "facility_id_name_map": json.dumps(
                [{"value": "FACILITY_A", "label": "Facility A"}]
            ),
            "race_id_name_map": None,
            "ethnicity_id_name_map": None,
            "months_at_facility_id_name_map": None,
            "sentence_length_min_id_name_map": None,
            "sentence_length_max_id_name_map": None,
            "charge_county_id_name_map": None,
            "offense_type_id_name_map": None,
            "charge_description_id_name_map": None,
            "admission_reason_id_name_map": None,
        }

        label_maps = build_label_maps(dynamic_filter_options)

        self.assertEqual({"FACILITY_A": "Facility A"}, label_maps["facility"])

    def test_tolerates_missing_keys(self) -> None:
        """Some dynamic_filter_options data (e.g. older or hand-written fixtures, see
        tests/public_pathways/fixtures/metric_metadata.csv) omits keys entirely rather
        than setting them to null. build_label_maps must not KeyError on those."""
        dynamic_filter_options: dict[str, str | None] = {
            "gender_id_name_map": json.dumps([{"value": "MALE", "label": "Male"}]),
            "date_in_population_id_name_map": json.dumps(
                [{"value": "2022-01-01", "label": "January 1, 2022"}]
            ),
        }

        label_maps = build_label_maps(dynamic_filter_options)

        self.assertNotIn("facility", label_maps)
        self.assertNotIn("race", label_maps)
        self.assertEqual("6 months", label_maps["time_period"]["months_0_6"])


class TestTranslateRow(unittest.TestCase):
    """Tests for _translate_row."""

    def test_translates_only_columns_present_in_label_maps(self) -> None:
        column_names = ["state_code", "facility", "age_group"]
        label_maps = {"facility": {"FACILITY_A": "Facility A"}}
        row = ["US_NY", "FACILITY_A", "25-29"]

        self.assertEqual(
            ["US_NY", "Facility A", "25-29"],
            _translate_row(row, column_names=column_names, label_maps=label_maps),
        )
