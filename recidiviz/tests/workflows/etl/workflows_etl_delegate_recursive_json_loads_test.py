#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2026 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for recursive_json_loads."""
from unittest import TestCase

from recidiviz.workflows.etl.workflows_etl_delegate import recursive_json_loads


class TestRecursiveJsonLoads(TestCase):
    """Tests for recursive_json_loads."""

    def test_json_object_string_is_parsed(self) -> None:
        self.assertEqual(
            {"personId": "A1042", "officerId": "OFC-7"},
            recursive_json_loads('{"personId": "A1042", "officerId": "OFC-7"}'),
        )

    def test_json_array_string_is_parsed(self) -> None:
        self.assertEqual(
            ["MINIMUM", "MEDIUM"],
            recursive_json_loads('["MINIMUM", "MEDIUM"]'),
        )

    def test_string_holding_object_inside_dict_is_parsed(self) -> None:
        self.assertEqual(
            {
                "personId": "A1042",
                "personName": {
                    "givenNames": "John",
                    "middleNames": "0",
                    "nameSuffix": "",
                    "surname": "Smith",
                },
            },
            recursive_json_loads(
                {
                    "personId": "A1042",
                    "personName": '{"givenNames": "John", "middleNames": "0", "nameSuffix": "", "surname": "Smith"}',
                }
            ),
        )

    def test_string_holding_array_inside_dict_is_parsed(self) -> None:
        self.assertEqual(
            {
                "personId": "A1042",
                "caseNotes": [
                    {"noteTitle": "Employment", "eventDate": "2026-03-02"},
                    {"noteTitle": "Drug screen", "eventDate": "2026-04-15"},
                ],
            },
            recursive_json_loads(
                {
                    "personId": "A1042",
                    "caseNotes": '[{"noteTitle": "Employment", "eventDate": "2026-03-02"}, '
                    '{"noteTitle": "Drug screen", "eventDate": "2026-04-15"}]',
                }
            ),
        )

    def test_json_strings_inside_list_are_parsed(self) -> None:
        self.assertEqual(
            [{"type": "BIRTHDAY_THIS_MONTH"}, {"type": "NO_VIOLATIONS"}],
            recursive_json_loads(
                ['{"type": "BIRTHDAY_THIS_MONTH"}', '{"type": "NO_VIOLATIONS"}']
            ),
        )

    def test_non_json_strings_are_unchanged(self) -> None:
        self.assertEqual(
            {
                "caseNote": "Client reported to office on time",
                "redactedNote": "[REDACTED] see case notes",
                "officerComment": "{incomplete",
            },
            recursive_json_loads(
                {
                    "caseNote": "Client reported to office on time",
                    "redactedNote": "[REDACTED] see case notes",
                    "officerComment": "{incomplete",
                }
            ),
        )

    def test_strings_holding_empty_containers_are_parsed(self) -> None:
        self.assertEqual(
            {"caseNotes": [], "eligibleCriteria": {}},
            recursive_json_loads({"caseNotes": "[]", "eligibleCriteria": "{}"}),
        )

    def test_non_string_values_are_strings(self) -> None:
        self.assertEqual(
            {"monthsOnSupervision": "18", "fineBalance": "12.5", "isEligible": "true"},
            recursive_json_loads(
                '{"monthsOnSupervision": "18", "fineBalance": "12.5", '
                '"isEligible": "true"}'
            ),
        )

    def test_does_not_mutate_input(self) -> None:
        record = {"eligibleCriteria": '{"supervisionLevel": "MINIMUM"}'}
        recursive_json_loads(record)
        self.assertEqual(
            {"eligibleCriteria": '{"supervisionLevel": "MINIMUM"}'}, record
        )
