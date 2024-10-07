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
"""Unit test for stable counts validations"""
import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entity_by_table_name,
)
from recidiviz.validation.views.state.stable_counts.entity_by_column_count_stable_counts import (
    exemptions_string_builder,
)
from recidiviz.validation.views.state.stable_counts.stable_counts import (
    ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME,
)


class TestStableCountsValidations(unittest.TestCase):
    """Unit tests to check that all entity names and columns in ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME
    are valid"""

    def test_valid_expected_entities(self) -> None:
        found_tables = [
            key.name for key in get_all_table_classes_in_schema(SchemaType.STATE)
        ]
        for entity, _ in ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME.items():
            self.assertIn(entity, found_tables)

    def test_valid_date_columns(self) -> None:
        for (
            entity_name,
            config,
        ) in ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME.items():
            entity = get_database_entity_by_table_name(schema, entity_name)
            for date_col in config.date_columns_to_check:
                self.assertTrue(hasattr(entity, date_col.date_column_name))


class TestStableCountsExemptions(unittest.TestCase):
    """Unit tests to test methods related to stable counts exemptions."""

    def test_exemptions_string_builder_single_exemption(self) -> None:
        exemptions = {StateCode.US_MI: [date(2023, 4, 1)]}
        output = exemptions_string_builder(exemptions)
        expected_output = 'NOT (state_code = "US_MI" AND month IN ("2023-04-01"))'
        self.assertEqual(output, expected_output)

    def test_exemptions_string_builder_multiple_exemptions(self) -> None:
        exemptions = {
            StateCode.US_MO: [date(2023, 3, 1), date(2023, 4, 1), date(2023, 5, 1)],
        }
        output = exemptions_string_builder(exemptions)
        expected_output = 'NOT (state_code = "US_MO" AND month IN ("2023-03-01", "2023-04-01", "2023-05-01"))'

        self.assertEqual(output, expected_output)

    def test_exemptions_string_builder_multiple_states(self) -> None:
        exemptions = {
            StateCode.US_PA: [date(2023, 2, 1), date(2023, 3, 1)],
            StateCode.US_MO: [date(2023, 3, 1), date(2023, 4, 1), date(2023, 5, 1)],
        }
        output = exemptions_string_builder(exemptions)
        expected_output = (
            'NOT (state_code = "US_PA" AND month IN ("2023-02-01", "2023-03-01"))'
            '\n      AND NOT (state_code = "US_MO" AND month IN ("2023-03-01", "2023-04-01", "2023-05-01"))'
        )

        self.assertEqual(output, expected_output)
