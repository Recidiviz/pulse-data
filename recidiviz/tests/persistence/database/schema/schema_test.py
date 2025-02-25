# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# pylint: disable=protected-access
"""
Tests for SQLAlchemy enums defined in recidiviz.persistence.database.schema
"""
from types import ModuleType
from typing import List
from unittest import TestCase

import sqlalchemy

from recidiviz.persistence.database.reserved_words import RESERVED_WORDS
from recidiviz.persistence.database.schema_utils import get_all_table_classes


class TestSchemaEnums(TestCase):
    """Base test class for validating schema enums are defined correctly"""

    def check_persistence_and_schema_enums_match(self, test_mapping, enums_package):
        schema_enums_by_name = {}
        num_enums = 0
        for attribute_name in dir(enums_package):
            attribute = getattr(enums_package, attribute_name)
            if isinstance(attribute, sqlalchemy.Enum):
                num_enums += 1
                schema_enums_by_name[attribute_name] = attribute
        self.assertNotEqual(
            0,
            num_enums,
            f"No enums found in package [{enums_package}] - is"
            f" this the correct package to search for schema"
            f" enums?",
        )

        for schema_enum_name, schema_enum in schema_enums_by_name.items():
            # This will throw a KeyError if a schema enum is not mapped to
            # a persistence layer enum to be tested against
            persistence_enum = test_mapping[schema_enum_name]
            if persistence_enum is not None:
                self._assert_enum_values_match(schema_enum, persistence_enum)

    # This test method currently does not account for situations where either
    # enum should have values that are excluded from comparison. If a situation
    # like that arises, this test case can be extended to have hard-coded
    # exclusions.
    def _assert_enum_values_match(self, schema_enum, persistence_enum):
        schema_enum_values = set(schema_enum.enums)
        # pylint: disable=protected-access
        persistence_enum_values = set(persistence_enum._member_names_)
        self.assertEqual(
            schema_enum_values,
            persistence_enum_values,
            msg=f'Values of schema enum "{schema_enum.name}" did not match values of '
            f'persistence enum "{persistence_enum.__name__}"',
        )

    @staticmethod
    def _get_all_sqlalchemy_enums_in_module(
        schema_module: ModuleType,
    ) -> List[sqlalchemy.Enum]:
        enums = []
        for attribute_name in dir(schema_module):
            attribute = getattr(schema_module, attribute_name)
            if isinstance(attribute, sqlalchemy.Enum):
                enums.append(attribute)
        return enums


class TestSchemaNoReservedKeywords(TestCase):
    """Test class for validating that our schema tables and columns don't contain
    any reserved keywords for Postgres or BigQuery.
    """

    def testNoReservedKeywords(self) -> None:
        for table in get_all_table_classes():
            self.assertNotIn(table.name.lower(), RESERVED_WORDS)

            for column in table.columns:
                self.assertNotIn(column.name.lower(), RESERVED_WORDS)
