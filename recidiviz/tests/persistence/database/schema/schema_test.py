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

import recidiviz.common.constants.county.charge
from recidiviz.common.constants.county import bond
from recidiviz.common.constants.shared_enums import charge, person_characteristics
from recidiviz.persistence.database.schema import shared_enums
from recidiviz.persistence.database.schema.aggregate import schema as aggregate_schema
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_utils import (
    _get_all_database_entities_in_module,
    get_all_table_classes_in_module,
)

ALL_SCHEMA_MODULES = [county_schema, state_schema, aggregate_schema]


class TestSchemaEnums(TestCase):
    """Base test class for validating schema enums are defined correctly"""

    # Mapping between name of schema enum and persistence layer enum. This
    # map controls which pairs of enums are tested.
    #
    # If a schema enum does not correspond to a persistence layer enum,
    # it should be mapped to None.
    SHARED_ENUMS_TEST_MAPPING = {
        "gender": person_characteristics.Gender,
        "race": person_characteristics.Race,
        "ethnicity": person_characteristics.Ethnicity,
        "residency_status": person_characteristics.ResidencyStatus,
        "bond_type": bond.BondType,
        "bond_status": bond.BondStatus,
        "degree": recidiviz.common.constants.county.charge.ChargeDegree,
        "charge_status": charge.ChargeStatus,
    }

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums
    def testPersistenceAndSchemaEnumsMatch(self):
        """Test case ensuring enum values match between persistence layer enums
        and schema enums."""
        self.check_persistence_and_schema_enums_match(
            self.SHARED_ENUMS_TEST_MAPPING, shared_enums
        )

    def testNoOverlappingEnumPostgresNames(self):
        postgres_names_set = set()
        enum_id_set = set()
        for schema_module in ALL_SCHEMA_MODULES:
            enums = self._get_all_sqlalchemy_enums_in_module(schema_module)
            for enum in enums:
                if id(enum) in enum_id_set:
                    continue
                postgres_name = enum.name
                self.assertNotIn(
                    postgres_name,
                    postgres_names_set,
                    f"SQLAlchemy enum with Postgres name [{postgres_name}]"
                    f" (defined in [{schema_module}]) already exists",
                )
                postgres_names_set.add(postgres_name)
                enum_id_set.add(id(enum))

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


class TestSchemaTableConsistency(TestCase):
    """Base test class for validating schema tables are defined correctly"""

    def testNoRepeatTableNames(self):
        table_names_set = set()
        for schema_module in ALL_SCHEMA_MODULES:
            table_classes = get_all_table_classes_in_module(schema_module)
            for table in table_classes:
                table_name = table.name
                self.assertNotIn(
                    table_name,
                    table_names_set,
                    f"Table name [{table_name}] defined in [{schema_module}]) "
                    f"already exists.",
                )
                table_names_set.add(table_name)

    def testNoRepeatDatabaseClassNames(self):
        table_class_names_set = set()
        for schema_module in ALL_SCHEMA_MODULES:
            db_classes = _get_all_database_entities_in_module(schema_module)
            for cls in db_classes:
                class_name = cls.__name__
                self.assertNotIn(
                    class_name,
                    table_class_names_set,
                    f"Table name [{class_name}] defined in "
                    f"[{schema_module}]) already exists.",
                )

    def testAllTableNamesMatchClassNames(self):
        for schema_module in ALL_SCHEMA_MODULES:
            for cls in _get_all_database_entities_in_module(schema_module):
                table_name = cls.__tablename__
                table_name_to_capital_case = table_name.title().replace("_", "")
                self.assertEqual(
                    table_name_to_capital_case,
                    cls.__name__,
                    f"Table class {cls.__name__} does not have matching table "
                    f"name: {table_name}",
                )

    def testDatabaseEntityFunctionality(self):
        for schema_module in ALL_SCHEMA_MODULES:
            for cls in _get_all_database_entities_in_module(schema_module):
                primary_key_col_name = cls.get_primary_key_column_name()
                self.assertIsNotNone(primary_key_col_name)
                primary_key_prop_name = cls.get_column_property_names()
                self.assertTrue(len(primary_key_prop_name) > 0)
                self.assertTrue(primary_key_col_name in primary_key_prop_name)
                # Just should not crash
                cls.get_foreign_key_names()
                cls.get_relationship_property_names()
                cls.get_relationship_property_names_and_properties()
