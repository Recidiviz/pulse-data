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
"""
Tests for SQLAlchemy enums defined in recidiviz.persistence.database.schema
"""

from unittest import TestCase

import sqlalchemy

import recidiviz.common.constants.bond as bond
import recidiviz.common.constants.charge as charge
import recidiviz.persistence.database.schema.shared_enums as shared_enums


from recidiviz.common.constants import person_characteristics


class SchemaEnumsTest(TestCase):
    """Base test class for validating schema enums are defined correctly"""

    # Mapping between name of schema enum and persistence layer enum. This
    # map controls which pairs of enums are tested.
    #
    # If a schema enum does not correspond to a persistence layer enum,
    # it should be mapped to None.
    SHARED_ENUMS_TEST_MAPPING = {
        'gender': person_characteristics.Gender,
        'race': person_characteristics.Race,
        'ethnicity': person_characteristics.Ethnicity,
        'residency_status': person_characteristics.ResidencyStatus,
        'bond_type': bond.BondType,
        'bond_status': bond.BondStatus,
        'degree': charge.ChargeDegree,
        'charge_status': charge.ChargeStatus,
    }

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums
    def testPersistenceAndSchemaEnumsMatch(self):
        self.check_persistence_and_schema_enums_match(
            self.SHARED_ENUMS_TEST_MAPPING,
            shared_enums)

    def check_persistence_and_schema_enums_match(self,
                                                 test_mapping,
                                                 enums_package):
        schema_enums_by_name = {}
        num_enums = 0
        for attribute_name in dir(enums_package):
            attribute = getattr(enums_package, attribute_name)
            if isinstance(attribute, sqlalchemy.Enum):
                num_enums += 1
                schema_enums_by_name[attribute_name] = attribute
        self.assertNotEqual(0, num_enums,
                            f'No enums found in package [{enums_package}] - is'
                            f' this the correct package to search for schema'
                            f' enums?')

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
            msg='Values of schema enum "{schema_enum}" did not match values of '
                'persistence enum "{persistence_enum}"'.format(
                    schema_enum=schema_enum.name,
                    persistence_enum=persistence_enum.__name__))
