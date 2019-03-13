# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Tests for schema.py."""

from unittest import TestCase

import sqlalchemy

import recidiviz.common.constants.bond as bond
import recidiviz.common.constants.booking as booking
import recidiviz.common.constants.charge as charge
import recidiviz.common.constants.hold as hold
import recidiviz.common.constants.person as person
import recidiviz.common.constants.sentence as sentence
import recidiviz.persistence.database.schema as schema


class SchemaTest(TestCase):
    """Test validating schema file"""

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums
    def testPersistenceAndSchemaEnumsMatch(self):
        # Mapping between name of schema enum and persistence layer enum. This
        # map controls which pairs of enums are tested.
        #
        # If a schema enum does not correspond to a persistence layer enum,
        # it should be mapped to None.
        test_mapping = {
            'gender': person.Gender,
            'race': person.Race,
            'ethnicity': person.Ethnicity,
            'residency_status': person.ResidencyStatus,
            'admission_reason': booking.AdmissionReason,
            'classification': booking.Classification,
            'custody_status': booking.CustodyStatus,
            'release_reason': booking.ReleaseReason,
            'hold_status': hold.HoldStatus,
            'bond_type': bond.BondType,
            'bond_status': bond.BondStatus,
            'sentence_status': sentence.SentenceStatus,
            'sentence_relationship_type': None,
            'degree': charge.ChargeDegree,
            'charge_class': charge.ChargeClass,
            'charge_status': charge.ChargeStatus,
            'court_type': charge.CourtType,
            'time_granularity': None
        }

        schema_enums_by_name = {}
        for attribute_name in dir(schema):
            attribute = getattr(schema, attribute_name)
            if isinstance(attribute, sqlalchemy.Enum):
                schema_enums_by_name[attribute_name] = attribute

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
