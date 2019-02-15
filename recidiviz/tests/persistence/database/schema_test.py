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
from sqlalchemy.inspection import inspect

import recidiviz.common.constants.bond as bond
import recidiviz.common.constants.booking as booking
import recidiviz.common.constants.charge as charge
import recidiviz.common.constants.hold as hold
import recidiviz.common.constants.person as person
import recidiviz.common.constants.sentence as sentence
import recidiviz.persistence.database.schema as schema
from recidiviz.persistence.database.schema import (
    Arrest, ArrestHistory,
    Bond, BondHistory,
    Booking, BookingHistory,
    Charge, ChargeHistory,
    Hold, HoldHistory,
    Person, PersonHistory,
    Sentence, SentenceHistory,
    SentenceRelationship, SentenceRelationshipHistory)


class SchemaTest(TestCase):
    """Test validating schema file"""

    # Test cases matching columns in master and historical tables for each
    # entity type

    # NOTE: This constraint is primarily enforced by the used of "SharedColumn"
    # mixins between the master and historical schema objects. However, as
    # foreign key columns cannot be shared in that manner (because foreign keys
    # on the master tables point to unique entities and foreign keys on the
    # historical tables do not, and therefore foreign key constraints can only
    # be enforced on the former), this test still serves as a useful catch for
    # those cases.

    def testMasterAndHistoryColumnsMatch_Arrest(self):
        self._assert_columns_match(Arrest,
                                   ArrestHistory,
                                   table_b_exclusions=[
                                       'arrest_history_id',
                                       'valid_from',
                                       'valid_to'])


    def testMasterAndHistoryColumnsMatch_Bond(self):
        self._assert_columns_match(Bond,
                                   BondHistory,
                                   table_b_exclusions=[
                                       'bond_history_id',
                                       'valid_from',
                                       'valid_to'])


    def testMasterAndHistoryColumnsMatch_Booking(self):
        self._assert_columns_match(Booking,
                                   BookingHistory,
                                   table_a_exclusions=['last_seen_time'],
                                   table_b_exclusions=[
                                       'booking_history_id',
                                       'valid_from',
                                       'valid_to'])


    def testMasterAndHistoryColumnsMatch_Charge(self):
        self._assert_columns_match(Charge,
                                   ChargeHistory,
                                   table_b_exclusions=[
                                       'charge_history_id',
                                       'valid_from',
                                       'valid_to'])


    def testMasterAndHistoryColumnsMatch_Hold(self):
        self._assert_columns_match(Hold,
                                   HoldHistory,
                                   table_b_exclusions=[
                                       'hold_history_id',
                                       'valid_from',
                                       'valid_to'])


    def testMasterAndHistoryColumnsMatch_Person(self):
        self._assert_columns_match(Person,
                                   PersonHistory,
                                   table_a_exclusions=[
                                       'full_name',
                                       'birthdate',
                                       'birthdate_inferred_from_age'],
                                   table_b_exclusions=[
                                       'person_history_id',
                                       'valid_from',
                                       'valid_to'])


    def testMasterAndHistoryColumnsMatch_Sentence(self):
        self._assert_columns_match(Sentence,
                                   SentenceHistory,
                                   table_b_exclusions=[
                                       'sentence_history_id',
                                       'valid_from',
                                       'valid_to'])


    def testMasterAndHistoryColumnsMatch_SentenceRelationship(self):
        self._assert_columns_match(SentenceRelationship,
                                   SentenceRelationshipHistory,
                                   table_b_exclusions=[
                                       'sentence_relationship_history_id',
                                       'valid_from',
                                       'valid_to'])


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
            'report_granularity': None
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


    def _assert_columns_match(self, table_a, table_b, table_a_exclusions=None,
                              table_b_exclusions=None):
        """Asserts that each column in table_a has a corresponding column with
        the same name in table_b, and vice versa.

        Any column name included in a table's list of exclusions will be ignored
        in this comparison.
        """

        if table_a_exclusions is None:
            table_a_exclusions = []
        if table_b_exclusions is None:
            table_b_exclusions = []

        table_a_column_names = {column[0] for column
                                in inspect(table_a).columns.items()}
        table_b_column_names = {column[0] for column
                                in inspect(table_b).columns.items()}

        # Remove all exclusions. This is done for each exclusion separately
        # rather than using a list comprehension so that an error can be thrown
        # if an exclusion is passed for a column that doesn't exist. (The
        # built-in exception for `remove` isn't used because it doesn't display
        # the missing value in the error message.)
        for exclusion in table_a_exclusions:
            if not exclusion in table_a_column_names:
                raise ValueError(
                    '{exclusion} is not a column in {table_name}'.format(
                        exclusion=exclusion, table_name=table_a.__name__))
            table_a_column_names.remove(exclusion)
        for exclusion in table_b_exclusions:
            if not exclusion in table_b_column_names:
                raise ValueError(
                    '{exclusion} is not a column in {table_name}'.format(
                        exclusion=exclusion, table_name=table_b.__name__))
            table_b_column_names.remove(exclusion)

        self.assertEqual(table_a_column_names, table_b_column_names)


    # This test method currently does not account for situations where either
    # enum should have values that are excluded from comparison. If a situation
    # like that arises, this test case can be extended to have hard-coded
    # exclusions in the same way as the master/historical column comparison
    # tests.
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
