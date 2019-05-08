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

"""Tests for snapshot conversion logic in update_historical_snapshots"""

import datetime
from inspect import isclass
from typing import Set
from unittest import TestCase

from more_itertools import one
from recidiviz import Session
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.county.booking import \
    AdmissionReason, Classification, CustodyStatus, ReleaseReason
from recidiviz.common.constants.charge import \
    ChargeDegree, ChargeStatus
from recidiviz.common.constants.county.charge import ChargeClass
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.constants.person_characteristics import \
    Ethnicity, Gender, Race, ResidencyStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.schema.county import schema
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.history.update_historical_snapshots import \
    update_historical_snapshots
from recidiviz.tests.utils import fakes


_ENTITY_TYPES_TO_IGNORE = [
    # TODO(#1145): remove once sentence relationships are implemented
    'SentenceRelationship',
]


class TestSnapshotConversion(TestCase):
    """Test that all database entity types can be converted correctly to their
    corresponding historical snapshots
    """

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testConvertRecordTree(self):
        person_id = 143
        booking_id = 938
        hold_id = 9945
        arrest_id = 861
        charge_id = 11111
        bond_id = 22222
        sentence_id = 12345

        person = schema.Person(
            person_id=person_id,
            full_name='name',
            birthdate=datetime.date(1980, 1, 5),
            birthdate_inferred_from_age=False,
            external_id='some_id',
            gender=Gender.EXTERNAL_UNKNOWN.value,
            gender_raw_text='Unknown',
            race=Race.OTHER.value,
            race_raw_text='Other',
            ethnicity=Ethnicity.EXTERNAL_UNKNOWN.value,
            ethnicity_raw_text='Unknown',
            residency_status=ResidencyStatus.TRANSIENT.value,
            resident_of_region=False,
            region='somewhere',
            jurisdiction_id='12345678',
        )
        booking = schema.Booking(
            booking_id=booking_id,
            person_id=person_id,
            external_id='booking_id',
            admission_date=datetime.date(2018, 7, 12),
            admission_date_inferred=True,
            admission_reason=AdmissionReason.TRANSFER.value,
            admission_reason_raw_text='Transferred',
            release_date=datetime.date(2018, 7, 30),
            release_date_inferred=False,
            projected_release_date=datetime.date(2018, 7, 25),
            release_reason=ReleaseReason.ACQUITTAL.value,
            release_reason_raw_text='Acquitted',
            custody_status=CustodyStatus.RELEASED.value,
            custody_status_raw_text='Released',
            facility='some facility',
            classification=Classification.MEDIUM.value,
            classification_raw_text='M',
            last_seen_time=datetime.datetime(2018, 7, 30),
            first_seen_time=datetime.datetime(2018, 7, 12),
        )
        person.bookings.append(booking)
        hold = schema.Hold(
            hold_id=hold_id,
            booking_id=booking_id,
            external_id='hold_id',
            jurisdiction_name='some jurisdiction',
            status=HoldStatus.INFERRED_DROPPED.value,
            status_raw_text=None,
        )
        booking.holds.append(hold)
        arrest = schema.Arrest(
            arrest_id=arrest_id,
            booking_id=booking_id,
            external_id='arrest_id',
            location='somewhere',
            agency='some agency',
            officer_name='some officer',
            officer_id='some officer ID',
        )
        booking.arrest = arrest
        charge = schema.Charge(
            charge_id=charge_id,
            booking_id=booking_id,
            bond_id=bond_id,
            sentence_id=sentence_id,
            external_id='charge_id',
            offense_date=datetime.date(2018, 7, 1),
            statute='some statute',
            name='charge name',
            attempted=False,
            degree=ChargeDegree.SECOND.value,
            degree_raw_text='2nd',
            charge_class=ChargeClass.CIVIL.value,
            class_raw_text='Civil',
            level='some level',
            fee_dollars=200,
            charging_entity='some entity',
            status=ChargeStatus.ACQUITTED.value,
            status_raw_text='Acquitted',
            court_type='court type',
            case_number='case_number',
            next_court_date=datetime.date(2018, 7, 14),
            judge_name='some name',
            charge_notes='some notes',
        )
        booking.charges.append(charge)
        bond = schema.Bond(
            bond_id=bond_id,
            booking_id=booking_id,
            external_id='bond_id',
            amount_dollars=2000,
            bond_type=BondType.CASH.value,
            bond_type_raw_text='Cash bond',
            status=BondStatus.POSTED.value,
            status_raw_text='Posted',
            bond_agent='some bond agent',
        )
        charge.bond = bond
        sentence = schema.Sentence(
            sentence_id=sentence_id,
            booking_id=booking_id,
            external_id='sentence_id',
            status=SentenceStatus.COMMUTED.value,
            status_raw_text='Commuted',
            sentencing_region='some region',
            min_length_days=90,
            max_length_days=180,
            date_imposed=datetime.date(2018, 7, 14),
            completion_date=datetime.date(2018, 7, 30),
            projected_completion_date=datetime.date(2018, 10, 1),
            is_life=False,
            is_probation=False,
            is_suspended=False,
            fine_dollars=500,
            parole_possible=True,
            post_release_supervision_length_days=60,
        )
        charge.sentence = sentence

        provided_entity_types = \
            self._get_all_entity_types_in_record_tree(person)

        # Ensure this test covers all entity types
        expected_entity_types = set()
        for attribute_name in dir(schema):
            attribute = getattr(schema, attribute_name)
            # Find all master (non-historical) entity types
            if isclass(attribute) and attribute is not DatabaseEntity and \
                    issubclass(attribute, DatabaseEntity) \
                    and not attribute_name.endswith('History'):
                expected_entity_types.add(attribute_name)

        missing_entity_types = []
        for entity_type in expected_entity_types:
            if entity_type not in provided_entity_types and \
                    entity_type not in _ENTITY_TYPES_TO_IGNORE:
                missing_entity_types.append(entity_type)
        if missing_entity_types:
            self.fail('Expected entity type(s) {} not found in provided entity '
                      'types'.format(', '.join(missing_entity_types)))

        act_session = Session()
        act_session.merge(person)

        metadata = IngestMetadata(region='somewhere', jurisdiction_id='12345',
                                  ingest_time=datetime.datetime(2018, 7, 30))
        update_historical_snapshots(act_session, [person], [], metadata)

        act_session.commit()
        act_session.close()

        assert_session = Session()

        person_snapshot = one(assert_session.query(schema.PersonHistory).filter(
            schema.PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(
            schema.BookingHistory).filter(
                schema.BookingHistory.booking_id == booking_id).all())
        hold_snapshot = one(assert_session.query(schema.HoldHistory).filter(
            schema.HoldHistory.hold_id == hold_id).all())
        arrest_snapshot = one(assert_session.query(schema.ArrestHistory).filter(
            schema.ArrestHistory.arrest_id == arrest_id).all())
        charge_snapshot = one(assert_session.query(schema.ChargeHistory).filter(
            schema.ChargeHistory.charge_id == charge_id).all())
        bond_snapshot = one(assert_session.query(schema.BondHistory).filter(
            schema.BondHistory.bond_id == bond_id).all())
        sentence_snapshot = one(assert_session.query(
            schema.SentenceHistory).filter(
                schema.SentenceHistory.sentence_id == sentence_id).all())

        self._assert_entity_and_snapshot_match(person, person_snapshot)
        self._assert_entity_and_snapshot_match(booking, booking_snapshot)
        self._assert_entity_and_snapshot_match(hold, hold_snapshot)
        self._assert_entity_and_snapshot_match(arrest, arrest_snapshot)
        self._assert_entity_and_snapshot_match(charge, charge_snapshot)
        self._assert_entity_and_snapshot_match(bond, bond_snapshot)
        self._assert_entity_and_snapshot_match(sentence, sentence_snapshot)

        assert_session.commit()
        assert_session.close()

    @staticmethod
    def _get_all_entity_types_in_record_tree(
            person: schema.Person) -> Set[str]:
        entity_types = set()

        unprocessed = list([person])
        processed = []
        while unprocessed:
            entity = unprocessed.pop()
            entity_types.add(type(entity).__name__)
            processed.append(entity)

            related_entities = []
            for relationship_name in entity.get_relationship_property_names():
                related = getattr(entity, relationship_name)
                # Relationship can return either a list or a single item
                if isinstance(related, list):
                    related_entities.extend(related)
                elif related is not None:
                    related_entities.append(related)

            unprocessed.extend([related_entity for related_entity
                                in related_entities
                                if related_entity not in processed
                                and related_entity not in unprocessed])

        return entity_types

    def _assert_entity_and_snapshot_match(
            self, entity, historical_snapshot) -> None:
        shared_property_names = \
            type(entity).get_column_property_names().intersection(
                type(historical_snapshot).get_column_property_names())
        for column_property_name in shared_property_names:
            entity_value = getattr(entity, column_property_name)
            historical_value = getattr(
                historical_snapshot, column_property_name)
            self.assertEqual(entity_value, historical_value)
