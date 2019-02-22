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
"""Tests for database_utils.py."""
from datetime import date, datetime
from unittest import TestCase

from more_itertools import one

from recidiviz import Session
from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.booking import CustodyStatus, ReleaseReason, \
    Classification, AdmissionReason
from recidiviz.common.constants.charge import ChargeDegree, ChargeClass, \
    ChargeStatus, CourtType
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.common.constants.person import Gender, Race, Ethnicity
from recidiviz.common.constants.sentence import SentenceStatus
from recidiviz.persistence import entities
from recidiviz.persistence.database import schema
from recidiviz.persistence.database.database_utils import convert
from recidiviz.tests.utils import fakes

_PERSON = entities.Person(
    external_id="external_id",
    full_name="full_name",
    birthdate=date(year=2000, month=1, day=2),
    birthdate_inferred_from_age=True,
    gender=Gender.MALE,
    gender_raw_text='M',
    race=Race.WHITE,
    race_raw_text='W',
    region="region",
    ethnicity=Ethnicity.NOT_HISPANIC,
    ethnicity_raw_text='NOT HISPANIC',
    place_of_residence="residence",
    person_id=1234,
    bookings=[entities.Booking(
        booking_id=2345,
        external_id="external_id",
        admission_date=date(year=2000, month=1, day=3),
        admission_reason=AdmissionReason.TRANSFER,
        admission_reason_raw_text='transfer!',
        admission_date_inferred=True,
        release_date=date(year=2000, month=1, day=4),
        release_date_inferred=True,
        projected_release_date=date(year=2000, month=1, day=5),
        release_reason=ReleaseReason.TRANSFER,
        release_reason_raw_text='TRANSFER',
        custody_status=CustodyStatus.IN_CUSTODY,
        custody_status_raw_text='IN CUSTODY',
        facility="facility",
        classification=Classification.HIGH,
        classification_raw_text='HIGH',
        last_seen_time=datetime(year=2000, month=1, day=6, hour=13),
        holds=[entities.Hold(
            hold_id=3456,
            external_id="external_id",
            jurisdiction_name="jurisdiction_name",
            status=HoldStatus.ACTIVE,
            status_raw_text='active'
        )],
        arrest=entities.Arrest(
            arrest_id=4567,
            external_id="external_id",
            arrest_date=date(year=2000, month=1, day=6),
            location="location",
            agency="agency",
            officer_name="officer_name",
            officer_id="officer_id",
        ),
        charges=[entities.Charge(
            charge_id=5678,
            external_id="external_id",
            offense_date=date(year=2000, month=1, day=6),
            statute="statute",
            name="name",
            attempted=True,
            degree=ChargeDegree.FIRST,
            degree_raw_text='FIRST',
            charge_class=ChargeClass.FELONY,
            class_raw_text='F',
            level="level",
            fee_dollars=1,
            charging_entity="charging_entity",
            status=ChargeStatus.DROPPED,
            status_raw_text='DROPPED',
            court_type=CourtType.CIRCUIT,
            court_type_raw_text='CIRCUIT',
            case_number="case_number",
            next_court_date=date(year=2000, month=1, day=7),
            judge_name="judge_name",
            charge_notes='notes',

            bond=entities.Bond(
                bond_id=6789,
                external_id="external_id",
                amount_dollars=2,
                bond_type=BondType.CASH,
                bond_type_raw_text='CASH',
                status=BondStatus.SET,
                status_raw_text='SET',
                bond_agent='bond_agent',
                booking_id=2345
            ),
            sentence=entities.Sentence(
                sentence_id=7890,
                external_id="external_id",
                sentencing_region='sentencing_region',
                status=SentenceStatus.SERVING,
                status_raw_text='SERVING',
                min_length_days=3,
                max_length_days=4,
                is_life=False,
                is_probation=False,
                is_suspended=True,
                fine_dollars=5,
                parole_possible=True,
                post_release_supervision_length_days=0,
                booking_id=2345,
                related_sentences=[]
            )
        )]
    )]
)


class TestDatabaseUtils(TestCase):

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_convert_person(self):
        schema_person = convert(_PERSON)
        session = Session()
        session.add(schema_person)
        session.commit()

        people = session.query(schema.Person).all()
        self.assertEqual(len(people), 1)
        self.assertEqual(convert(one(people)), _PERSON)
