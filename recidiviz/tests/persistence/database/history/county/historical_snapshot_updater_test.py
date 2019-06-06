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
"""Tests for CountyHistoricalSnapshotUpdater"""

import datetime

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
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.tests.persistence.database.history.\
    base_historical_snapshot_updater_test import (
        BaseHistoricalSnapshotUpdaterTest
    )


_SCHEMA_OBJECT_TYPES_TO_IGNORE = [
    # TODO(#1145): remove once sentence relationships are implemented
    'SentenceRelationship',
]


class TestCountyHistoricalSnapshotUpdater(BaseHistoricalSnapshotUpdaterTest):
    """Tests for CountyHistoricalSnapshotUpdater"""

    def testConvertCountyRecordTree(self):
        person_id = 143
        booking_id = 938
        hold_id = 9945
        arrest_id = 861
        charge_id = 11111
        bond_id = 22222
        sentence_id = 12345

        person = county_schema.Person(
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
        booking = county_schema.Booking(
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
        hold = county_schema.Hold(
            hold_id=hold_id,
            booking_id=booking_id,
            external_id='hold_id',
            jurisdiction_name='some jurisdiction',
            status=HoldStatus.INFERRED_DROPPED.value,
            status_raw_text=None,
        )
        booking.holds.append(hold)
        arrest = county_schema.Arrest(
            arrest_id=arrest_id,
            booking_id=booking_id,
            external_id='arrest_id',
            location='somewhere',
            agency='some agency',
            officer_name='some officer',
            officer_id='some officer ID',
        )
        booking.arrest = arrest
        charge = county_schema.Charge(
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
        bond = county_schema.Bond(
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
        sentence = county_schema.Sentence(
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

        self._check_person_has_relationships_to_all_schema_object_types(
            person, county_schema, _SCHEMA_OBJECT_TYPES_TO_IGNORE)

        ingest_time = datetime.datetime(2018, 7, 30)
        self._commit_person(person,
                            SystemLevel.COUNTY,
                            ingest_time)

        all_schema_objects = [person, booking, hold, arrest,
                              charge, bond, sentence]

        self._check_all_non_history_schema_object_types_in_list(
            all_schema_objects, county_schema, _SCHEMA_OBJECT_TYPES_TO_IGNORE)

        ingest_time_overrides = {
            id(sentence): datetime.datetime(2018, 7, 14)
        }

        for schema_object in all_schema_objects:
            expected_ingest_time = ingest_time
            if id(schema_object) in ingest_time_overrides:
                expected_ingest_time = ingest_time_overrides[id(schema_object)]

            self._assert_expected_snapshots_for_schema_object(
                schema_object, [expected_ingest_time])
