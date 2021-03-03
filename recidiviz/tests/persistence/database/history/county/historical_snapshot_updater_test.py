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
from typing import Dict, Type, Optional

from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.county.booking import (
    AdmissionReason,
    Classification,
    CustodyStatus,
    ReleaseReason,
)
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.charge import ChargeClass, ChargeDegree
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.constants.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.entity.core_entity import primary_key_value_from_obj
from recidiviz.tests.persistence.database.history.base_historical_snapshot_updater_test import (
    BaseHistoricalSnapshotUpdaterTest,
)
from recidiviz.tests.utils import fakes

_SCHEMA_OBJECT_TYPES_TO_IGNORE = [
    # TODO(#1145): remove once sentence relationships are implemented
    "SentenceRelationship",
    "ScraperSuccess",
]


class TestCountyHistoricalSnapshotUpdater(BaseHistoricalSnapshotUpdaterTest):
    """Tests for CountyHistoricalSnapshotUpdater"""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def generate_schema_county_person_obj_tree(self) -> county_schema.Person:
        """Test util for generating a Person schema object that has at least one
         child of each possible schema object type defined on county/schema.py.

        Returns:
            A test instance of a Person schema object.
        """
        person_id = 143
        booking_id = 938
        hold_id = 9945
        arrest_id = 861
        charge_id = 11111
        bond_id = 22222
        sentence_id = 12345

        person = county_schema.Person(
            person_id=person_id,
            full_name="name",
            birthdate=datetime.date(1980, 1, 5),
            birthdate_inferred_from_age=False,
            external_id="some_id",
            gender=Gender.EXTERNAL_UNKNOWN.value,
            gender_raw_text="Unknown",
            race=Race.OTHER.value,
            race_raw_text="Other",
            ethnicity=Ethnicity.EXTERNAL_UNKNOWN.value,
            ethnicity_raw_text="Unknown",
            residency_status=ResidencyStatus.TRANSIENT.value,
            resident_of_region=False,
            region="somewhere",
            jurisdiction_id="12345678",
        )
        booking = county_schema.Booking(
            booking_id=booking_id,
            person_id=person_id,
            external_id="booking_id",
            admission_date=datetime.date(2018, 7, 12),
            admission_date_inferred=True,
            admission_reason=AdmissionReason.TRANSFER.value,
            admission_reason_raw_text="Transferred",
            release_date=datetime.date(2018, 7, 30),
            release_date_inferred=False,
            projected_release_date=datetime.date(2018, 7, 25),
            release_reason=ReleaseReason.ACQUITTAL.value,
            release_reason_raw_text="Acquitted",
            custody_status=CustodyStatus.RELEASED.value,
            custody_status_raw_text="Released",
            facility="some facility",
            classification=Classification.MEDIUM.value,
            classification_raw_text="M",
            last_seen_time=datetime.datetime(2018, 7, 30),
            first_seen_time=datetime.datetime(2018, 7, 12),
        )
        person.bookings.append(booking)
        hold = county_schema.Hold(
            hold_id=hold_id,
            booking_id=booking_id,
            external_id="hold_id",
            jurisdiction_name="some jurisdiction",
            status=HoldStatus.INFERRED_DROPPED.value,
            status_raw_text=None,
        )
        booking.holds.append(hold)
        arrest = county_schema.Arrest(
            arrest_id=arrest_id,
            booking_id=booking_id,
            external_id="arrest_id",
            location="somewhere",
            agency="some agency",
            officer_name="some officer",
            officer_id="some officer ID",
        )
        booking.arrest = arrest
        charge = county_schema.Charge(
            charge_id=charge_id,
            booking_id=booking_id,
            bond_id=bond_id,
            sentence_id=sentence_id,
            external_id="charge_id",
            offense_date=datetime.date(2018, 7, 1),
            statute="some statute",
            name="charge name",
            attempted=False,
            degree=ChargeDegree.SECOND.value,
            degree_raw_text="2nd",
            charge_class=ChargeClass.CIVIL.value,
            class_raw_text="Civil",
            level="some level",
            fee_dollars=200,
            charging_entity="some entity",
            status=ChargeStatus.ACQUITTED.value,
            status_raw_text="Acquitted",
            court_type="court type",
            case_number="case_number",
            next_court_date=datetime.date(2018, 7, 14),
            judge_name="some name",
            charge_notes="some notes",
        )
        booking.charges.append(charge)
        bond = county_schema.Bond(
            bond_id=bond_id,
            booking_id=booking_id,
            external_id="bond_id",
            amount_dollars=2000,
            bond_type=BondType.CASH.value,
            bond_type_raw_text="Cash bond",
            status=BondStatus.POSTED.value,
            status_raw_text="Posted",
            bond_agent="some bond agent",
        )
        charge.bond = bond
        sentence = county_schema.Sentence(
            sentence_id=sentence_id,
            booking_id=booking_id,
            external_id="sentence_id",
            status=SentenceStatus.COMMUTED.value,
            status_raw_text="Commuted",
            sentencing_region="some region",
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

        return person

    def testConvertCountyRecordTree(self):
        person = self.generate_schema_county_person_obj_tree()

        ingest_time = datetime.datetime(2018, 7, 30)
        self._commit_person(person, SystemLevel.COUNTY, ingest_time)

        all_schema_objects = self._get_all_schema_objects_in_db(
            county_schema.Person, county_schema, _SCHEMA_OBJECT_TYPES_TO_IGNORE
        )

        ingest_time_overrides_by_type: Dict[
            Type[DatabaseEntity], Dict[int, datetime.date]
        ] = {
            county_schema.Sentence: {
                12345: datetime.datetime(2018, 7, 14),
            }
        }

        def _get_ingest_time_override_for_obj(
            obj: DatabaseEntity,
        ) -> Optional[datetime.date]:
            obj_type = type(obj)
            if obj_type in ingest_time_overrides_by_type:
                overrides_for_type = ingest_time_overrides_by_type[obj_type]
                if primary_key in overrides_for_type:
                    return overrides_for_type[primary_key]

            return None

        for schema_object in all_schema_objects:
            primary_key = primary_key_value_from_obj(schema_object)
            expected_ingest_time = (
                _get_ingest_time_override_for_obj(schema_object) or ingest_time
            )

            self._assert_expected_snapshots_for_schema_object(
                schema_object, [expected_ingest_time]
            )
