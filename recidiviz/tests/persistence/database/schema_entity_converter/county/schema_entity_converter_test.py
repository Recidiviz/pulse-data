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
"""Tests for county/schema_entity_converter.py."""
from datetime import date, datetime
from unittest import TestCase

from more_itertools import one

from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import (
    AdmissionReason,
    Classification,
    CustodyStatus,
    ReleaseReason,
)
from recidiviz.common.constants.county.charge import ChargeClass, ChargeDegree
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.constants.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.persistence.database.schema.county import schema
from recidiviz.persistence.database.schema_entity_converter.county.schema_entity_converter import (
    CountyEntityToSchemaConverter,
    CountySchemaToEntityConverter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.county import entities
from recidiviz.tests.utils import fakes

_PERSON = entities.Person.new_with_defaults(
    external_id="external_id",
    full_name="full_name",
    birthdate=date(year=2000, month=1, day=2),
    birthdate_inferred_from_age=True,
    gender=Gender.MALE,
    gender_raw_text="M",
    race=Race.WHITE,
    race_raw_text="W",
    region="region",
    ethnicity=Ethnicity.NOT_HISPANIC,
    ethnicity_raw_text="NOT HISPANIC",
    residency_status=ResidencyStatus.PERMANENT,
    resident_of_region=True,
    person_id=1234,
    jurisdiction_id="12345678",
    bookings=[
        entities.Booking.new_with_defaults(
            booking_id=2345,
            external_id="external_id",
            admission_date=date(year=2000, month=1, day=3),
            admission_reason=AdmissionReason.TRANSFER,
            admission_reason_raw_text="transfer!",
            admission_date_inferred=True,
            release_date=date(year=2000, month=1, day=4),
            release_date_inferred=True,
            projected_release_date=date(year=2000, month=1, day=5),
            release_reason=ReleaseReason.TRANSFER,
            release_reason_raw_text="TRANSFER",
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text="IN CUSTODY",
            facility="facility",
            classification=Classification.HIGH,
            classification_raw_text="HIGH",
            last_seen_time=datetime(year=2000, month=1, day=6, hour=13),
            first_seen_time=datetime(year=2000, month=1, day=1, hour=3),
            holds=[
                entities.Hold.new_with_defaults(
                    hold_id=3456,
                    external_id="external_id",
                    jurisdiction_name="jurisdiction_name",
                    status=HoldStatus.ACTIVE,
                    status_raw_text="active",
                )
            ],
            arrest=entities.Arrest.new_with_defaults(
                arrest_id=4567,
                external_id="external_id",
                arrest_date=date(year=2000, month=1, day=6),
                location="location",
                agency="agency",
                officer_name="officer_name",
                officer_id="officer_id",
            ),
            charges=[
                entities.Charge.new_with_defaults(
                    charge_id=5678,
                    external_id="external_id",
                    offense_date=date(year=2000, month=1, day=6),
                    statute="statute",
                    name="name",
                    attempted=True,
                    degree=ChargeDegree.FIRST,
                    degree_raw_text="FIRST",
                    charge_class=ChargeClass.FELONY,
                    class_raw_text="F",
                    level="level",
                    fee_dollars=1,
                    charging_entity="charging_entity",
                    status=ChargeStatus.DROPPED,
                    status_raw_text="DROPPED",
                    court_type="CIRCUIT",
                    case_number="case_number",
                    next_court_date=date(year=2000, month=1, day=7),
                    judge_name="judge_name",
                    charge_notes="notes",
                    bond=entities.Bond.new_with_defaults(
                        bond_id=6789,
                        external_id="external_id",
                        amount_dollars=2,
                        bond_type=BondType.CASH,
                        bond_type_raw_text="CASH",
                        status=BondStatus.SET,
                        status_raw_text="SET",
                        bond_agent="bond_agent",
                        booking_id=2345,
                    ),
                    sentence=entities.Sentence.new_with_defaults(
                        sentence_id=7890,
                        external_id="external_id",
                        sentencing_region="sentencing_region",
                        status=SentenceStatus.SERVING,
                        status_raw_text="SERVING",
                        min_length_days=3,
                        max_length_days=4,
                        date_imposed=date(year=2000, month=1, day=2),
                        completion_date=date(year=2001, month=1, day=2),
                        projected_completion_date=date(year=2001, month=1, day=2),
                        is_life=False,
                        is_probation=False,
                        is_suspended=True,
                        fine_dollars=5,
                        parole_possible=True,
                        post_release_supervision_length_days=0,
                        booking_id=2345,
                        related_sentences=[],
                    ),
                )
            ],
        )
    ],
)


class TestCountySchemaEntityConverter(TestCase):
    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def test_convert_person(self):
        schema_person = CountyEntityToSchemaConverter().convert(_PERSON)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(schema_person)
            session.commit()

            people = session.query(schema.Person).all()
            self.assertEqual(len(people), 1)
            self.assertEqual(
                CountySchemaToEntityConverter().convert((one(people))), _PERSON
            )
