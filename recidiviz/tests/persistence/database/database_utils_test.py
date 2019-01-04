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

from recidiviz.persistence import entities
from recidiviz.persistence.database.database_utils import convert_person

_PERSON = entities.Person(
    external_id="external_id",
    surname="surname",
    given_names="given_names",
    birthdate=date(year=2000, month=1, day=2),
    birthdate_inferred_from_age=True,
    gender="gender",
    race="race",
    region="region",
    ethnicity="ethnicity",
    place_of_residence="residence",
    person_id=1234,
    bookings=[entities.Booking(
        booking_id=2345,
        external_id="external_id",
        admission_date=date(year=2000, month=1, day=3),
        admission_date_inferred=True,
        release_date=date(year=2000, month=1, day=4),
        release_date_inferred=True,
        projected_release_date=date(year=2000, month=1, day=5),
        release_reason="release_reason",
        custody_status="custody_status",
        held_for_other_jurisdiction="held_for_other_jurisdiction",
        facility="facility",
        classification="classification",
        last_seen_time=datetime(year=2000, month=1, day=6, hour=13),
        holds=[entities.Hold(
            hold_id=3456,
            external_id="external_id",
            jurisdiction_name="jurisdiction_name",
            hold_status="hold_status",
        )],
        arrest=entities.Arrest(
            arrest_id=4567,
            external_id="external_id",
            date=date(year=2000, month=1, day=6),
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
            degree="degree",
            charge_class="charge_class",
            level="level",
            fee_dollars=1,
            charging_entity="charging_entity",
            status="status",
            court_type="court_type",
            case_number="case_number",
            next_court_date=date(year=2000, month=1, day=7),
            judge_name="judge_name",

            bond=entities.Bond(
                bond_id=6789,
                external_id="external_id",
                amount_dollars=2,
                bond_type="bond_type",
                status="status"
            ),
            sentence=entities.Sentence(
                sentence_id="sentence_id",
                external_id="external_id",
                date_imposed=date(year=2000, month=1, day=8),
                min_length_days=3,
                max_length_days=4,
                is_life=False,
                is_probation=False,
                is_suspended=True,
                fine_dollars=5,
                parole_possible=True,
                post_release_supervision_length_days=0,
                related_sentences=[]
            )
        )]
    )]
)


class TestDatabaseUtils(TestCase):

    def test_convert_person(self):
        result = convert_person(convert_person(_PERSON))
        self.assertEqual(_PERSON, result)
