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
"""
Define tables in our SQL Database.

These table definitions should not be used outside the sql_database package!
"""

import enum
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, \
    DateTime, Boolean, Enum


class Dummy(enum.Enum):
    dummy = 1


METADATA = MetaData()

PEOPLE = Table('people', METADATA,
               Column('person_id', Integer, primary_key=True),
               Column('scraped_person_id', Integer),
               Column('surname', String),
               Column('given_names', String),
               Column('birthdate', DateTime),
               Column('birthdate_inferred_from_age', Boolean),
               Column('race', Enum(Dummy)),
               Column('ethnicity', Enum(Dummy)),
               Column('place_of_residence', String))

BOOKINGS = Table('bookings', METADATA,
                 Column('booking_id', Integer, primary_key=True),
                 Column('arrest_id', Integer,
                        ForeignKey("arrests.arrest_id")),
                 Column('charge_id', Integer,
                        ForeignKey("charges.charge_id")),
                 Column('admission_date', DateTime),
                 Column('release_date', DateTime),
                 Column('release_reason', Enum(Dummy)),
                 Column('custody_status', Enum(Dummy)),
                 Column('held_for_other_jurisdiction', Boolean),
                 Column('hold', String),
                 Column('facility', String),
                 Column('classification', Enum(Dummy)))

ARRESTS = Table('arrests', METADATA,
                Column('arrest_id', Integer, primary_key=True),
                Column('date', DateTime),
                Column('location', String),
                Column('agency', String),
                Column('officer_name', String),
                Column('officer_id', String))

CHARGES = Table('charges', METADATA,
                Column('charge_id', Integer, primary_key=True),
                Column('bond_id', Integer,
                       ForeignKey('bonds.bond_id')),
                Column('sentence_id', Integer,
                       ForeignKey('sentences.sentence_id')),
                Column('offence_date', DateTime),
                Column('statute', String),
                Column('offense_code', Integer),
                Column('name', String),
                Column('attempted', Boolean),
                Column('degree', Enum(Dummy)),
                Column('class', Enum(Dummy)),
                Column('level', String),
                Column('fee', Integer),
                Column('charging_entity', String),
                Column('status', Enum(Dummy)),
                Column('number_of_counts', Integer),
                Column('court_type', Enum(Dummy)),
                Column('case_number', String),
                Column('next_court_date', DateTime),
                Column('judge_name', String))

BONDS = Table('bonds', METADATA,
              Column('bond_id', Integer, primary_key=True),
              Column('amount', Integer),
              Column('type', Enum(Dummy)),
              Column('status', Enum(Dummy)))

SENTENCES = Table('sentences', METADATA,
                  Column('sentence_id', Integer, primary_key=True),
                  Column('date_imposed', DateTime),
                  Column('min_length_days', Integer),
                  Column('max_length_days', Integer),
                  Column('is_life', Boolean),
                  Column('is_probation', Boolean),
                  Column('is_suspended', Boolean),
                  Column('fine', Integer),
                  Column('parole_possible', Boolean),
                  Column('post_release_supervision_length_days',
                         Integer),
                  Column('concurrent_with',
                         ForeignKey('sentences.sentence_id')),
                  Column('consecutive_wth',
                         ForeignKey('sentences.sentence_id')))
