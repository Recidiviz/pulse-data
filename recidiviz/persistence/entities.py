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
# ============================================================================
"""Domain logic entities used in the persistence layer.

Note: These classes mirror the SQL Alchemy ORM objects but are kept separate.
This allows these persistence layer objects additional flexibility that the SQL
Alchemy ORM objects can't provide.
"""
import attr

from recidiviz.common.buildable_attr import BuildableAttr


@attr.s
class Person(BuildableAttr):
    external_id = attr.ib()
    surname = attr.ib()
    given_names = attr.ib()
    birthdate = attr.ib()
    birthdate_inferred_from_age = attr.ib()
    gender = attr.ib()
    race = attr.ib()
    region = attr.ib()
    ethnicity = attr.ib()
    place_of_residence = attr.ib()

    person_id = attr.ib(default=None)
    bookings = attr.ib(factory=list)


@attr.s
class Booking(object):
    booking_id = attr.ib(default=None)
    external_id = attr.ib(default=None)
    admission_date = attr.ib(default=None)
    admission_date_inferred = attr.ib(default=None)
    release_date = attr.ib(default=None)
    release_date_inferred = attr.ib(default=None)
    projected_release_date = attr.ib(default=None)
    release_reason = attr.ib(default=None)
    custody_status = attr.ib(default=None)
    held_for_other_jurisdiction = attr.ib(default=None)
    facility = attr.ib(default=None)
    classification = attr.ib(default=None)
    last_seen_time = attr.ib(default=None)

    holds = attr.ib(factory=list)
    arrest = attr.ib(default=None)
    charges = attr.ib(factory=list)


@attr.s
class Hold(object):
    hold_id = attr.ib(default=None)
    external_id = attr.ib(default=None)
    jurisdiction_name = attr.ib(default=None)
    hold_status = attr.ib(default=None)


@attr.s
class Arrest(object):
    arrest_id = attr.ib(default=None)
    external_id = attr.ib(default=None)
    date = attr.ib(default=None)
    location = attr.ib(default=None)
    agency = attr.ib(default=None)
    officer_name = attr.ib(default=None)
    officer_id = attr.ib(default=None)


@attr.s
class Charge(object):
    charge_id = attr.ib(default=None)
    external_id = attr.ib(default=None)
    offense_date = attr.ib(default=None)
    statute = attr.ib(default=None)
    name = attr.ib(default=None)
    attempted = attr.ib(default=None)
    degree = attr.ib(default=None)
    charge_class = attr.ib(default=None)
    level = attr.ib(default=None)
    fee_dollars = attr.ib(default=None)
    charging_entity = attr.ib(default=None)
    status = attr.ib(default=None)
    court_type = attr.ib(default=None)
    case_number = attr.ib(default=None)
    next_court_date = attr.ib(default=None)
    judge_name = attr.ib(default=None)

    bond = attr.ib(default=None)
    sentence = attr.ib(default=None)


@attr.s
class Bond(object):
    bond_id = attr.ib(default=None)
    external_id = attr.ib(default=None)
    amount_dollars = attr.ib(default=None)
    bond_type = attr.ib(default=None)
    status = attr.ib(default=None)


@attr.s
class Sentence(object):
    sentence_id = attr.ib(default=None)
    external_id = attr.ib(default=None)
    date_imposed = attr.ib(default=None)
    county_of_commitment = attr.ib(default=None)
    min_length_days = attr.ib(default=None)
    max_length_days = attr.ib(default=None)
    is_life = attr.ib(default=None)
    is_probation = attr.ib(default=None)
    is_suspended = attr.ib(default=None)
    fine_dollars = attr.ib(default=None)
    parole_possible = attr.ib(default=None)
    post_release_supervision_length_days = attr.ib(default=None)

    # To avoid recursive references, store only 1 level of related_sentences
    # (ie. don't store related_sentences of these related_sentences).
    related_sentences = attr.ib(default=None)
