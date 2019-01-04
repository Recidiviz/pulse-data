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
from typing import List

import datetime
import attr

from recidiviz.common.buildable_attr import BuildableAttr


@attr.s
class Person(BuildableAttr):
    external_id: str = attr.ib()
    surname: str = attr.ib()
    given_names: str = attr.ib()
    birthdate: datetime.date = attr.ib()
    birthdate_inferred_from_age: bool = attr.ib()
    gender: str = attr.ib()
    race: str = attr.ib()
    region: str = attr.ib()
    ethnicity: str = attr.ib()
    place_of_residence: str = attr.ib()

    person_id: int = attr.ib(default=None)
    bookings: List['Booking'] = attr.ib(factory=list)


@attr.s
class Booking:
    external_id: str = attr.ib(default=None)
    admission_date: datetime.date = attr.ib(default=None)
    admission_date_inferred: bool = attr.ib(default=None)
    release_date: datetime.date = attr.ib(default=None)
    release_date_inferred: bool = attr.ib(default=None)
    projected_release_date: datetime.date = attr.ib(default=None)
    release_reason: str = attr.ib(default=None)
    custody_status: str = attr.ib(default=None)
    held_for_other_jurisdiction: str = attr.ib(default=None)
    facility: str = attr.ib(default=None)
    classification: str = attr.ib(default=None)
    last_seen_time: datetime.datetime = attr.ib(default=None)

    booking_id: int = attr.ib(default=None)
    holds: List['Hold'] = attr.ib(factory=list)
    arrest: 'Arrest' = attr.ib(default=None)
    charges: List['Charge'] = attr.ib(factory=list)


@attr.s
class Hold:
    external_id: str = attr.ib(default=None)
    jurisdiction_name: str = attr.ib(default=None)
    hold_status: str = attr.ib(default=None)

    hold_id: int = attr.ib(default=None)


@attr.s
class Arrest:
    external_id: str = attr.ib(default=None)
    date: datetime.date = attr.ib(default=None)
    location: str = attr.ib(default=None)
    agency: str = attr.ib(default=None)
    officer_name: str = attr.ib(default=None)
    officer_id: str = attr.ib(default=None)

    arrest_id: int = attr.ib(default=None)


@attr.s
class Charge:
    external_id: str = attr.ib(default=None)
    offense_date: datetime.date = attr.ib(default=None)
    statute: str = attr.ib(default=None)
    name: str = attr.ib(default=None)
    attempted: bool = attr.ib(default=None)
    degree: str = attr.ib(default=None)
    charge_class: str = attr.ib(default=None)
    level: str = attr.ib(default=None)
    fee_dollars: int = attr.ib(default=None)
    charging_entity: str = attr.ib(default=None)
    status: str = attr.ib(default=None)
    court_type: str = attr.ib(default=None)
    case_number: str = attr.ib(default=None)
    next_court_date: datetime.date = attr.ib(default=None)
    judge_name: str = attr.ib(default=None)

    charge_id: int = attr.ib(default=None)
    bond: 'Bond' = attr.ib(default=None)
    sentence: 'Sentence' = attr.ib(default=None)


@attr.s
class Bond:
    external_id: str = attr.ib(default=None)
    amount_dollars: int = attr.ib(default=None)
    bond_type: str = attr.ib(default=None)
    status: str = attr.ib(default=None)

    bond_id: int = attr.ib(default=None)


@attr.s
class Sentence:
    external_id: str = attr.ib(default=None)
    date_imposed: datetime.date = attr.ib(default=None)
    sentencing_region: str = attr.ib(default=None)
    min_length_days: int = attr.ib(default=None)
    max_length_days: int = attr.ib(default=None)
    is_life: bool = attr.ib(default=None)
    is_probation: bool = attr.ib(default=None)
    is_suspended: bool = attr.ib(default=None)
    fine_dollars: int = attr.ib(default=None)
    parole_possible: bool = attr.ib(default=None)
    post_release_supervision_length_days: int = attr.ib(default=None)

    sentence_id: str = attr.ib(default=None)

    # To avoid recursive references, store only 1 level of related_sentences
    # (ie. don't store related_sentences of these related_sentences).
    related_sentences: List['Sentence'] = attr.ib(default=None)
