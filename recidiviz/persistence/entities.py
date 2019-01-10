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
from typing import List, Optional

import datetime
import attr

from recidiviz.common.buildable_attr import BuildableAttr
from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.booking import ReleaseReason, CustodyStatus, \
    Classification
from recidiviz.common.constants.charge import ChargeClass, ChargeDegree, \
    ChargeStatus, CourtType
from recidiviz.common.constants.person import Race, Ethnicity, Gender


@attr.s
class Person(BuildableAttr):
    external_id: Optional[str] = attr.ib()
    surname: Optional[str] = attr.ib()
    given_names: Optional[str] = attr.ib()
    birthdate: Optional[datetime.date] = attr.ib()
    birthdate_inferred_from_age: Optional[bool] = attr.ib()
    gender: Optional[Gender] = attr.ib()
    race: Optional[Race] = attr.ib()
    region: Optional[str] = attr.ib()
    ethnicity: Optional[Ethnicity] = attr.ib()
    place_of_residence: Optional[str] = attr.ib()

    person_id: Optional[int] = attr.ib(default=None)
    bookings: List['Booking'] = attr.ib(factory=list)


@attr.s
class Booking(BuildableAttr):
    external_id: Optional[str] = attr.ib()
    admission_date: Optional[datetime.date] = attr.ib()
    admission_date_inferred: Optional[bool] = attr.ib()
    release_date: Optional[datetime.date] = attr.ib()
    release_date_inferred: Optional[bool] = attr.ib()
    projected_release_date: Optional[datetime.date] = attr.ib()
    release_reason: Optional[ReleaseReason] = attr.ib()
    custody_status: Optional[CustodyStatus] = attr.ib()
    facility: Optional[str] = attr.ib()
    classification: Optional[Classification] = attr.ib()
    last_seen_time: Optional[datetime.datetime] = attr.ib()

    booking_id: Optional[int] = attr.ib(default=None)
    holds: List['Hold'] = attr.ib(factory=list)
    arrest: Optional['Arrest'] = attr.ib(default=None)
    charges: List['Charge'] = attr.ib(factory=list)


@attr.s
class Hold:
    external_id: Optional[str] = attr.ib(default=None)
    jurisdiction_name: Optional[str] = attr.ib(default=None)
    hold_status: Optional[str] = attr.ib(default=None)

    hold_id: Optional[int] = attr.ib(default=None)


@attr.s
class Arrest(BuildableAttr):
    external_id: Optional[str] = attr.ib()
    date: Optional[datetime.date] = attr.ib()
    location: Optional[str] = attr.ib()
    agency: Optional[str] = attr.ib()
    officer_name: Optional[str] = attr.ib()
    officer_id: Optional[str] = attr.ib()

    arrest_id: Optional[int] = attr.ib(default=None)


@attr.s
class Charge(BuildableAttr):
    external_id: Optional[str] = attr.ib()
    offense_date: Optional[datetime.date] = attr.ib()
    statute: Optional[str] = attr.ib()
    name: Optional[str] = attr.ib()
    attempted: Optional[bool] = attr.ib()
    degree: Optional[ChargeDegree] = attr.ib()
    charge_class: Optional[ChargeClass] = attr.ib()
    level: Optional[str] = attr.ib()
    fee_dollars: Optional[int] = attr.ib()
    charging_entity: Optional[str] = attr.ib()
    status: Optional[ChargeStatus] = attr.ib()
    court_type: Optional[CourtType] = attr.ib()
    case_number: Optional[str] = attr.ib()
    next_court_date: Optional[datetime.date] = attr.ib()
    judge_name: Optional[str] = attr.ib()

    charge_id: Optional[int] = attr.ib(default=None)
    bond: Optional['Bond'] = attr.ib(default=None)
    sentence: Optional['Sentence'] = attr.ib(default=None)


@attr.s
class Bond:
    external_id: Optional[Optional[str]] = attr.ib(default=None)
    amount_dollars: Optional[int] = attr.ib(default=None)
    bond_type: Optional[BondType] = attr.ib(default=None)
    status: Optional[BondStatus] = attr.ib(default=None)

    bond_id: Optional[int] = attr.ib(default=None)


@attr.s
class Sentence:
    external_id: Optional[str] = attr.ib(default=None)
    date_imposed: Optional[datetime.date] = attr.ib(default=None)
    sentencing_region: Optional[str] = attr.ib(default=None)
    min_length_days: Optional[int] = attr.ib(default=None)
    max_length_days: Optional[int] = attr.ib(default=None)
    is_life: Optional[bool] = attr.ib(default=None)
    is_probation: Optional[bool] = attr.ib(default=None)
    is_suspended: Optional[bool] = attr.ib(default=None)
    fine_dollars: Optional[int] = attr.ib(default=None)
    parole_possible: Optional[bool] = attr.ib(default=None)
    post_release_supervision_length_days: Optional[int] = attr.ib(default=None)

    sentence_id: Optional[int] = attr.ib(default=None)

    # To avoid recursive references, store only 1 level of related_sentences
    # (ie. don't store related_sentences of these related_sentences).
    related_sentences: List['Sentence'] = attr.ib(factory=list)
