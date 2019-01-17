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
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.common.constants.person import Race, Ethnicity, Gender


@attr.s
class Entity:
    external_id: Optional[str] = attr.ib()

    # Consider Entity abstract and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is Entity:
            raise Exception('Abstract class cannot be instantiated')
        return super().__new__(cls)

@attr.s
class Person(Entity, BuildableAttr):
    full_name: Optional[str] = attr.ib()
    birthdate: Optional[datetime.date] = attr.ib()
    birthdate_inferred_from_age: Optional[bool] = attr.ib()
    gender: Optional[Gender] = attr.ib()
    gender_raw_text: Optional[str] = attr.ib()
    race: Optional[Race] = attr.ib()
    race_raw_text: Optional[str] = attr.ib()
    region: Optional[str] = attr.ib()
    ethnicity: Optional[Ethnicity] = attr.ib()
    ethnicity_raw_text: Optional[str] = attr.ib()
    place_of_residence: Optional[str] = attr.ib()

    person_id: Optional[int] = attr.ib(default=None)
    bookings: List['Booking'] = attr.ib(factory=list)


@attr.s
class Booking(Entity, BuildableAttr):
    admission_date: Optional[datetime.date] = attr.ib()
    admission_date_inferred: Optional[bool] = attr.ib()
    release_date: Optional[datetime.date] = attr.ib()
    release_date_inferred: Optional[bool] = attr.ib()
    projected_release_date: Optional[datetime.date] = attr.ib()
    release_reason: Optional[ReleaseReason] = attr.ib()
    release_reason_raw_text: Optional[str] = attr.ib()
    custody_status: Optional[CustodyStatus] = attr.ib()
    custody_status_raw_text: Optional[str] = attr.ib()
    facility: Optional[str] = attr.ib()
    classification: Optional[Classification] = attr.ib()
    classification_raw_text: Optional[str] = attr.ib()
    last_seen_time: Optional[datetime.datetime] = attr.ib()

    booking_id: Optional[int] = attr.ib(default=None)
    holds: List['Hold'] = attr.ib(factory=list)
    arrest: Optional['Arrest'] = attr.ib(default=None)
    charges: List['Charge'] = attr.ib(factory=list)


@attr.s
class Arrest(Entity, BuildableAttr):
    external_id: Optional[str] = attr.ib()
    date: Optional[datetime.date] = attr.ib()
    location: Optional[str] = attr.ib()
    agency: Optional[str] = attr.ib()
    officer_name: Optional[str] = attr.ib()
    officer_id: Optional[str] = attr.ib()

    arrest_id: Optional[int] = attr.ib(default=None)


@attr.s
class Charge(Entity, BuildableAttr):
    offense_date: Optional[datetime.date] = attr.ib()
    statute: Optional[str] = attr.ib()
    name: Optional[str] = attr.ib()
    attempted: Optional[bool] = attr.ib()
    degree: Optional[ChargeDegree] = attr.ib()
    degree_raw_text: Optional[str] = attr.ib()
    charge_class: Optional[ChargeClass] = attr.ib()
    class_raw_text: Optional[str] = attr.ib()
    level: Optional[str] = attr.ib()
    fee_dollars: Optional[int] = attr.ib()
    charging_entity: Optional[str] = attr.ib()
    status: Optional[ChargeStatus] = attr.ib()
    status_raw_text: Optional[str] = attr.ib()
    court_type: Optional[CourtType] = attr.ib()
    court_type_raw_text: Optional[str] = attr.ib()
    case_number: Optional[str] = attr.ib()
    next_court_date: Optional[datetime.date] = attr.ib()
    judge_name: Optional[str] = attr.ib()

    charge_id: Optional[int] = attr.ib(default=None)
    bond: Optional['Bond'] = attr.ib(default=None)
    sentence: Optional['Sentence'] = attr.ib(default=None)


@attr.s
class Hold(Entity, BuildableAttr):
    jurisdiction_name: Optional[str] = attr.ib()
    hold_status: Optional[HoldStatus] = attr.ib()
    hold_status_raw_text: Optional[str] = attr.ib()

    hold_id: Optional[int] = attr.ib(default=None)


@attr.s
class Bond(Entity, BuildableAttr):
    amount_dollars: Optional[int] = attr.ib()
    bond_type: Optional[BondType] = attr.ib()
    bond_type_raw_text: Optional[str] = attr.ib()
    status: Optional[BondStatus] = attr.ib()
    status_raw_text: Optional[str] = attr.ib()

    bond_id: Optional[int] = attr.ib(default=None)


@attr.s
class Sentence(Entity, BuildableAttr):
    date_imposed: Optional[datetime.date] = attr.ib()
    sentencing_region: Optional[str] = attr.ib()
    min_length_days: Optional[int] = attr.ib()
    max_length_days: Optional[int] = attr.ib()
    is_life: Optional[bool] = attr.ib()
    is_probation: Optional[bool] = attr.ib()
    is_suspended: Optional[bool] = attr.ib()
    fine_dollars: Optional[int] = attr.ib()
    parole_possible: Optional[bool] = attr.ib()
    post_release_supervision_length_days: Optional[int] = attr.ib()

    sentence_id: Optional[int] = attr.ib(default=None)

    # To avoid recursive references, store only 1 level of related_sentences
    # (ie. don't store related_sentences of these related_sentences).
    related_sentences: List['Sentence'] = attr.ib(factory=list)
