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
# ============================================================================
"""Domain logic entities used in the persistence layer.

Note: These classes mirror the SQL Alchemy ORM objects but are kept separate.
This allows these persistence layer objects additional flexibility that the SQL
Alchemy ORM objects can't provide.
"""
import datetime
from typing import List, Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.county.bond import BondStatus, BondType
from recidiviz.common.constants.county.booking import (
    AdmissionReason,
    Classification,
    CustodyStatus,
    ReleaseReason,
)
from recidiviz.common.constants.county.charge import (
    ChargeClass,
    ChargeDegree,
    ChargeStatus,
)
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.constants.county.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.persistence.entity.base_entity import ExternalIdEntity


@attr.s
class Person(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a Person moving through the criminal justice system."""

    full_name: Optional[str] = attr.ib()
    birthdate: Optional[datetime.date] = attr.ib()
    birthdate_inferred_from_age: Optional[bool] = attr.ib()
    gender: Optional[Gender] = attr.ib()
    gender_raw_text: Optional[str] = attr.ib()
    race: Optional[Race] = attr.ib()
    race_raw_text: Optional[str] = attr.ib()
    region: str = attr.ib()  # non-nullable
    ethnicity: Optional[Ethnicity] = attr.ib()
    ethnicity_raw_text: Optional[str] = attr.ib()
    jurisdiction_id: str = attr.ib()  # non-nullable
    residency_status: Optional[ResidencyStatus] = attr.ib()
    resident_of_region: Optional[bool] = attr.ib()

    person_id: Optional[int] = attr.ib(default=None)
    bookings: List["Booking"] = attr.ib(factory=list)


@attr.s
class Booking(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a particular Booking into jail for a Person."""

    admission_date: Optional[datetime.date] = attr.ib()
    admission_reason: Optional[AdmissionReason] = attr.ib()
    admission_reason_raw_text: Optional[str] = attr.ib()
    admission_date_inferred: Optional[bool] = attr.ib()
    release_date: Optional[datetime.date] = attr.ib()
    release_date_inferred: Optional[bool] = attr.ib()
    projected_release_date: Optional[datetime.date] = attr.ib()
    release_reason: Optional[ReleaseReason] = attr.ib()
    release_reason_raw_text: Optional[str] = attr.ib()
    custody_status: CustodyStatus = attr.ib()  # non-nullable
    custody_status_raw_text: Optional[str] = attr.ib()
    facility: Optional[str] = attr.ib()
    facility_id: Optional[str] = attr.ib()
    classification: Optional[Classification] = attr.ib()
    classification_raw_text: Optional[str] = attr.ib()
    last_seen_time: datetime.datetime = attr.ib()  # non-nullable
    first_seen_time: datetime.datetime = attr.ib()  # non-nullable

    booking_id: Optional[int] = attr.ib(default=None)
    holds: List["Hold"] = attr.ib(factory=list)
    arrest: Optional["Arrest"] = attr.ib(default=None)
    charges: List["Charge"] = attr.ib(factory=list)


@attr.s
class Arrest(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models the Arrest for a particular Booking."""

    arrest_date: Optional[datetime.date] = attr.ib()
    location: Optional[str] = attr.ib()
    agency: Optional[str] = attr.ib()
    officer_name: Optional[str] = attr.ib()
    officer_id: Optional[str] = attr.ib()

    arrest_id: Optional[int] = attr.ib(default=None)


@attr.s
class Charge(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a Charge on a particular Booking."""

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
    status: ChargeStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()
    court_type: Optional[str] = attr.ib()
    case_number: Optional[str] = attr.ib()
    next_court_date: Optional[datetime.date] = attr.ib()
    judge_name: Optional[str] = attr.ib()
    charge_notes: Optional[str] = attr.ib()

    charge_id: Optional[int] = attr.ib(default=None)
    bond: Optional["Bond"] = attr.ib(default=None)
    sentence: Optional["Sentence"] = attr.ib(default=None)


@attr.s
class Hold(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a jurisdictional Hold on a particular Booking."""

    jurisdiction_name: Optional[str] = attr.ib()
    status: HoldStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    hold_id: Optional[int] = attr.ib(default=None)


@attr.s
class Bond(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a Bond on a particular Charge."""

    amount_dollars: Optional[int] = attr.ib()
    bond_type: Optional[BondType] = attr.ib()
    bond_type_raw_text: Optional[str] = attr.ib()
    status: BondStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()
    bond_agent: Optional[str] = attr.ib()

    bond_id: Optional[int] = attr.ib(default=None)
    booking_id: Optional[int] = attr.ib(default=None)


@attr.s
class Sentence(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a Sentence for one or more Charges on a particular Booking."""

    sentencing_region: Optional[str] = attr.ib()
    status: SentenceStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()
    min_length_days: Optional[int] = attr.ib()
    max_length_days: Optional[int] = attr.ib()
    date_imposed: Optional[datetime.date] = attr.ib()
    completion_date: Optional[datetime.date] = attr.ib()
    projected_completion_date: Optional[datetime.date] = attr.ib()
    is_life: Optional[bool] = attr.ib()
    is_probation: Optional[bool] = attr.ib()
    is_suspended: Optional[bool] = attr.ib()
    fine_dollars: Optional[int] = attr.ib()
    parole_possible: Optional[bool] = attr.ib()
    post_release_supervision_length_days: Optional[int] = attr.ib()

    sentence_id: Optional[int] = attr.ib(default=None)
    booking_id: Optional[int] = attr.ib(default=None)

    # To avoid recursive references, store only 1 level of related_sentences
    # (ie. don't store related_sentences of these related_sentences).
    related_sentences: List["Sentence"] = attr.ib(factory=list)
