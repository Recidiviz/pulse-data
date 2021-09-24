# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Fake schema for use in ingest_view_file_parser_test.py."""
import datetime
from enum import Enum
from typing import List, Optional

import attr

from recidiviz.common import attr_validators
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
)


class FakeGender(Enum):
    FEMALE = "FEMALE_ENUM_VALUE"
    MALE = "MALE_ENUM_VALUE"
    TRANS_FEMALE = "TRANS_FEMALE_ENUM_VALUE"
    TRANS_MALE = "TRANS_MALE_ENUM_VALUE"


class FakeRace(Enum):
    ASIAN = "ASIAN_VALUE"
    BLACK = "BLACK_VALUE"
    WHITE = "WHITE_VALUE"


@attr.s(eq=False)
class FakePerson(Entity):
    """Fake person entity for ingest parser tests."""

    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    name: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    birthdate: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    gender: Optional[FakeGender] = attr.ib(
        default=None, validator=attr_validators.is_opt(FakeGender)
    )
    gender_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    current_address: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    ssn: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Fake primary key field
    fake_person_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    external_ids: List["FakePersonExternalId"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    aliases: List["FakePersonAlias"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    races: List["FakePersonRace"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    current_officer: Optional["FakeAgent"] = attr.ib(default=None)


@attr.s(eq=False)
class FakePersonAlias(Entity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Fake primary key field
    fake_person_alias_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Back edge relationship
    person: Optional["FakePerson"] = attr.ib(default=None)


@attr.s(eq=False)
class FakePersonExternalId(Entity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    external_id: str = attr.ib(validator=attr_validators.is_str)
    id_type: str = attr.ib(validator=attr_validators.is_str)

    # Fake primary key field
    fake_person_external_id_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Back edge relationship
    person: Optional["FakePerson"] = attr.ib(default=None)


@attr.s(eq=False)
class FakePersonRace(EnumEntity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    race: Optional[FakeRace] = attr.ib(
        default=None, validator=attr_validators.is_opt(FakeRace)
    )
    race_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Fake primary key field
    fake_person_race_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Back edge relationship
    person: Optional["FakePerson"] = attr.ib(default=None)


@attr.s(eq=False)
class FakeAgent(ExternalIdEntity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    name: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Fake primary key field
    fake_agent_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
