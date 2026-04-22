# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Pipeline-internal entity classes for the batch identity clustering pipeline.

These are lightweight representations used within the pipeline and are distinct
from both the Identity Service's domain objects and the ingest pipeline's
StatePerson/StateStaff entities. The structure mirrors StatePerson/StatePersonRace
so that ingest view mapping YAMLs and IngestViewManifestCompiler can be reused.
"""
from __future__ import annotations

import datetime

import attr

from recidiviz.common.attr_validators import (
    is_list_of,
    is_opt,
    is_opt_str,
    is_str,
    is_valid_email,
    is_valid_phone_number,
)
from recidiviz.common.demographics import Gender, Race
from recidiviz.persistence.entity.base_entity import (
    Entity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.state.reasonable_date_validators import (
    REASONABLE_OPT_BIRTHDATE_VALIDATOR,
)


@attr.s(eq=False, kw_only=True)
class IdentityFragmentExternalId(ExternalIdEntity):
    fragment: IdentityFragment | None = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityFragmentRace(Entity):
    race: Race = attr.ib(validator=attr.validators.instance_of(Race))
    race_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    fragment: IdentityFragment | None = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityFragmentPhoneNumber(Entity):
    number: str = attr.ib(validator=is_valid_phone_number)
    fragment: IdentityFragment | None = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityFragmentEmail(Entity):
    address: str = attr.ib(validator=is_valid_email)
    fragment: IdentityFragment | None = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityFragmentName(Entity):
    # TODO(#73389): Add validators that enforce that given_name, surname, and
    # middle_name fields do not contain digits.
    given_name: str | None = attr.ib(default=None, validator=is_opt_str)
    surname: str | None = attr.ib(default=None, validator=is_opt_str)
    middle_name: str | None = attr.ib(default=None, validator=is_opt_str)
    name_suffix: str | None = attr.ib(default=None, validator=is_opt_str)
    fragment: IdentityFragment | None = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityFragment(
    HasMultipleExternalIdsEntity[IdentityFragmentExternalId], RootEntity
):
    """One dataset's view of a person (a single row from a single data source)."""

    # TODO(#73568): Add a validator to ensure this is a valid tenant, or change to type Tenant
    tenant: str = attr.ib(validator=is_str)

    external_ids: list[IdentityFragmentExternalId] = attr.ib(
        validator=is_list_of(IdentityFragmentExternalId)
    )

    name: IdentityFragmentName | None = attr.ib(
        default=None, validator=is_opt(IdentityFragmentName)
    )

    birthdate: datetime.date | None = attr.ib(
        default=None, validator=REASONABLE_OPT_BIRTHDATE_VALIDATOR
    )

    gender: Gender | None = attr.ib(default=None, validator=is_opt(Gender))
    gender_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)

    races: list[IdentityFragmentRace] = attr.ib(
        factory=list, validator=is_list_of(IdentityFragmentRace)
    )

    phone_numbers: list[IdentityFragmentPhoneNumber] = attr.ib(
        factory=list, validator=is_list_of(IdentityFragmentPhoneNumber)
    )

    emails: list[IdentityFragmentEmail] = attr.ib(
        factory=list, validator=is_list_of(IdentityFragmentEmail)
    )

    def get_external_ids(self) -> list[IdentityFragmentExternalId]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "fragment"
