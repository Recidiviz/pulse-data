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
"""Entity classes for the batch identity clustering pipeline.

These are lightweight representations used within the pipeline and are distinct
from both the Identity Service's domain objects and the activity ingest pipeline's
StatePerson/StateStaff entities. The structure mirrors StatePerson/StatePersonRace
so that ingest view mapping YAMLs and IngestViewManifestCompiler can be reused.
"""
import datetime

import attr

from recidiviz.common.attr_validators import (
    is_list_of,
    is_none,
    is_opt,
    is_opt_str,
    is_opt_valid_name_part,
    is_opt_valid_name_suffix,
    is_valid_email,
    is_valid_phone_number,
)
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.entity_field_index import (
    EntityFieldIndex,
    EntityFieldType,
)
from recidiviz.persistence.entity.identity.identity_entity_mixin import (
    IdentityEntityMixin,
)
from recidiviz.persistence.entity.reasonable_date_validators import (
    REASONABLE_OPT_BIRTHDATE_VALIDATOR,
)


@attr.s(eq=False, kw_only=True)
class IdentityExternalId(IdentityEntityMixin, ExternalIdEntity):
    fragment: "IdentityFragment | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityName(IdentityEntityMixin, Entity):
    given_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    preferred_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    surname: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    middle_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    name_suffix: str | None = attr.ib(default=None, validator=is_opt_valid_name_suffix)
    identity_attributes: "IdentityAttributes | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityGender(IdentityEntityMixin, EnumEntity):
    gender: Gender = attr.ib(validator=attr.validators.instance_of(Gender))
    gender_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_attributes: "IdentityAttributes | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentitySex(IdentityEntityMixin, EnumEntity):
    sex: Sex = attr.ib(validator=attr.validators.instance_of(Sex))
    sex_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_attributes: "IdentityAttributes | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityRace(IdentityEntityMixin, EnumEntity):
    race: Race = attr.ib(validator=attr.validators.instance_of(Race))
    race_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_attributes: "IdentityAttributes | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityEthnicity(IdentityEntityMixin, EnumEntity):
    ethnicity: Ethnicity = attr.ib(validator=attr.validators.instance_of(Ethnicity))
    ethnicity_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_attributes: "IdentityAttributes | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityPhoneNumber(IdentityEntityMixin, Entity):
    number: str = attr.ib(validator=is_valid_phone_number)
    identity_attributes: "IdentityAttributes | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityEmail(IdentityEntityMixin, Entity):
    address: str = attr.ib(validator=is_valid_email)
    identity_attributes: "IdentityAttributes | None" = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class IdentityAttributes(IdentityEntityMixin, Entity):
    """Identity attributes associated with one dataset's view of a person
    (as the attributes field on IdentityFragment) or with a cluster's chosen
    best-known attributes (as the attributes field on IdentityCluster)."""

    person_type: PersonType = attr.ib(validator=attr.validators.instance_of(PersonType))

    # Always None at runtime (enforced by is_none). Present because the manifest
    # compiler pairs every enum field with a _raw_text companion (see
    # EnumLiteralFieldManifest.additional_field_manifests). person_type comes
    # from $literal_enum in the YAML, which auto-injects None for the raw_text
    # counterpart. The type annotation stays `str | None` so the serialization
    # framework's attribute-type introspection keeps working.
    person_type_raw_text: str | None = attr.ib(default=None, validator=is_none)

    name: "IdentityName | None" = attr.ib(default=None, validator=is_opt(IdentityName))

    birthdate: datetime.date | None = attr.ib(
        default=None, validator=REASONABLE_OPT_BIRTHDATE_VALIDATOR
    )

    gender: "IdentityGender | None" = attr.ib(
        default=None, validator=is_opt(IdentityGender)
    )

    sex: "IdentitySex | None" = attr.ib(default=None, validator=is_opt(IdentitySex))

    races: list["IdentityRace"] = attr.ib(
        factory=list, validator=is_list_of(IdentityRace)
    )

    ethnicity: "IdentityEthnicity | None" = attr.ib(
        default=None, validator=is_opt(IdentityEthnicity)
    )

    phone_numbers: list["IdentityPhoneNumber"] = attr.ib(
        factory=list, validator=is_list_of(IdentityPhoneNumber)
    )

    emails: list["IdentityEmail"] = attr.ib(
        factory=list, validator=is_list_of(IdentityEmail)
    )

    fragment: "IdentityFragment | None" = attr.ib(default=None)

    ATTRIBUTE_FIELDS_TO_EXCLUDE_WHEN_CHECKING_FOR_AT_LEAST_ONE = frozenset(
        {"tenant", "person_type"}
    )

    def has_at_least_one_attribute(self, field_index: EntityFieldIndex) -> bool:
        """Returns True if at least one field beyond tenant/person_type is
        non-empty."""
        non_empty = field_index.get_fields_with_non_empty_values(
            self, EntityFieldType.FLAT_FIELD
        ) | field_index.get_fields_with_non_empty_values(
            self, EntityFieldType.FORWARD_EDGE
        )
        return bool(
            non_empty - self.ATTRIBUTE_FIELDS_TO_EXCLUDE_WHEN_CHECKING_FOR_AT_LEAST_ONE
        )

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "identity_attributes"


@attr.s(eq=False, kw_only=True)
class IdentityFragment(
    IdentityEntityMixin, HasMultipleExternalIdsEntity[IdentityExternalId], RootEntity
):
    """One dataset's view of a person (a single row from a single data source)."""

    external_ids: list["IdentityExternalId"] = attr.ib(
        validator=is_list_of(IdentityExternalId)
    )

    attributes: "IdentityAttributes" = attr.ib(
        validator=attr.validators.instance_of(IdentityAttributes),
    )

    def get_external_ids(self) -> list[IdentityExternalId]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "fragment"
