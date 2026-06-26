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
"""Domain types returned by IdentityServiceQuerier methods.

Two Identity forms are provided:

- ``Identity`` — the core record with external IDs and attributes. Loaded for
  every query path. Does not carry audit history.
- ``IdentityHistory`` — pairs an ``Identity`` with its ``merge_events`` and
  ``split_events`` audit trail.
"""
import datetime
import uuid

import attr

from recidiviz.common import attr_validators, demographics
from recidiviz.common.constants.identity import (
    AttributeType,
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    NameUse,
    PersonType,
    PhoneType,
    ProductApp,
    SourceType,
    SplitTrigger,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.utils.types import assert_type

# ---------------------------------------------------------------------------
# Value types — the leaf objects carried inside SourcedAttributeValue.
# ---------------------------------------------------------------------------


@attr.define(frozen=True, kw_only=True)
class CanonicalAttributeMixin:
    """Mixin for sourced attribute value types that participate in canonical
    selection when multiple sources disagree."""

    canonical: bool = attr.ib(validator=attr_validators.is_bool)
    """True if this row is the system's default when sources disagree."""

    canonical_locked: bool = attr.ib(validator=attr_validators.is_bool)
    """True if the canonical flag has been manually pinned and must not be
    changed by the reconciliation pass."""


@attr.define(frozen=True, kw_only=True)
class Name:
    """A single sourced name value."""

    surname: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Family / last name, if provided."""

    given_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """First / given name, if provided."""

    middle_names: list[str] = attr.ib(validator=[attr_validators.is_list_of(str)])
    """Ordered list of middle names (empty list when none provided)."""

    name_suffix: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Generational or honorific suffix (e.g. 'Jr.', 'III'), if provided."""

    use: NameUse | None = attr.ib(validator=attr_validators.is_opt(NameUse))
    """How this name is designated by the source (official, preferred, etc.)."""

    @classmethod
    def from_dict(cls, data: dict[str, str | list[str] | None]) -> "Name":
        """Builds a Name from its JSONB-stored dict form."""
        return cls(
            surname=assert_type(v, str) if (v := data["surname"]) is not None else None,
            given_name=(
                assert_type(v, str) if (v := data["given_name"]) is not None else None
            ),
            middle_names=list(assert_type(data["middle_names"], list)),
            name_suffix=(
                assert_type(v, str) if (v := data["name_suffix"]) is not None else None
            ),
            use=NameUse(assert_type(v, str))
            if (v := data["use"]) is not None
            else None,
        )


@attr.define(frozen=True, kw_only=True)
class DateOfBirth(CanonicalAttributeMixin):
    """A single sourced date-of-birth value."""

    date: datetime.date = attr.ib(validator=attr_validators.is_date)
    """The date of birth."""

    @classmethod
    def from_dict(cls, data: dict[str, str | bool]) -> "DateOfBirth":
        """Builds a DateOfBirth from its JSONB-stored dict form."""
        return cls(
            date=datetime.date.fromisoformat(assert_type(data["date"], str)),
            canonical=assert_type(data["canonical"], bool),
            canonical_locked=assert_type(data["canonical_locked"], bool),
        )


@attr.define(frozen=True, kw_only=True)
class Gender(CanonicalAttributeMixin):
    """A single sourced gender value."""

    gender: demographics.Gender = attr.ib(
        validator=attr.validators.instance_of(demographics.Gender)
    )
    """The gender value."""

    @classmethod
    def from_dict(cls, data: dict[str, str | bool]) -> "Gender":
        """Builds a Gender from its JSONB-stored dict form."""
        return cls(
            gender=demographics.Gender(assert_type(data["gender"], str)),
            canonical=assert_type(data["canonical"], bool),
            canonical_locked=assert_type(data["canonical_locked"], bool),
        )


@attr.define(frozen=True, kw_only=True)
class Race:
    """A single sourced race value."""

    race: demographics.Race = attr.ib(
        validator=attr.validators.instance_of(demographics.Race)
    )
    """The race value."""

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "Race":
        """Builds a Race from its JSONB-stored dict form."""
        return cls(race=demographics.Race(assert_type(data["race"], str)))


@attr.define(frozen=True, kw_only=True)
class Sex(CanonicalAttributeMixin):
    """A single sourced sex value."""

    sex: demographics.Sex = attr.ib(
        validator=attr.validators.instance_of(demographics.Sex)
    )
    """The sex value."""

    @classmethod
    def from_dict(cls, data: dict[str, str | bool]) -> "Sex":
        """Builds a Sex from its JSONB-stored dict form."""
        return cls(
            sex=demographics.Sex(assert_type(data["sex"], str)),
            canonical=assert_type(data["canonical"], bool),
            canonical_locked=assert_type(data["canonical_locked"], bool),
        )


@attr.define(frozen=True, kw_only=True)
class Ethnicity(CanonicalAttributeMixin):
    """A single sourced ethnicity value."""

    ethnicity: demographics.Ethnicity = attr.ib(
        validator=attr.validators.instance_of(demographics.Ethnicity)
    )
    """The ethnicity value."""

    @classmethod
    def from_dict(cls, data: dict[str, str | bool]) -> "Ethnicity":
        """Builds an Ethnicity from its JSONB-stored dict form."""
        return cls(
            ethnicity=demographics.Ethnicity(assert_type(data["ethnicity"], str)),
            canonical=assert_type(data["canonical"], bool),
            canonical_locked=assert_type(data["canonical_locked"], bool),
        )


@attr.define(frozen=True, kw_only=True)
class PhoneNumber:
    """A single sourced phone number value."""

    number: str = attr.ib(validator=attr_validators.is_str)
    """Phone number as the source provided it (no normalization applied)."""

    type: PhoneType | None = attr.ib(validator=attr_validators.is_opt(PhoneType))
    """Category of phone (cell/home/work/other), if provided."""

    preferred: bool | None = attr.ib(validator=attr_validators.is_opt(bool))
    """True if this is the person's preferred phone, None if not specified."""

    @classmethod
    def from_dict(cls, data: dict[str, str | bool | None]) -> "PhoneNumber":
        """Builds a PhoneNumber from its JSONB-stored dict form."""
        return cls(
            number=assert_type(data["number"], str),
            type=(
                PhoneType(assert_type(v, str))
                if (v := data["type"]) is not None
                else None
            ),
            preferred=(
                assert_type(v, bool) if (v := data["preferred"]) is not None else None
            ),
        )


@attr.define(frozen=True, kw_only=True)
class Email:
    """A single sourced email address value."""

    address: str = attr.ib(validator=attr_validators.is_str)
    """Email address as the source provided it (case preserved)."""

    address_hash: str = attr.ib(validator=attr_validators.is_str)
    """Normalized hash of the email address used for the application-layer
    uniqueness check."""

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "Email":
        """Builds an Email from its JSONB-stored dict form."""
        return cls(
            address=assert_type(data["address"], str),
            address_hash=assert_type(data["address_hash"], str),
        )


# Union of all attribute value types. Use this annotation instead of `Any`
# when a variable or field holds one of the leaf value objects above.
AttributeValue = (
    Name | DateOfBirth | Gender | Race | Sex | Ethnicity | PhoneNumber | Email
)

# Tuple form of the same union for use in validators (isinstance accepts tuples).
_ATTRIBUTE_VALUE_TYPES = (
    Name,
    DateOfBirth,
    Gender,
    Race,
    Sex,
    Ethnicity,
    PhoneNumber,
    Email,
)


def _attribute_value_from_dict(
    attribute_type: AttributeType, value_data: dict
) -> AttributeValue:
    """Decodes the JSONB-stored `value` dict into the leaf type named by
    `attribute_type`."""
    if attribute_type is AttributeType.NAME:
        return Name.from_dict(value_data)
    if attribute_type is AttributeType.DATE_OF_BIRTH:
        return DateOfBirth.from_dict(value_data)
    if attribute_type is AttributeType.GENDER:
        return Gender.from_dict(value_data)
    if attribute_type is AttributeType.RACE:
        return Race.from_dict(value_data)
    if attribute_type is AttributeType.SEX:
        return Sex.from_dict(value_data)
    if attribute_type is AttributeType.ETHNICITY:
        return Ethnicity.from_dict(value_data)
    if attribute_type is AttributeType.PHONE_NUMBER:
        return PhoneNumber.from_dict(value_data)
    if attribute_type is AttributeType.EMAIL:
        return Email.from_dict(value_data)
    raise ValueError(f"Unexpected attribute type [{attribute_type}]")


@attr.define(frozen=True, kw_only=True)
class SourcedAttributeValue:
    """An attribute value together with its provenance metadata.

    ``value`` holds one of the concrete leaf types (``Name``, ``Email``, etc.).
    The concrete type determines which sourced schema the API layer uses when
    serializing this object.
    """

    value: AttributeValue = attr.ib(
        validator=attr.validators.instance_of(_ATTRIBUTE_VALUE_TYPES)
    )
    """The typed attribute value."""

    source_type: SourceType = attr.ib(validator=attr.validators.instance_of(SourceType))
    """Where this value came from (external system, product app, admin override)."""

    source_product_app: ProductApp | None = attr.ib(
        validator=attr_validators.is_opt(ProductApp)
    )
    """When ``source_type`` is ``PRODUCT_APP``, the app that set the value.
    None for all other source types."""

    last_updated_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When this attribute value was last modified."""

    @classmethod
    def from_dict(
        cls, data: dict, *, attribute_type: AttributeType
    ) -> "SourcedAttributeValue":
        """Builds a SourcedAttributeValue from its JSONB-stored dict form. The
        wrapped value is polymorphic, so `attribute_type` (carried on the audit
        row, not the JSONB) selects which leaf type to decode."""
        return cls(
            value=_attribute_value_from_dict(
                attribute_type, assert_type(data["value"], dict)
            ),
            source_type=SourceType(assert_type(data["source_type"], str)),
            source_product_app=(
                ProductApp(assert_type(v, str))
                if (v := data["source_product_app"]) is not None
                else None
            ),
            last_updated_utc=datetime.datetime.fromisoformat(
                assert_type(data["last_updated_utc"], str)
            ),
        )


# ---------------------------------------------------------------------------
# External ID
# ---------------------------------------------------------------------------


@attr.define(frozen=True, kw_only=True)
class ExternalId:
    """An external system identifier attached to an identity."""

    external_id: str = attr.ib(validator=attr_validators.is_str)
    """The identifier as it appears in the source system."""

    id_type: IdentifierType = attr.ib(
        validator=attr.validators.instance_of(IdentifierType)
    )
    """Which kind of external identifier this is."""

    is_active: bool = attr.ib(validator=attr_validators.is_bool)
    """False for external IDs left behind by splits (kept for audit history
    but no longer associated with the identity for lookups)."""


# ---------------------------------------------------------------------------
# Attributes container
# ---------------------------------------------------------------------------


@attr.define(frozen=True, kw_only=True)
class IdentityAttributes:
    """All sourced attribute values belonging to an identity.

    Each field is a list of ``SourcedAttributeValue`` objects whose ``value``
    is of the corresponding leaf type.
    """

    names: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced names for this identity."""

    dates_of_birth: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced dates of birth. At most one should have ``canonical=True``."""

    genders: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced gender values."""

    races: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced race values."""

    sexes: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced sex values."""

    ethnicities: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced ethnicity values."""

    phone_numbers: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced phone numbers."""

    emails: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """All sourced email addresses."""


# ---------------------------------------------------------------------------
# Audit types (merge / split events)
# ---------------------------------------------------------------------------


@attr.define(frozen=True, kw_only=True)
class AttributeConflict:
    """Snapshot of a same-source attribute conflict resolved during a merge."""

    attribute_type: AttributeType = attr.ib(
        validator=attr.validators.instance_of(AttributeType)
    )
    """Which kind of attribute the conflict involves."""

    retired_value: SourcedAttributeValue = attr.ib(
        validator=attr.validators.instance_of(SourcedAttributeValue)
    )
    """The attribute value from the identity that was retired by the merge."""

    surviving_value: SourcedAttributeValue = attr.ib(
        validator=attr.validators.instance_of(SourcedAttributeValue)
    )
    """The attribute value from the identity that survived the merge."""


@attr.define(frozen=True, kw_only=True)
class MergeEvent:
    """Audit record of an identity merge."""

    surviving_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))
    """The recidiviz_id of the identity that remained ACTIVE after the merge."""

    retired_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))
    """The recidiviz_id of the identity that was retired (absorbed) by this merge."""

    trigger: MergeTrigger = attr.ib(validator=attr.validators.instance_of(MergeTrigger))
    """What initiated the merge (import pipeline or explicit API call)."""

    requested_by: str | None = attr.ib(validator=attr_validators.is_opt_valid_email)
    """For MERGE_ENDPOINT triggers, the email of the user who called the endpoint.
    None for IMPORT triggers."""

    timestamp_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When the merge was performed."""

    conflicts: list[AttributeConflict] = attr.ib(
        validator=[attr_validators.is_list_of(AttributeConflict)]
    )
    """Attribute conflicts that were resolved when the two identities merged."""


@attr.define(frozen=True, kw_only=True)
class SplitDestination:
    """One of the identities produced when an identity is split."""

    new_recidiviz_id: uuid.UUID = attr.ib(
        validator=attr.validators.instance_of(uuid.UUID)
    )
    """The recidiviz_id assigned to the newly-created identity."""

    external_ids: list[ExternalId] = attr.ib(
        validator=[attr_validators.is_list_of(ExternalId)]
    )
    """External IDs moved to this new identity during the split."""

    attributes: list[SourcedAttributeValue] = attr.ib(
        validator=[attr_validators.is_list_of(SourcedAttributeValue)]
    )
    """Attribute values moved to this new identity during the split."""


@attr.define(frozen=True, kw_only=True)
class SplitEvent:
    """Audit record of an identity split."""

    original_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))
    """The recidiviz_id of the identity that was split (remains ACTIVE)."""

    trigger: SplitTrigger = attr.ib(validator=attr.validators.instance_of(SplitTrigger))
    """What initiated the split (import pipeline or explicit API call)."""

    requested_by: str | None = attr.ib(validator=attr_validators.is_opt_valid_email)
    """For SPLIT_ENDPOINT triggers, the email of the user who called the endpoint.
    None for IMPORT triggers."""

    timestamp_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When the split was performed."""

    destinations: list[SplitDestination] = attr.ib(
        validator=[attr_validators.is_list_of(SplitDestination)]
    )
    """The new identities produced by this split, one entry per new identity."""


# ---------------------------------------------------------------------------
# Identity record types
# ---------------------------------------------------------------------------


@attr.define(frozen=True, kw_only=True)
class Identity:
    """Recidiviz-assigned identity record.

    Carries the core fields plus external IDs and attributes. Does not carry
    merge/split history.
    """

    recidiviz_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))
    """Immutable Recidiviz-assigned id for this person."""

    created_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When the Identity record was first created."""

    last_updated_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When any field on this Identity or its child attributes was last modified."""

    tenant: Tenant = attr.ib(validator=attr.validators.instance_of(Tenant))
    """The jurisdiction or organization through which the person entered the
     system."""

    person_type: PersonType = attr.ib(validator=attr.validators.instance_of(PersonType))
    """Category of person this identity represents."""

    status: IdentityStatus = attr.ib(
        validator=attr.validators.instance_of(IdentityStatus)
    )
    """Lifecycle status: ACTIVE for active records, RETIRED after a merge."""

    merged_into: uuid.UUID | None = attr.ib(validator=attr_validators.is_opt(uuid.UUID))
    """The surviving Identity that absorbed this one. None for ACTIVE identities."""

    last_cluster_hash: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Hash of the cluster's external IDs and attributes as of the last successful
    import run."""

    skip_demographic_guard: bool = attr.ib(validator=attr_validators.is_bool)
    """When TRUE, the next per-cluster update pass skips the demographic guard for this
    Identity. Set on the original identity after an auto-split so the update pass can
    correct attributes without the guard blocking. Clear back to FALSE immediately
    after the successful update pass."""

    external_ids: list[ExternalId] = attr.ib(
        validator=[attr_validators.is_list_of(ExternalId)]
    )
    """External system identifiers attached to this identity."""

    attributes: IdentityAttributes = attr.ib(
        validator=attr.validators.instance_of(IdentityAttributes)
    )
    """All sourced demographic and contact attributes for this identity."""

    def __attrs_post_init__(self) -> None:
        # Validate that `status` and `merged_into` fields are consistent.
        if self.status is IdentityStatus.RETIRED and self.merged_into is None:
            raise ValueError(
                f"Identity [{self.recidiviz_id}] has status RETIRED but no merged_into"
            )
        if self.merged_into is not None and self.status is not IdentityStatus.RETIRED:
            raise ValueError(
                f"Identity [{self.recidiviz_id}] has merged_into=[{self.merged_into}] "
                f"but status [{self.status}] is not RETIRED"
            )


@attr.define(frozen=True, kw_only=True)
class IdentityHistory:
    """An identity paired with its full audit history."""

    identity: Identity = attr.ib(validator=attr.validators.instance_of(Identity))
    """The identity record."""

    merge_events: list[MergeEvent] = attr.ib(
        validator=[attr_validators.is_list_of(MergeEvent)]
    )
    """Merge events where this identity was the surviving record."""

    split_events: list[SplitEvent] = attr.ib(
        validator=[attr_validators.is_list_of(SplitEvent)]
    )
    """Split events originating from this identity."""


# TODO(OBT-35306): Add domain types for CreateCandidate, UpdateAttributeCandidate,
# MergeCandidate, MergeCandidateIdentity, SplitCandidate, and NoMerge when
# implementing the split/merge candidate review APIs.
