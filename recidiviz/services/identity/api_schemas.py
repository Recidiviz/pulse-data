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
"""Marshmallow response schemas for the Identity Service API."""
from typing import Any

import attr
import marshmallow
from marshmallow import fields

from recidiviz.common import demographics
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
from recidiviz.services.identity import types
from recidiviz.utils.api_schemas import CamelCaseSchema

# Internal reconciliation bookkeeping stripped from the demographic attributes in the
# default serialized form. Dotted paths reach into the nested `value` schema.
_CANONICAL_FLAGS = ("value.canonical", "value.canonical_locked")


class NameSchema(CamelCaseSchema):
    surname = fields.Str(allow_none=True)
    given_name = fields.Str(allow_none=True)
    middle_names = fields.List(fields.Str())
    name_suffix = fields.Str(allow_none=True)
    use = fields.Enum(NameUse, allow_none=True)


class CanonicalAttributeSchemaMixin:
    """Shared canonical-selection fields for the value schemas whose values can
    be designated the system default when sources disagree."""

    canonical = fields.Bool()
    canonical_locked = fields.Bool()


class DateOfBirthSchema(CamelCaseSchema, CanonicalAttributeSchemaMixin):
    date = fields.Date()


class GenderSchema(CamelCaseSchema, CanonicalAttributeSchemaMixin):
    gender = fields.Enum(demographics.Gender)


class RaceSchema(CamelCaseSchema):
    race = fields.Enum(demographics.Race)


class SexSchema(CamelCaseSchema, CanonicalAttributeSchemaMixin):
    sex = fields.Enum(demographics.Sex)


class EthnicitySchema(CamelCaseSchema, CanonicalAttributeSchemaMixin):
    ethnicity = fields.Enum(demographics.Ethnicity)


class PhoneNumberSchema(CamelCaseSchema):
    number = fields.Str()
    type = fields.Enum(PhoneType, allow_none=True)
    preferred = fields.Bool(allow_none=True)


class EmailSchema(CamelCaseSchema):
    address = fields.Str()


class SourcedAttributeValueSchema(CamelCaseSchema):
    """Provenance metadata shared by every sourced value. Typed subclasses add `value`."""

    source_type = fields.Enum(SourceType)
    source_product_app = fields.Enum(ProductApp, allow_none=True)
    last_updated_utc = fields.DateTime()


class SourcedNameSchema(SourcedAttributeValueSchema):
    value = fields.Nested(NameSchema())


class SourcedDateOfBirthSchema(SourcedAttributeValueSchema):
    value = fields.Nested(DateOfBirthSchema())


class SourcedGenderSchema(SourcedAttributeValueSchema):
    value = fields.Nested(GenderSchema())


class SourcedRaceSchema(SourcedAttributeValueSchema):
    value = fields.Nested(RaceSchema())


class SourcedSexSchema(SourcedAttributeValueSchema):
    value = fields.Nested(SexSchema())


class SourcedEthnicitySchema(SourcedAttributeValueSchema):
    value = fields.Nested(EthnicitySchema())


class SourcedPhoneNumberSchema(SourcedAttributeValueSchema):
    value = fields.Nested(PhoneNumberSchema())


class SourcedEmailSchema(SourcedAttributeValueSchema):
    value = fields.Nested(EmailSchema())


class ExternalIdSchema(CamelCaseSchema):
    """Serialized external identifier. The default identity form excludes `is_active`."""

    external_id = fields.Str()
    id_type = fields.Enum(IdentifierType)
    is_active = fields.Bool()


class IdentityAttributesSchema(CamelCaseSchema):
    """Default-form sourced attributes: a single canonical `date_of_birth` and canonical
    bookkeeping flags stripped from the demographic attributes."""

    names = fields.List(fields.Nested(SourcedNameSchema()))
    date_of_birth = fields.Method("_serialize_canonical_date_of_birth")
    genders = fields.List(fields.Nested(SourcedGenderSchema(exclude=_CANONICAL_FLAGS)))
    races = fields.List(fields.Nested(SourcedRaceSchema()))
    sexes = fields.List(fields.Nested(SourcedSexSchema(exclude=_CANONICAL_FLAGS)))
    ethnicities = fields.List(
        fields.Nested(SourcedEthnicitySchema(exclude=_CANONICAL_FLAGS))
    )
    phone_numbers = fields.List(fields.Nested(SourcedPhoneNumberSchema()))
    emails = fields.List(fields.Nested(SourcedEmailSchema()))

    def _serialize_canonical_date_of_birth(
        self, attributes: types.IdentityAttributes
    ) -> dict | None:
        for dob in attributes.dates_of_birth:
            if isinstance(dob.value, types.DateOfBirth) and dob.value.canonical:
                return SourcedDateOfBirthSchema(exclude=_CANONICAL_FLAGS).dump(dob)
        return None


class IdentityFullAttributesSchema(IdentityAttributesSchema):
    """`?full=true` attributes: the complete sourced lists with canonical flags retained."""

    class Meta:
        exclude = ("date_of_birth",)

    dates_of_birth = fields.List(fields.Nested(SourcedDateOfBirthSchema()))
    genders = fields.List(fields.Nested(SourcedGenderSchema()))
    sexes = fields.List(fields.Nested(SourcedSexSchema()))
    ethnicities = fields.List(fields.Nested(SourcedEthnicitySchema()))


# Maps a value type to the sourced schema that renders it. Used to serialize the
# heterogeneous SourcedAttributeValues carried by audit events (full form only).
_SOURCED_SCHEMA_BY_VALUE_TYPE: dict[type, type[CamelCaseSchema]] = {
    types.Name: SourcedNameSchema,
    types.DateOfBirth: SourcedDateOfBirthSchema,
    types.Gender: SourcedGenderSchema,
    types.Race: SourcedRaceSchema,
    types.Sex: SourcedSexSchema,
    types.Ethnicity: SourcedEthnicitySchema,
    types.PhoneNumber: SourcedPhoneNumberSchema,
    types.Email: SourcedEmailSchema,
}


def _dump_sourced_attribute_value(sourced_value: types.SourcedAttributeValue) -> dict:
    schema_class = _SOURCED_SCHEMA_BY_VALUE_TYPE[type(sourced_value.value)]
    return schema_class().dump(sourced_value)


class MergeEventSchema(CamelCaseSchema):
    """Audit record of a merge (full form only)."""

    class AttributeConflictSchema(CamelCaseSchema):
        attribute_type = fields.Enum(AttributeType)
        retired_value = fields.Method("_serialize_retired_value")
        surviving_value = fields.Method("_serialize_surviving_value")

        def _serialize_retired_value(self, conflict: types.AttributeConflict) -> dict:
            return _dump_sourced_attribute_value(conflict.retired_value)

        def _serialize_surviving_value(self, conflict: types.AttributeConflict) -> dict:
            return _dump_sourced_attribute_value(conflict.surviving_value)

    surviving_id = fields.UUID()
    retired_id = fields.UUID()
    trigger = fields.Enum(MergeTrigger)
    requested_by = fields.Str(allow_none=True)
    timestamp_utc = fields.DateTime()
    conflicts = fields.List(fields.Nested(AttributeConflictSchema()))


class SplitEventSchema(CamelCaseSchema):
    """Audit record of a split (full form only)."""

    class SplitDestinationSchema(CamelCaseSchema):
        new_recidiviz_id = fields.UUID()
        external_ids = fields.List(fields.Nested(ExternalIdSchema()))
        attributes = fields.Method("_serialize_attributes")

        def _serialize_attributes(
            self, destination: types.SplitDestination
        ) -> list[dict]:
            return [
                _dump_sourced_attribute_value(value) for value in destination.attributes
            ]

    original_id = fields.UUID()
    trigger = fields.Enum(SplitTrigger)
    requested_by = fields.Str(allow_none=True)
    timestamp_utc = fields.DateTime()
    destinations = fields.List(fields.Nested(SplitDestinationSchema()))


class IdentitySchema(CamelCaseSchema):
    """Default serialized form of an identity: internals and audit history omitted."""

    recidiviz_id = fields.UUID(required=True)
    created_utc = fields.DateTime(required=True)
    last_updated_utc = fields.DateTime(required=True)
    tenant = fields.Enum(Tenant, required=True)
    person_type = fields.Enum(PersonType, required=True)
    status = fields.Enum(IdentityStatus, required=True)
    merged_into = fields.UUID(allow_none=True)
    external_ids = fields.List(fields.Nested(ExternalIdSchema(exclude=("is_active",))))
    attributes = fields.Nested(IdentityAttributesSchema())


class IdentityFullSchema(IdentitySchema):
    """Full serialized form of an Identity: the same input as IdentitySchema, plus the
    internal reconciliation fields and the complete attribute lists with canonical flags
    retained. The merge/split audit history is added by IdentityHistorySchema."""

    last_cluster_hash = fields.Str(allow_none=True)
    skip_demographic_guard = fields.Bool()
    external_ids = fields.List(fields.Nested(ExternalIdSchema()))
    attributes = fields.Nested(IdentityFullAttributesSchema())


class IdentityHistorySchema(IdentityFullSchema):
    """`?full=true` serialized form: an IdentityFullSchema plus its merge/split audit
    history. Consumes an IdentityHistory (use dump_identity_history)."""

    merge_events = fields.List(fields.Nested(MergeEventSchema()))
    split_events = fields.List(fields.Nested(SplitEventSchema()))

    @marshmallow.pre_dump
    def flatten_history(self, data: types.IdentityHistory, **_kwargs: Any) -> dict:
        """Flattens an IdentityHistory into a dict the inherited Identity fields can
        consume."""
        result = attr.asdict(data.identity, recurse=False)
        result["merge_events"] = data.merge_events
        result["split_events"] = data.split_events
        return result


class MergeResponseSchema(CamelCaseSchema):
    """Response body for POST /identities/merge."""

    surviving_identity = fields.Nested(IdentitySchema())


class SplitResponseSchema(CamelCaseSchema):
    """Response body for POST /identity/{recidiviz_id}/split."""

    original_identity = fields.Nested(IdentitySchema())
    new_identity = fields.Nested(IdentitySchema())


class SearchResponseSchema(CamelCaseSchema):
    """Response body for POST /identities/search."""

    results = fields.List(fields.Nested(IdentitySchema()))
    next_cursor = fields.Str(allow_none=True)


class IdentityByUuidRequestSchema(marshmallow.Schema):
    """Validates query parameters for GET /identity/<recidiviz_id>."""

    full = fields.Bool(load_default=False)


class IdentityByQueryParametersRequestSchema(marshmallow.Schema):
    """Validates query parameters for GET /identity.

    Exactly one of two lookup modes must be provided:
    - external_id + id_type
    - tenant + email_hash
    """

    # Mode 1: external ID lookup
    external_id = fields.Str(load_default=None)
    id_type = fields.Enum(IdentifierType, by_value=True, load_default=None)
    # Mode 2: email hash lookup
    tenant = fields.Enum(Tenant, by_value=True, load_default=None)
    email_hash = fields.Str(load_default=None)
    # Shared
    full = fields.Bool(load_default=False)

    @marshmallow.validates_schema
    def validate_lookup_mode(self, data: dict, **_kwargs: Any) -> None:
        has_external_id = data["external_id"] is not None
        has_id_type = data["id_type"] is not None
        has_tenant = data["tenant"] is not None
        has_email_hash = data["email_hash"] is not None

        external_id_mode = has_external_id or has_id_type
        email_hash_mode = has_tenant or has_email_hash

        if external_id_mode and email_hash_mode:
            raise marshmallow.ValidationError(
                "Provide either external_id+id_type or tenant+email_hash, not both."
            )
        if external_id_mode:
            if not (has_external_id and has_id_type):
                raise marshmallow.ValidationError(
                    "Both external_id and id_type are required."
                )
        elif email_hash_mode:
            if not (has_tenant and has_email_hash):
                raise marshmallow.ValidationError(
                    "Both tenant and email_hash are required."
                )
        else:
            raise marshmallow.ValidationError(
                "Provide either external_id+id_type or tenant+email_hash."
            )
