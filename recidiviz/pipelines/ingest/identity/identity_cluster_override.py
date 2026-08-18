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
"""A human-recorded override of the identity pipeline's automated decision for
one cluster, and the schema of the identity_cluster_override table its rows are
read from. The value object and the schema live together so they cannot drift
apart.

A reviewer records an override when the pipeline gets a specific cluster wrong.
A BLESS override says a cluster whose fragments conflict really is one person and
supplies the values its identity should carry, so the pipeline keeps the cluster
instead of rejecting it. A blessing applies only while the cluster's conflicts
still hash to the value recorded at review time, so a cluster whose conflict
evidence changes after the review is rejected again rather than kept on a stale
decision. An EXCLUDE override says a cluster is bad and drops it,
whether or not its fragments conflict, so it never reaches the kept-cluster
tables the Identity Service imports and never clutters the rejected-cluster
review queue.

The blessed values are per-person PII (a real name, a real birthdate), so this
table lives in BigQuery per tenant rather than in a checked-in config file.
"""
import datetime
from enum import Enum
from typing import TypeVar

import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.common.attr_validators import (
    is_opt_valid_name_part,
    is_opt_valid_name_suffix,
)
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Sex
from recidiviz.persistence.entity.reasonable_date_validators import (
    REASONABLE_OPT_BIRTHDATE_VALIDATOR,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
)
from recidiviz.pipelines.ingest.transforms.types import ClusterKey
from recidiviz.utils.types import assert_type

IDENTITY_CLUSTER_OVERRIDE_TABLE_ID = "identity_cluster_override"

# Top-level columns of the identity_cluster_override table.
_TENANT_COL = "tenant"
_PERSON_TYPE_COL = "person_type"
_EXTERNAL_IDS_COL = "external_ids"
_DISPOSITION_COL = "disposition"
_BLESSED_VALUES_COL = "blessed_values"
_CONFLICTS_HASH_COL = "conflicts_hash"
_RECORDED_BY_COL = "recorded_by"
_RECORDED_AT_COL = "recorded_at"
_NOTE_COL = "note"

# Members of the external_ids record column.
_EXTERNAL_ID_FIELD = "external_id"
_ID_TYPE_FIELD = "id_type"

# Members of the blessed_values record column. Each names the identity value a
# BLESS override supplies for the matching conflict-checked attribute, so the
# names track ConflictCheckedAttribute's values.
_GIVEN_NAME_FIELD = ConflictCheckedAttribute.GIVEN_NAME.value
_MIDDLE_NAME_FIELD = ConflictCheckedAttribute.MIDDLE_NAME.value
_SURNAME_FIELD = ConflictCheckedAttribute.SURNAME.value
_NAME_SUFFIX_FIELD = ConflictCheckedAttribute.NAME_SUFFIX.value
_BIRTHDATE_FIELD = ConflictCheckedAttribute.BIRTHDATE.value
_SEX_FIELD = ConflictCheckedAttribute.SEX.value
_GENDER_FIELD = ConflictCheckedAttribute.GENDER.value
_ETHNICITY_FIELD = ConflictCheckedAttribute.ETHNICITY.value


class IdentityClusterOverrideDisposition(Enum):
    """What a reviewer decided to do with a cluster the pipeline got wrong.
    BLESS keeps a conflicting cluster as one person, using the recorded values.
    EXCLUDE drops a cluster entirely, whether or not it conflicts.
    """

    BLESS = "BLESS"
    EXCLUDE = "EXCLUDE"


def identity_cluster_override_schema_fields() -> list[bigquery.SchemaField]:
    """Returns the BQ schema of the identity_cluster_override table.

    The nested record fields are NULLABLE rather than REQUIRED because the BQ
    emulator's DDL engine cannot create NOT NULL nested columns (it fails with
    "Nested column attributes are unsupported"); IdentityClusterOverride's
    validators enforce which values are present.
    """
    return [
        bigquery.SchemaField(
            _TENANT_COL,
            "STRING",
            mode="REQUIRED",
            description="Tenant the overridden cluster belongs to.",
        ),
        bigquery.SchemaField(
            _PERSON_TYPE_COL,
            "STRING",
            mode="REQUIRED",
            description="Person type of the overridden cluster.",
        ),
        bigquery.SchemaField(
            _EXTERNAL_IDS_COL,
            "RECORD",
            mode="REPEATED",
            description=(
                "The exact set of external ids identifying the cluster this "
                "override applies to. A cluster whose ids gain or lose a member "
                "no longer matches and is decided by the pipeline again."
            ),
            fields=(
                bigquery.SchemaField(
                    _EXTERNAL_ID_FIELD, "STRING", description="External id value."
                ),
                bigquery.SchemaField(
                    _ID_TYPE_FIELD, "STRING", description="External id type."
                ),
            ),
        ),
        bigquery.SchemaField(
            _DISPOSITION_COL,
            "STRING",
            mode="REQUIRED",
            description=(
                "The reviewer's decision: BLESS keeps the cluster with the "
                "blessed values; EXCLUDE drops it entirely."
            ),
        ),
        bigquery.SchemaField(
            _BLESSED_VALUES_COL,
            "RECORD",
            mode="NULLABLE",
            description=(
                "The identity values a BLESS override supplies, one per "
                "conflicting attribute; null for an EXCLUDE override."
            ),
            fields=(
                bigquery.SchemaField(
                    _GIVEN_NAME_FIELD,
                    "STRING",
                    description="Blessed given name, if the cluster conflicts on it.",
                ),
                bigquery.SchemaField(
                    _MIDDLE_NAME_FIELD,
                    "STRING",
                    description="Blessed middle name, if the cluster conflicts on it.",
                ),
                bigquery.SchemaField(
                    _SURNAME_FIELD,
                    "STRING",
                    description="Blessed surname, if the cluster conflicts on it.",
                ),
                bigquery.SchemaField(
                    _NAME_SUFFIX_FIELD,
                    "STRING",
                    description="Blessed name suffix, if the cluster conflicts on it.",
                ),
                bigquery.SchemaField(
                    _BIRTHDATE_FIELD,
                    "DATE",
                    description="Blessed birthdate, if the cluster conflicts on it.",
                ),
                bigquery.SchemaField(
                    _SEX_FIELD,
                    "STRING",
                    description="Blessed sex, if the cluster conflicts on it.",
                ),
                bigquery.SchemaField(
                    _GENDER_FIELD,
                    "STRING",
                    description="Blessed gender, if the cluster conflicts on it.",
                ),
                bigquery.SchemaField(
                    _ETHNICITY_FIELD,
                    "STRING",
                    description="Blessed ethnicity, if the cluster conflicts on it.",
                ),
            ),
        ),
        bigquery.SchemaField(
            _CONFLICTS_HASH_COL,
            "STRING",
            mode="NULLABLE",
            description=(
                "Hash of the conflicts the reviewer saw, copied from the "
                "rejected cluster's row at recording time; null for an EXCLUDE "
                "override. A BLESS applies only while the cluster's current "
                "conflicts still hash to this value, so a cluster whose "
                "conflict evidence changes after the review is rejected again "
                "rather than kept on a stale blessing."
            ),
        ),
        bigquery.SchemaField(
            _RECORDED_BY_COL,
            "STRING",
            mode="REQUIRED",
            description="The reviewer who recorded the override.",
        ),
        bigquery.SchemaField(
            _RECORDED_AT_COL,
            "TIMESTAMP",
            mode="REQUIRED",
            description="When the override was recorded.",
        ),
        bigquery.SchemaField(
            _NOTE_COL,
            "STRING",
            mode="REQUIRED",
            description="The reviewer's justification for the override.",
        ),
    ]


@attr.define(frozen=True, kw_only=True)
class BlessedIdentityValues:
    """The identity values a BLESS override supplies for a conflicting cluster.
    Each field is the authoritative value for the matching conflict-checked
    attribute, or None. A conflicting attribute left None is stored as null on
    the kept cluster rather than resolved to one of the fragments' values.
    """

    given_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    """The person's given name, or None if the override does not supply one."""

    middle_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    """The person's middle name, or None if the override does not supply one."""

    surname: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    """The person's surname, or None if the override does not supply one."""

    name_suffix: str | None = attr.ib(default=None, validator=is_opt_valid_name_suffix)
    """The person's name suffix, or None if the override does not supply one."""

    birthdate: datetime.date | None = attr.ib(
        default=None, validator=REASONABLE_OPT_BIRTHDATE_VALIDATOR
    )
    """The person's birthdate, or None if the override does not supply one."""

    sex: Sex | None = attr.ib(
        default=None, validator=attr.validators.optional(attr.validators.in_(Sex))
    )
    """The person's sex, or None if the override does not supply one."""

    gender: Gender | None = attr.ib(
        default=None, validator=attr.validators.optional(attr.validators.in_(Gender))
    )
    """The person's gender, or None if the override does not supply one."""

    ethnicity: Ethnicity | None = attr.ib(
        default=None, validator=attr.validators.optional(attr.validators.in_(Ethnicity))
    )
    """The person's ethnicity, or None if the override does not supply one."""

    def to_bq_dict(self) -> dict[str, object]:
        """Returns the blessed_values record for this override's table row."""
        return {
            _GIVEN_NAME_FIELD: self.given_name,
            _MIDDLE_NAME_FIELD: self.middle_name,
            _SURNAME_FIELD: self.surname,
            _NAME_SUFFIX_FIELD: self.name_suffix,
            _BIRTHDATE_FIELD: (
                self.birthdate.isoformat() if self.birthdate is not None else None
            ),
            _SEX_FIELD: self.sex.value if self.sex is not None else None,
            _GENDER_FIELD: self.gender.value if self.gender is not None else None,
            _ETHNICITY_FIELD: (
                self.ethnicity.value if self.ethnicity is not None else None
            ),
        }

    @classmethod
    def from_bq_dict(cls, record: dict[str, object]) -> "BlessedIdentityValues":
        """Builds the blessed values from a table row's blessed_values record."""
        return cls(
            given_name=_opt_str(record[_GIVEN_NAME_FIELD]),
            middle_name=_opt_str(record[_MIDDLE_NAME_FIELD]),
            surname=_opt_str(record[_SURNAME_FIELD]),
            name_suffix=_opt_str(record[_NAME_SUFFIX_FIELD]),
            birthdate=_opt_date(record[_BIRTHDATE_FIELD]),
            sex=_opt_enum(Sex, record[_SEX_FIELD]),
            gender=_opt_enum(Gender, record[_GENDER_FIELD]),
            ethnicity=_opt_enum(Ethnicity, record[_ETHNICITY_FIELD]),
        )


@attr.define(frozen=True, kw_only=True)
class IdentityClusterOverride:
    """A human-recorded override of the pipeline's automated decision for one
    cluster, matched to a cluster by its exact set of external ids. A BLESS
    override keeps a conflicting cluster using its blessed values; an EXCLUDE
    override drops the cluster entirely.
    """

    tenant: Tenant = attr.ib(validator=attr.validators.in_(Tenant))
    """Tenant the overridden cluster belongs to."""

    person_type: PersonType = attr.ib(validator=attr.validators.in_(PersonType))
    """Person type of the overridden cluster."""

    external_ids: ClusterKey = attr.ib(
        validator=[
            attr_validators.is_non_empty_tuple,
            attr_validators.is_tuple_of(tuple),
        ]
    )
    """The exact set of (external_id, id_type) pairs identifying the cluster this
    override applies to. An override matches a cluster only when the two sets are
    equal, so a cluster that gains or loses an external id no longer matches and
    is decided by the pipeline again."""

    disposition: IdentityClusterOverrideDisposition = attr.ib(
        validator=attr.validators.in_(IdentityClusterOverrideDisposition)
    )
    """Whether to keep the cluster with blessed values (BLESS) or drop it
    (EXCLUDE)."""

    blessed_values: BlessedIdentityValues | None = attr.ib(
        default=None,
        validator=attr.validators.optional(
            attr.validators.instance_of(BlessedIdentityValues)
        ),
    )
    """The values a BLESS override supplies, or None for an EXCLUDE override."""

    conflicts_hash: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_non_empty_str
    )
    """Hash of the conflicts the reviewer saw, copied from the rejected
    cluster's row at recording time, or None for an EXCLUDE override. A BLESS
    applies only while the cluster's current conflicts still hash to this
    value, so changed conflict evidence forces re-review."""

    recorded_by: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The reviewer who recorded the override."""

    recorded_at: datetime.datetime = attr.ib(validator=attr_validators.is_datetime)
    """When the override was recorded, as a timezone-aware timestamp."""

    note: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The reviewer's justification for the override."""

    def __attrs_post_init__(self) -> None:
        if self.recorded_at.tzinfo is None:
            raise ValueError(
                f"Expected a timezone-aware recorded_at, found [{self.recorded_at}]."
            )
        if self.disposition is IdentityClusterOverrideDisposition.EXCLUDE:
            if self.blessed_values is not None:
                raise ValueError(
                    f"An EXCLUDE override drops the cluster, so it must carry no "
                    f"blessed values, but found blessed values for cluster "
                    f"[{self.external_ids}]."
                )
            if self.conflicts_hash is not None:
                raise ValueError(
                    f"An EXCLUDE override drops the cluster regardless of its "
                    f"conflicts, so it must carry no conflicts_hash, but found "
                    f"one for cluster [{self.external_ids}]."
                )
            return
        if self.blessed_values is None:
            raise ValueError(
                f"A BLESS override must carry a blessed_values record (its fields "
                f"may all be null) for cluster [{self.external_ids}]."
            )
        if self.conflicts_hash is None:
            raise ValueError(
                f"A BLESS override must carry the conflicts_hash of the rejected "
                f"cluster it blesses for cluster [{self.external_ids}]."
            )

    def to_bq_row(self) -> dict[str, object]:
        """Returns the identity_cluster_override table row for this override."""
        return {
            _TENANT_COL: self.tenant.value,
            _PERSON_TYPE_COL: self.person_type.value,
            _EXTERNAL_IDS_COL: [
                {_EXTERNAL_ID_FIELD: external_id, _ID_TYPE_FIELD: id_type}
                for external_id, id_type in self.external_ids
            ],
            _DISPOSITION_COL: self.disposition.value,
            _BLESSED_VALUES_COL: (
                self.blessed_values.to_bq_dict()
                if self.blessed_values is not None
                else None
            ),
            _CONFLICTS_HASH_COL: self.conflicts_hash,
            _RECORDED_BY_COL: self.recorded_by,
            _RECORDED_AT_COL: self.recorded_at.isoformat(),
            _NOTE_COL: self.note,
        }

    @classmethod
    def from_bq_row(cls, row: dict[str, object]) -> "IdentityClusterOverride":
        """Builds an override from one identity_cluster_override table row."""
        blessed_values_record = row[_BLESSED_VALUES_COL]
        return cls(
            tenant=Tenant(row[_TENANT_COL]),
            person_type=PersonType(row[_PERSON_TYPE_COL]),
            external_ids=tuple(
                (record[_EXTERNAL_ID_FIELD], record[_ID_TYPE_FIELD])
                for record in assert_type(row[_EXTERNAL_IDS_COL], list)
            ),
            disposition=IdentityClusterOverrideDisposition(row[_DISPOSITION_COL]),
            blessed_values=(
                BlessedIdentityValues.from_bq_dict(
                    assert_type(blessed_values_record, dict)
                )
                if blessed_values_record is not None
                else None
            ),
            conflicts_hash=_opt_str(row[_CONFLICTS_HASH_COL]),
            recorded_by=assert_type(row[_RECORDED_BY_COL], str),
            recorded_at=_datetime(row[_RECORDED_AT_COL]),
            note=assert_type(row[_NOTE_COL], str),
        )


@attr.define(frozen=True, kw_only=True)
class IdentityClusterOverrides:
    """Every recorded override for one tenant, indexed for exact lookup by a
    cluster's set of external ids. The pipeline loads this once at construction
    and consults it for every cluster it builds.
    """

    tenant: Tenant = attr.ib(validator=attr.validators.in_(Tenant))
    """Tenant these overrides belong to; every override matches it."""

    _overrides_by_external_ids: dict[
        frozenset[tuple[str, str]], IdentityClusterOverride
    ] = attr.ib(validator=attr.validators.instance_of(dict))
    """Each override keyed by the frozenset of its external ids, so a cluster's
    external ids look up its override regardless of their order."""

    @classmethod
    def empty(cls, tenant: Tenant) -> "IdentityClusterOverrides":
        """Returns an empty set of overrides for the given tenant."""
        return cls(tenant=tenant, overrides_by_external_ids={})

    @classmethod
    def for_overrides(
        cls, *, tenant: Tenant, overrides: list[IdentityClusterOverride]
    ) -> "IdentityClusterOverrides":
        """Indexes the given overrides for lookup, raising if any override
        belongs to another tenant or if two overrides share one set of external
        ids (an ambiguous instruction for the same cluster)."""
        overrides_by_external_ids: dict[
            frozenset[tuple[str, str]], IdentityClusterOverride
        ] = {}
        for override in overrides:
            if override.tenant is not tenant:
                raise ValueError(
                    f"Override for cluster [{override.external_ids}] belongs to "
                    f"tenant [{override.tenant.value}], not [{tenant.value}]."
                )
            key = frozenset(override.external_ids)
            if key in overrides_by_external_ids:
                raise ValueError(
                    f"Found two overrides for the same cluster with external ids "
                    f"[{sorted(key)}]; a cluster may have at most one override."
                )
            overrides_by_external_ids[key] = override
        return cls(tenant=tenant, overrides_by_external_ids=overrides_by_external_ids)

    def get_override(self, external_ids: ClusterKey) -> IdentityClusterOverride | None:
        """Returns the override whose external ids exactly equal the given
        cluster's, or None if no override matches."""
        return self._overrides_by_external_ids.get(frozenset(external_ids))


def _opt_str(value: object) -> str | None:
    """Returns the value as a string, or None if it is None."""
    return assert_type(value, str) if value is not None else None


def _opt_date(value: object) -> datetime.date | None:
    """Returns the value as a date, parsing an ISO string, or None if None."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(assert_type(value, str))


_EnumT = TypeVar("_EnumT", bound=Enum)


def _opt_enum(enum_cls: type[_EnumT], value: object) -> _EnumT | None:
    """Returns the value parsed into the given enum, or None if it is None."""
    if value is None:
        return None
    return enum_cls(assert_type(value, str))


def _datetime(value: object) -> datetime.datetime:
    """Returns the value as a datetime, parsing an ISO string."""
    if isinstance(value, datetime.datetime):
        return value
    return datetime.datetime.fromisoformat(assert_type(value, str))
