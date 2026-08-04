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
"""A cluster the identity pipeline rejected because its fragments have
conflicting attributes, and the schema of the rejected_identity_cluster table its
rows are written to. The value object and the schema live together so they
cannot drift apart.
"""
import datetime
from enum import Enum

import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
)
from recidiviz.pipelines.ingest.identity.types import AttributeConflict, ConflictValue
from recidiviz.pipelines.ingest.transforms.types import ClusterKey

REJECTED_IDENTITY_CLUSTER_TABLE_ID = "rejected_identity_cluster"

# Top-level columns of the rejected_identity_cluster table. IDENTITY_CLUSTER_ID_COL is
# public because the table clusters on it; REJECTED_AT_COL because the pipeline
# test harness excludes it from fixture comparison.
IDENTITY_CLUSTER_ID_COL = "identity_cluster_id"
REJECTED_AT_COL = "rejected_at"
_TENANT_COL = "tenant"
_PERSON_TYPE_COL = "person_type"
_EXTERNAL_IDS_COL = "external_ids"
_RECORDED_CONFLICTS_COL = "recorded_conflicts"
_CONTRIBUTING_FRAGMENT_IDS_COL = "contributing_fragment_ids"

# Members of the external_ids record column.
_EXTERNAL_ID_FIELD = "external_id"
_ID_TYPE_FIELD = "id_type"

# Members of the recorded_conflicts record column.
_CONFLICT_FIELD_FIELD = "field"
_CONFLICT_VALUES_FIELD = "values"


def rejected_identity_cluster_schema_fields() -> list[bigquery.SchemaField]:
    """Returns the BQ schema of the rejected_identity_cluster table.

    The nested record fields are NULLABLE rather than REQUIRED because the BQ
    emulator's DDL engine cannot create NOT NULL nested columns (it fails with
    "Nested column attributes are unsupported"); RejectedIdentityCluster's
    validators enforce the values are present.
    """
    return [
        bigquery.SchemaField(
            REJECTED_AT_COL,
            "TIMESTAMP",
            mode="REQUIRED",
            description="When the pipeline run that rejected this cluster ran.",
        ),
        bigquery.SchemaField(
            _TENANT_COL,
            "STRING",
            mode="REQUIRED",
            description="Tenant the rejected cluster belongs to.",
        ),
        bigquery.SchemaField(
            _PERSON_TYPE_COL,
            "STRING",
            mode="REQUIRED",
            description="Person type shared by the cluster's fragments.",
        ),
        bigquery.SchemaField(
            IDENTITY_CLUSTER_ID_COL,
            "STRING",
            mode="REQUIRED",
            description=(
                "The identity_cluster_id the cluster would carry if kept, derived by "
                "hashing its external ids. Matches the kept cluster's id once the "
                "conflict is resolved."
            ),
        ),
        bigquery.SchemaField(
            _EXTERNAL_IDS_COL,
            "RECORD",
            mode="REPEATED",
            description="The cluster's external ids.",
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
            _RECORDED_CONFLICTS_COL,
            "RECORD",
            mode="REPEATED",
            description="The attribute conflicts that caused the rejection.",
            fields=(
                bigquery.SchemaField(
                    _CONFLICT_FIELD_FIELD,
                    "STRING",
                    description="Name of the conflicting attribute, e.g. birthdate.",
                ),
                bigquery.SchemaField(
                    _CONFLICT_VALUES_FIELD,
                    "STRING",
                    mode="REPEATED",
                    description="The distinct values the fragments held for the field.",
                ),
            ),
        ),
        bigquery.SchemaField(
            _CONTRIBUTING_FRAGMENT_IDS_COL,
            "STRING",
            mode="REPEATED",
            description=(
                "Ids of the fragments that contributed to the cluster, matching rows in "
                "the {tenant}_identity_fragment tables."
            ),
        ),
    ]


@attr.define(frozen=True, kw_only=True)
class RejectedIdentityCluster:
    """A cluster the identity pipeline rejected because its fragments have
    conflicting attributes: they likely do not describe one person. Rejected
    clusters are never written to the cluster tables the Identity Service
    imports; each becomes one self-contained row in the rejected_identity_cluster
    table instead."""

    tenant: Tenant = attr.ib(validator=attr.validators.in_(Tenant))
    """Tenant the pipeline was running for."""

    person_type: PersonType = attr.ib(validator=attr.validators.in_(PersonType))
    """The single person type shared by the cluster's fragments."""

    external_ids: ClusterKey = attr.ib(
        validator=[
            attr_validators.is_non_empty_tuple,
            attr_validators.is_tuple_of(tuple),
        ]
    )
    """The cluster's external ids, as (external_id, id_type) pairs."""

    conflicts: tuple[AttributeConflict, ...] = attr.ib(
        validator=[
            attr_validators.is_non_empty_tuple,
            attr_validators.is_tuple_of(AttributeConflict),
        ]
    )
    """The attribute conflicts that caused the rejection."""

    contributing_fragment_ids: tuple[str, ...] = attr.ib(
        validator=[
            attr_validators.is_non_empty_tuple,
            attr_validators.is_tuple_of(str),
        ]
    )
    """Ids of the cluster's contributing fragments, matching rows in the
    {tenant}_identity_fragment tables."""

    @property
    def identity_cluster_id(self) -> str:
        """Returns the id the cluster would carry if kept. Derived from
        external_ids only, so it matches the kept cluster's id once the conflict
        is resolved."""
        return IdentityCluster.cluster_id_for_external_ids(
            tenant=self.tenant,
            external_ids=tuple(
                IdentityClusterExternalId(
                    tenant=self.tenant, external_id=external_id, id_type=id_type
                )
                for external_id, id_type in self.external_ids
            ),
            person_type=self.person_type,
        )

    def to_bq_row(self, *, rejected_at: datetime.datetime) -> dict[str, object]:
        """Returns the rejected_identity_cluster table row for this cluster,
        stamped with the given run timestamp."""
        if rejected_at.tzinfo is None:
            raise ValueError(
                f"Expected a timezone-aware rejected_at, found [{rejected_at}]."
            )
        return {
            REJECTED_AT_COL: rejected_at.isoformat(),
            _TENANT_COL: self.tenant.value,
            _PERSON_TYPE_COL: self.person_type.value,
            IDENTITY_CLUSTER_ID_COL: self.identity_cluster_id,
            _EXTERNAL_IDS_COL: [
                {_EXTERNAL_ID_FIELD: external_id, _ID_TYPE_FIELD: id_type}
                for external_id, id_type in self.external_ids
            ],
            _RECORDED_CONFLICTS_COL: [
                {
                    _CONFLICT_FIELD_FIELD: conflict.field,
                    _CONFLICT_VALUES_FIELD: [
                        _serialize_conflict_value(value) for value in conflict.values
                    ],
                }
                for conflict in self.conflicts
            ],
            _CONTRIBUTING_FRAGMENT_IDS_COL: list(self.contributing_fragment_ids),
        }


def _serialize_conflict_value(value: ConflictValue) -> str:
    """Serializes a conflict value for the recorded_conflicts string column.
    Enums render as their value (MALE, not Sex.MALE); dates render as ISO
    strings; strings render as themselves.
    """
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)
