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
"""The record of an identity fragment that had sentinel external IDs stripped by
FilterSentinelExternalIds, and the schema of the
sentinel_stripped_identity_fragment table its rows are written to. The value
object and the schema live together so they cannot drift apart.
"""
import datetime

import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.common.constants.tenants import Tenant
from recidiviz.pipelines.ingest.types import ExternalIdKey, UpperBoundDate

SENTINEL_STRIPPED_IDENTITY_FRAGMENT_TABLE_ID = "sentinel_stripped_identity_fragment"

# Top-level columns of the sentinel_stripped_identity_fragment table.
# STRIPPED_AT_COL and UPPER_BOUND_DATE_COL are public because the pipeline test
# harness excludes them from fixture comparison; PRESTRIPPED_IDENTITY_FRAGMENT_ID_COL
# because the table clusters on it.
STRIPPED_AT_COL = "stripped_at"
UPPER_BOUND_DATE_COL = "upper_bound_date"
PRESTRIPPED_IDENTITY_FRAGMENT_ID_COL = "prestripped_identity_fragment_id"
_TENANT_COL = "tenant"
_INGEST_VIEW_NAME_COL = "ingest_view_name"
_STRIPPED_EXTERNAL_IDS_COL = "stripped_external_ids"
_SURVIVING_EXTERNAL_IDS_COL = "surviving_external_ids"
_FULLY_DROPPED_COL = "fully_dropped"

# Members of the stripped_external_ids and surviving_external_ids record columns.
_EXTERNAL_ID_FIELD = "external_id"
_ID_TYPE_FIELD = "id_type"


def _external_ids_record_fields() -> tuple[bigquery.SchemaField, ...]:
    return (
        bigquery.SchemaField(
            _EXTERNAL_ID_FIELD,
            "STRING",
            mode="REQUIRED",
            description="External id value.",
        ),
        bigquery.SchemaField(
            _ID_TYPE_FIELD,
            "STRING",
            mode="REQUIRED",
            description="External id type.",
        ),
    )


def sentinel_stripped_identity_fragment_schema_fields() -> list[bigquery.SchemaField]:
    """Returns the BQ schema of the sentinel_stripped_identity_fragment table."""
    return [
        bigquery.SchemaField(
            STRIPPED_AT_COL,
            "TIMESTAMP",
            mode="REQUIRED",
            description="When the pipeline run that stripped this fragment ran.",
        ),
        bigquery.SchemaField(
            _TENANT_COL,
            "STRING",
            mode="REQUIRED",
            description="Tenant the stripped fragment belongs to.",
        ),
        bigquery.SchemaField(
            _INGEST_VIEW_NAME_COL,
            "STRING",
            mode="REQUIRED",
            description="Name of the identity ingest view that produced the fragment.",
        ),
        bigquery.SchemaField(
            UPPER_BOUND_DATE_COL,
            "TIMESTAMP",
            mode="REQUIRED",
            description=(
                "Raw data snapshot (upper bound date) the fragment was generated "
                "from. Sentinels are identified per snapshot, so the same external "
                "id value can be stripped at one date and kept at another."
            ),
        ),
        bigquery.SchemaField(
            PRESTRIPPED_IDENTITY_FRAGMENT_ID_COL,
            "STRING",
            mode="REQUIRED",
            description=(
                "Content hash of the fragment before stripping. This is not the "
                "identity_fragment_id of any row in the {tenant}_identity_fragment "
                "tables: those tables are written after stripping (and after the "
                "per-view merge), so a stripped fragment's post-strip content "
                "hashes differently and a fully dropped fragment appears nowhere "
                "else at all."
            ),
        ),
        bigquery.SchemaField(
            _STRIPPED_EXTERNAL_IDS_COL,
            "RECORD",
            mode="REPEATED",
            description="The sentinel external ids stripped from the fragment.",
            fields=_external_ids_record_fields(),
        ),
        bigquery.SchemaField(
            _SURVIVING_EXTERNAL_IDS_COL,
            "RECORD",
            mode="REPEATED",
            description=(
                "The fragment's remaining external ids, empty if the fragment was "
                "fully dropped."
            ),
            fields=_external_ids_record_fields(),
        ),
        bigquery.SchemaField(
            _FULLY_DROPPED_COL,
            "BOOLEAN",
            mode="REQUIRED",
            description=(
                "Whether every external id on the fragment was a sentinel, meaning "
                "the whole fragment was dropped from clustering."
            ),
        ),
    ]


@attr.define(frozen=True, kw_only=True)
class SentinelStrippedIdentityFragment:
    """The record of an identity fragment that had sentinel external IDs
    stripped by FilterSentinelExternalIds. Each becomes one row in the
    sentinel_stripped_identity_fragment table, the only durable record of what
    the filter removed: the {tenant}_identity_fragment debug tables are written
    after the filter runs, and a fully dropped fragment leaves no other trace."""

    tenant: Tenant = attr.ib(validator=attr.validators.in_(Tenant))
    """Tenant the pipeline was running for."""

    ingest_view_name: str = attr.ib(validator=attr_validators.is_str)
    """Name of the identity ingest view that produced the fragment."""

    upper_bound_date: UpperBoundDate = attr.ib(validator=attr_validators.is_float)
    """UTC timestamp of the raw data snapshot the fragment was generated from."""

    prestripped_identity_fragment_id: str = attr.ib(validator=attr_validators.is_str)
    """Content hash of the fragment before stripping, computed with the same
    derivation as identity_fragment_id (assign_identity_fragment_id)."""

    stripped_external_ids: tuple[ExternalIdKey, ...] = attr.ib(
        validator=[
            attr_validators.is_non_empty_tuple,
            attr_validators.is_tuple_of(tuple),
        ]
    )
    """The sentinel external ids stripped from the fragment, as
    (external_id, id_type) pairs."""

    surviving_external_ids: tuple[ExternalIdKey, ...] = attr.ib(
        validator=attr_validators.is_tuple_of(tuple)
    )
    """The fragment's remaining external ids, as (external_id, id_type) pairs.
    Empty if the fragment was fully dropped."""

    @property
    def fully_dropped(self) -> bool:
        """Returns whether every external id on the fragment was a sentinel,
        meaning the whole fragment was dropped from clustering."""
        return not self.surviving_external_ids

    def to_bq_row(self, *, stripped_at: datetime.datetime) -> dict[str, object]:
        """Returns the sentinel_stripped_identity_fragment table row for this
        record, stamped with the given run timestamp."""
        if stripped_at.tzinfo is None:
            raise ValueError(
                f"Expected a timezone-aware stripped_at, found [{stripped_at}]."
            )
        return {
            STRIPPED_AT_COL: stripped_at.isoformat(),
            _TENANT_COL: self.tenant.value,
            _INGEST_VIEW_NAME_COL: self.ingest_view_name,
            UPPER_BOUND_DATE_COL: datetime.datetime.fromtimestamp(
                self.upper_bound_date, tz=datetime.UTC
            ).isoformat(),
            PRESTRIPPED_IDENTITY_FRAGMENT_ID_COL: self.prestripped_identity_fragment_id,
            _STRIPPED_EXTERNAL_IDS_COL: [
                {_EXTERNAL_ID_FIELD: external_id, _ID_TYPE_FIELD: id_type}
                for external_id, id_type in self.stripped_external_ids
            ],
            _SURVIVING_EXTERNAL_IDS_COL: [
                {_EXTERNAL_ID_FIELD: external_id, _ID_TYPE_FIELD: id_type}
                for external_id, id_type in self.surviving_external_ids
            ],
            _FULLY_DROPPED_COL: self.fully_dropped,
        }
