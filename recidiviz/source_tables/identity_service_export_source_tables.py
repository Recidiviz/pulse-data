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
"""Defines the identity_service_export source-table collection: the read-only
BigQuery export of the Identity Service's Postgres state, written by the
Identity Service export entrypoint and consumed by the activity pipeline,
Roster Sync, and other readers of identity data.

The collection covers only the tables that carry identity data consumers read;
it deliberately excludes identity-system operational tables (review candidates,
merge/split audit events, no_merge pairs) and the import-machinery columns on
the identities table.
"""
from google.cloud import bigquery
from sqlalchemy import Table

from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.source_tables.source_table_config import (
    CALC_UPDATE_GROUPS,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.utils.types import assert_type

IDENTITY_SERVICE_EXPORT_DATASET_ID = "identity_service_export"

PERSON_OR_STAFF_ID_COLUMN = "person_or_staff_id"

# Columns on the identities table that exist to support the import process and
# carry no meaning for readers of the export.
_EXCLUDED_IDENTITIES_COLUMNS = frozenset(
    {"last_cluster_hash", "skip_demographic_guard"}
)

_PERSON_OR_STAFF_ID_DESCRIPTION = (
    "Primary key for this identity in activity pipeline output (person_id for "
    "justice-involved individuals, staff_id for staff), computed at export time "
    "from the identity's active external IDs with the same code the activity "
    "pipeline uses. NULL for identities with no active external IDs (e.g. "
    "Recidiviz employees, or identities retired by a merge) or whose tenant "
    "does not correspond to a state."
)

# Postgres tables mirrored into the export as-is, and the BQ table description
# for each, written specifically for BigQuery readers. The identities table is
# not mirrored as-is (see _identities_schema) and so is not listed here.
MIRRORED_TABLES_TO_DESCRIPTIONS: dict[Table, str] = {
    assert_type(
        schema.ExternalId.__table__, Table
    ): "External identifiers attached to identities.",
    assert_type(schema.Name.__table__, Table): (
        "Name attributes attached to identities, one row per name per source "
        "(state data system, product app, or admin override). The use column "
        "distinguishes official, preferred, former, and alias names."
    ),
    assert_type(schema.DateOfBirth.__table__, Table): (
        "Date-of-birth attributes attached to identities, one row per value "
        "per source. The canonical column marks the row to use when sources "
        "disagree."
    ),
    assert_type(schema.Gender.__table__, Table): (
        "Gender attributes attached to identities, one row per value per "
        "source. The canonical column marks the row to use when sources "
        "disagree."
    ),
    assert_type(schema.Race.__table__, Table): (
        "Race attributes attached to identities, one row per race per "
        "source; a person may have multiple races."
    ),
    assert_type(schema.Sex.__table__, Table): (
        "Sex attributes attached to identities, one row per value per "
        "source. The canonical column marks the row to use when sources "
        "disagree."
    ),
    assert_type(schema.Ethnicity.__table__, Table): (
        "Ethnicity attributes attached to identities, one row per value per "
        "source. The canonical column marks the row to use when sources "
        "disagree."
    ),
    assert_type(schema.PhoneNumber.__table__, Table): (
        "Phone number attributes attached to identities, one row per number "
        "per source. The preferred column marks the person's preferred "
        "number."
    ),
    assert_type(schema.Email.__table__, Table): (
        "Email address attributes attached to identities, one row per "
        "address per source, including the normalized address hash used by "
        "auth."
    ),
}


def _identities_schema() -> list[bigquery.SchemaField]:
    """Returns the BQ schema for the exported identities table: the Postgres
    columns minus the import-machinery columns, plus the computed
    person_or_staff_id column."""
    fields = [
        field
        for field in schema_for_sqlalchemy_table(
            assert_type(schema.Identity.__table__, Table)
        )
        if field.name not in _EXCLUDED_IDENTITIES_COLUMNS
    ]
    fields.append(
        bigquery.SchemaField(
            PERSON_OR_STAFF_ID_COLUMN,
            bigquery.enums.SqlTypeNames.INTEGER.value,
            mode="NULLABLE",
            description=_PERSON_OR_STAFF_ID_DESCRIPTION,
        )
    )
    return fields


def build_identity_service_export_source_table_collection() -> SourceTableCollection:
    """Returns the source-table collection for the identity_service_export
    dataset, with one table per exported Identity Service Postgres table.
    """
    collection = SourceTableCollection(
        dataset_id=IDENTITY_SERVICE_EXPORT_DATASET_ID,
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        update_groups=CALC_UPDATE_GROUPS,
        description="Read-only export of the Identity Service's Postgres state.",
    )

    collection.add_source_table(
        table_id=assert_type(schema.Identity.__tablename__, str),
        description=(
            "Identity records, keyed by an immutable Recidiviz-assigned UUID, "
            "plus the computed person_or_staff_id column."
        ),
        schema_fields=_identities_schema(),
        # Readers and the per-tenant export refresh both filter on tenant.
        clustering_fields=[assert_type(schema.Identity.tenant.name, str)],
    )

    for table, description in MIRRORED_TABLES_TO_DESCRIPTIONS.items():
        collection.add_source_table(
            table_id=table.name,
            description=description,
            schema_fields=schema_for_sqlalchemy_table(table),
        )

    return collection
