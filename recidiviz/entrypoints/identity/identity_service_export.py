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
"""Entrypoint that exports the Identity Service's Postgres state to the
identity_service_export BigQuery dataset.

A bare invocation exports the full state: every exported table is read in a
single repeatable-read transaction and every BigQuery table is rewritten in
full. With a tenant argument, the export refreshes that tenant in place: it
reads only the tenant's identities, deletes the tenant's rows from every
BigQuery table, and appends the fresh rows, leaving other tenants' rows
untouched.

The exported identities table additionally carries legacy_person_id and
legacy_staff_id: the activity pipeline primary key computed from the identity's
active external IDs with the same code the activity pipeline uses, written to
the column matching the identity's person_type.
"""
import argparse
import datetime
import enum
import uuid
from typing import Any

from google.cloud import bigquery
from sqlalchemy import text

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.generate_primary_key import (
    generate_primary_key_from_external_id_keys,
)
from recidiviz.source_tables.identity_service_export_source_tables import (
    IDENTITY_SERVICE_EXPORT_DATASET_ID,
    LEGACY_PERSON_ID_COLUMN,
    LEGACY_STAFF_ID_COLUMN,
    MIRRORED_TABLES_TO_DESCRIPTIONS,
    build_identity_service_export_source_table_collection,
)

_IDENTITIES_ADDRESS = BigQueryAddress(
    dataset_id=IDENTITY_SERVICE_EXPORT_DATASET_ID,
    table_id=schema.Identity.__tablename__,
)


class IdentityServiceExportEntrypoint(EntrypointInterface):
    """Entrypoint that exports the Identity Service's Postgres state to the
    identity_service_export BigQuery dataset."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--tenant",
            help=(
                "When set, refresh only this tenant's rows in the export "
                "dataset. When omitted, rewrite the full dataset."
            ),
            type=Tenant,
            choices=list(Tenant),
            metavar="TENANT",
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        export_identity_service_to_bigquery(
            bq_client=BigQueryClientImpl(), tenant=args.tenant
        )


def export_identity_service_to_bigquery(
    *,
    bq_client: BigQueryClient,
    tenant: Tenant | None = None,
    use_proxy_session: bool = False,
) -> None:
    """Reads the Identity Service's Postgres state and writes it to the
    identity_service_export BigQuery dataset: the full state rewritten when
    tenant is None, or one tenant's rows refreshed in place. Set
    use_proxy_session to read Postgres through a locally running Cloud SQL
    Proxy (see recidiviz.tools.identity.run_identity_service_export) instead of
    a direct connection."""
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)
    session_context = (
        SessionFactory.for_proxy(database_key, autocommit=False)
        if use_proxy_session
        else SessionFactory.using_database(database_key, autocommit=False)
    )
    with session_context as session:
        # Read everything at one consistent snapshot so the exported tables
        # cannot disagree with each other about a concurrent import's writes.
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        identities_query = session.query(schema.Identity)
        if tenant is not None:
            identities_query = identities_query.filter(schema.Identity.tenant == tenant)
        rows_by_address = _build_export_rows(identities_query.all())

    if tenant is None:
        _rewrite_all_tables(bq_client, rows_by_address)
    else:
        _refresh_tenant_rows(bq_client, rows_by_address, tenant=tenant)


def _build_export_rows(
    identities: list[schema.Identity],
) -> dict[BigQueryAddress, list[dict[str, Any]]]:
    """Returns the rows to write to each BigQuery table for the given
    identities, keyed by table address and serialized to JSON-compatible
    values. Child table rows are collected through the Identity relationships,
    whose names match the child table names."""
    collection = build_identity_service_export_source_table_collection()

    def field_names_for_address(address: BigQueryAddress) -> list[str]:
        return [
            field.name
            for field in collection.source_tables_by_address[address].schema_fields
        ]

    rows_by_address: dict[BigQueryAddress, list[dict[str, Any]]] = {
        _IDENTITIES_ADDRESS: [
            _serialize_identity_row(
                identity, field_names=field_names_for_address(_IDENTITIES_ADDRESS)
            )
            for identity in identities
        ]
    }
    for table in MIRRORED_TABLES_TO_DESCRIPTIONS:
        address = BigQueryAddress(
            dataset_id=IDENTITY_SERVICE_EXPORT_DATASET_ID,
            table_id=table.name,
        )
        field_names = field_names_for_address(address)
        rows_by_address[address] = [
            {name: _serialize_value(getattr(child, name)) for name in field_names}
            for identity in identities
            for child in getattr(identity, table.name)
        ]
    return rows_by_address


def _rewrite_all_tables(
    bq_client: BigQueryClient,
    rows_by_address: dict[BigQueryAddress, list[dict[str, Any]]],
) -> None:
    """Rewrites every export table in full: a truncating load for tables with
    rows, a full delete for tables without (an empty load job is invalid).

    Loads use WRITE_TRUNCATE_DATA, which replaces the rows but keeps the
    table's schema and metadata such as clustering; plain WRITE_TRUNCATE
    would replace them with the load job's own (unclustered) configuration,
    undoing the table metadata the source-table update process manages."""
    jobs: list[bigquery.job.LoadJob | bigquery.QueryJob] = []
    for address, rows in rows_by_address.items():
        if rows:
            jobs.append(
                bq_client.load_into_table_async(
                    address=address,
                    rows=rows,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
                )
            )
        else:
            jobs.append(bq_client.delete_from_table_async(address))
    bq_client.wait_for_big_query_jobs(jobs)


def _refresh_tenant_rows(
    bq_client: BigQueryClient,
    rows_by_address: dict[BigQueryAddress, list[dict[str, Any]]],
    *,
    tenant: Tenant,
) -> None:
    """Replaces one tenant's rows in every export table, in four steps: delete
    the tenant's child rows, delete the tenant's identities rows, append the
    fresh identities rows, and append the fresh child rows.

    The steps run in this order because child tables carry no tenant column: a
    tenant's child rows can only be found by joining to the identities table.
    The child deletes therefore run while the old identities rows still exist,
    and the identities append lands before the child appends so that if the run
    fails partway through, the next run's child deletes can still find whatever
    child rows were already written.
    """
    identities_for_query = _IDENTITIES_ADDRESS.to_project_specific_address(
        bq_client.project_id
    ).format_address_for_query()
    tenant_children_filter = (
        f"WHERE recidiviz_id IN (SELECT recidiviz_id FROM {identities_for_query} "
        f"WHERE tenant = '{tenant.value}')"
    )
    child_delete_jobs = [
        bq_client.delete_from_table_async(address, filter_clause=tenant_children_filter)
        for address in rows_by_address
        if address != _IDENTITIES_ADDRESS
    ]
    bq_client.wait_for_big_query_jobs(child_delete_jobs)

    bq_client.delete_from_table_async(
        _IDENTITIES_ADDRESS, filter_clause=f"WHERE tenant = '{tenant.value}'"
    ).result()

    if rows_by_address[_IDENTITIES_ADDRESS]:
        bq_client.load_into_table_async(
            address=_IDENTITIES_ADDRESS,
            rows=rows_by_address[_IDENTITIES_ADDRESS],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ).result()

    child_append_jobs = [
        bq_client.load_into_table_async(
            address=address,
            rows=rows,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        for address, rows in rows_by_address.items()
        if rows and address != _IDENTITIES_ADDRESS
    ]
    bq_client.wait_for_big_query_jobs(child_append_jobs)


def _serialize_identity_row(
    identity: schema.Identity, *, field_names: list[str]
) -> dict[str, Any]:
    """Returns the BigQuery row for one identity: its Postgres columns plus the
    computed legacy_person_id and legacy_staff_id columns."""
    legacy_id_columns = _legacy_id_columns_for_identity(identity)
    return {
        name: (
            legacy_id_columns[name]
            if name in legacy_id_columns
            else _serialize_value(getattr(identity, name))
        )
        for name in field_names
    }


def _legacy_id_columns_for_identity(
    identity: schema.Identity,
) -> dict[str, int | None]:
    """Returns the values of the legacy_person_id and legacy_staff_id columns
    for one identity: the derived legacy key in the column matching the
    identity's person_type, and None in the other. Both are None for Recidiviz
    employees, who have no activity pipeline key."""
    legacy_id = _legacy_id_for_identity(identity)
    return {
        LEGACY_PERSON_ID_COLUMN: (
            legacy_id if identity.person_type is PersonType.JII else None
        ),
        LEGACY_STAFF_ID_COLUMN: (
            legacy_id if identity.person_type is PersonType.STAFF else None
        ),
    }


def _legacy_id_for_identity(identity: schema.Identity) -> int | None:
    """Returns the primary key this identity gets in activity pipeline output,
    derived from its active external IDs exactly as the activity pipeline
    derives it. Returns None when the identity has no active external IDs or
    its tenant is not a state."""
    active_external_id_keys = {
        (external_id.external_id, external_id.id_type.value)
        for external_id in identity.external_ids
        if external_id.is_active
    }
    if not active_external_id_keys:
        return None
    if not StateCode.is_state_code(identity.tenant.value):
        return None
    return generate_primary_key_from_external_id_keys(
        active_external_id_keys,
        state_code=identity.tenant.to_state_code(),
    )


def _serialize_value(value: Any) -> Any:
    """Returns the JSON-compatible form of a Postgres column value for a
    BigQuery load job."""
    if value is None:
        return None
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, uuid.UUID):
        return str(value)
    # datetime must be checked before date; it is a date subclass. BQ DATETIME
    # takes an ISO string with no offset, and UTCDateTime columns read back as
    # UTC-aware datetimes.
    if isinstance(value, datetime.datetime):
        return value.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (str, int, bool, float)):
        return value
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    raise ValueError(f"Unhandled value type [{type(value)}] in export serialization")
