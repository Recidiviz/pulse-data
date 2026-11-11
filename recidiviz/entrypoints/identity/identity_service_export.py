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

Identities are read in keyset-paginated batches, each batch is serialized and
appended to one newline-delimited JSON GCS file per table, and each BigQuery
table is then rewritten by a single load job from its GCS file, so memory use
is bounded by the batch size no matter how many identities are exported.

The exported identities table additionally carries legacy_person_id and
legacy_staff_id: the activity pipeline primary key computed from the identity's
active external IDs with the same code the activity pipeline uses, written to
the column matching the identity's person_type.
"""
import argparse
import contextlib
import datetime
import enum
import json
import uuid
from typing import IO, Any

import attr
from google.cloud import bigquery
from sqlalchemy import text
from sqlalchemy.orm import Session

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators
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
from recidiviz.source_tables.identity_service_export_names import (
    IDENTITY_SERVICE_EXPORT_DATASET_ID,
    LEGACY_PERSON_ID_COLUMN,
    LEGACY_STAFF_ID_COLUMN,
)
from recidiviz.source_tables.identity_service_export_source_tables import (
    MIRRORED_TABLES_TO_DESCRIPTIONS,
    build_identity_service_export_source_table_collection,
)

_IDENTITIES_ADDRESS = BigQueryAddress(
    dataset_id=IDENTITY_SERVICE_EXPORT_DATASET_ID,
    table_id=schema.Identity.__tablename__,
)

# Identities read from Postgres per batch. Each batch's ORM objects and
# serialized rows are released before the next batch is read, so this bounds
# the export's memory use.
_EXPORT_BATCH_SIZE = 2000

# Must match the name_suffix of the identity_service_export_load_files bucket
# module in recidiviz/tools/deploy/terraform/cloud-run-identity-service-export.tf.
_LOAD_FILES_BUCKET_SUFFIX = "identity-service-export"

# Buffer size per open GCS writer before a chunk is flushed to GCS. One writer
# is open per exported table, so worst-case buffered memory is this times the
# table count. Must be a multiple of 256 KiB per GCS resumable-upload rules.
_GCS_WRITER_CHUNK_SIZE = 4 * 1024 * 1024


@attr.define(frozen=True, kw_only=True)
class _ExportTable:
    """One exported BigQuery table and the GCS file its rows are staged in
    during an export run."""

    address: BigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddress)
    )
    """Address of the BigQuery table this export rewrites."""

    schema_fields: list[bigquery.SchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(bigquery.SchemaField),
        ]
    )
    """BigQuery schema of the table, from the source-table collection."""

    load_file_path: GcsfsFilePath = attr.ib(
        validator=attr.validators.instance_of(GcsfsFilePath)
    )
    """GCS path the table's rows are staged in for this run, unique per run."""

    @property
    def field_names(self) -> list[str]:
        return [field.name for field in self.schema_fields]


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
            bq_client=BigQueryClientImpl(),
            fs=GcsfsFactory.build(),
            tenant=args.tenant,
        )


def export_identity_service_to_bigquery(
    *,
    bq_client: BigQueryClient,
    fs: GCSFileSystem,
    tenant: Tenant | None = None,
    use_proxy_session: bool = False,
) -> None:
    """Reads the Identity Service's Postgres state and writes it to the
    identity_service_export BigQuery dataset: the full state rewritten when
    tenant is None, or one tenant's rows refreshed in place. Set
    use_proxy_session to read Postgres through a locally running Cloud SQL
    Proxy (see recidiviz.tools.identity.run_identity_service_export) instead of
    a direct connection."""
    collection = build_identity_service_export_source_table_collection()
    run_prefix = f"{tenant.value if tenant else 'full'}_{uuid.uuid4()}"
    export_tables_by_address = {
        address: _ExportTable(
            address=address,
            schema_fields=table.schema_fields,
            load_file_path=GcsfsFilePath.from_absolute_path(
                f"gs://{bq_client.project_id}-{_LOAD_FILES_BUCKET_SUFFIX}/"
                f"{run_prefix}/{address.table_id}.json"
            ),
        )
        for address, table in collection.source_tables_by_address.items()
    }

    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)
    session_context = (
        SessionFactory.for_proxy(database_key, autocommit=False)
        if use_proxy_session
        else SessionFactory.using_database(database_key, autocommit=False)
    )
    with session_context as session:
        row_counts_by_address = _stream_export_rows_to_gcs(
            session=session,
            fs=fs,
            export_tables_by_address=export_tables_by_address,
            tenant=tenant,
        )

    if tenant is None:
        _rewrite_all_tables(
            bq_client,
            export_tables_by_address=export_tables_by_address,
            row_counts_by_address=row_counts_by_address,
        )
    else:
        _refresh_tenant_rows(
            bq_client,
            export_tables_by_address=export_tables_by_address,
            row_counts_by_address=row_counts_by_address,
            tenant=tenant,
        )

    # The staged files have served their purpose. A failure during streaming
    # aborts the uploads and leaves no files at all; a failure during the load
    # phase leaves the staged files behind for debugging until the bucket's
    # lifecycle rule removes them.
    for export_table in export_tables_by_address.values():
        fs.delete(export_table.load_file_path)


def _stream_export_rows_to_gcs(
    *,
    session: Session,
    fs: GCSFileSystem,
    export_tables_by_address: dict[BigQueryAddress, "_ExportTable"],
    tenant: Tenant | None,
) -> dict[BigQueryAddress, int]:
    """Reads identities in keyset-paginated batches at one repeatable-read
    snapshot and appends each batch's rows to one newline-delimited JSON GCS
    file per exported table. Returns the number of rows written per table."""
    # Read everything at one consistent snapshot so the exported tables
    # cannot disagree with each other about a concurrent import's writes.
    session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))

    row_counts_by_address = {address: 0 for address in export_tables_by_address}
    with contextlib.ExitStack() as stack:
        # Each file is written as one resumable upload, which only commits its
        # object when the writer closes: a crash mid-stream leaves nothing
        # behind, and the Cloud Run job's retry re-runs the whole export under
        # a new run prefix. That makes losing in-flight rows acceptable here,
        # unlike the incremental exports GcsCsvPartWriter exists for.
        writers_by_address: dict[BigQueryAddress, IO[str]] = {
            address: stack.enter_context(
                fs.open(
                    export_table.load_file_path,
                    mode="w",
                    chunk_size=_GCS_WRITER_CHUNK_SIZE,
                    encoding="utf-8",
                    verifiable=False,
                )
            )
            for address, export_table in export_tables_by_address.items()
        }

        # Discrete keyset-paginated queries rather than one streamed query with
        # yield_per: each batch query fully materializes its identities, so the
        # selectin-loaded child relationships load per batch and expunge_all can
        # release the whole batch. yield_per would stream rows off an open
        # server-side cursor, which composes badly with the per-batch
        # relationship loads.
        last_recidiviz_id = None
        while True:
            batch_query = session.query(schema.Identity).order_by(
                schema.Identity.recidiviz_id
            )
            if tenant is not None:
                batch_query = batch_query.filter(schema.Identity.tenant == tenant)
            if last_recidiviz_id is not None:
                batch_query = batch_query.filter(
                    schema.Identity.recidiviz_id > last_recidiviz_id
                )
            batch = batch_query.limit(_EXPORT_BATCH_SIZE).all()
            if not batch:
                break
            rows_by_address = _build_export_rows(
                batch, export_tables_by_address=export_tables_by_address
            )
            for address, rows in rows_by_address.items():
                writer = writers_by_address[address]
                for row in rows:
                    writer.write(json.dumps(row) + "\n")
                row_counts_by_address[address] += len(rows)
            last_recidiviz_id = batch[-1].recidiviz_id
            # Release this batch's ORM objects so memory stays bounded by the
            # batch size.
            session.expunge_all()

    return row_counts_by_address


def _build_export_rows(
    identities: list[schema.Identity],
    *,
    export_tables_by_address: dict[BigQueryAddress, "_ExportTable"],
) -> dict[BigQueryAddress, list[dict[str, Any]]]:
    """Returns the rows to write to each BigQuery table for the given
    identities, keyed by table address and serialized to JSON-compatible
    values. Child table rows are collected through the Identity relationships,
    whose names match the child table names."""
    rows_by_address: dict[BigQueryAddress, list[dict[str, Any]]] = {
        _IDENTITIES_ADDRESS: [
            _serialize_identity_row(
                identity,
                field_names=export_tables_by_address[_IDENTITIES_ADDRESS].field_names,
            )
            for identity in identities
        ]
    }
    for table in MIRRORED_TABLES_TO_DESCRIPTIONS:
        address = BigQueryAddress(
            dataset_id=IDENTITY_SERVICE_EXPORT_DATASET_ID,
            table_id=table.name,
        )
        field_names = export_tables_by_address[address].field_names
        rows_by_address[address] = [
            {name: _serialize_value(getattr(child, name)) for name in field_names}
            for identity in identities
            for child in getattr(identity, table.name)
        ]
    return rows_by_address


def _load_table_from_staged_file(
    bq_client: BigQueryClient,
    export_table: "_ExportTable",
    *,
    write_disposition: str,
) -> None:
    """Runs a load job that writes the staged newline-delimited JSON file into
    the given BigQuery table."""
    bq_client.load_table_from_json_cloud_storage(
        source_uris=[export_table.load_file_path.uri()],
        destination_address=export_table.address,
        destination_table_schema=export_table.schema_fields,
        write_disposition=write_disposition,
    )


def _rewrite_all_tables(
    bq_client: BigQueryClient,
    *,
    export_tables_by_address: dict[BigQueryAddress, "_ExportTable"],
    row_counts_by_address: dict[BigQueryAddress, int],
) -> None:
    """Rewrites every export table in full: a truncating load for tables with
    rows, a full delete for tables without (an empty load job is invalid).

    Loads use WRITE_TRUNCATE_DATA, which replaces the rows but keeps the
    table's schema and metadata such as clustering; plain WRITE_TRUNCATE
    would replace them with the load job's own (unclustered) configuration,
    undoing the table metadata the source-table update process manages."""
    delete_jobs: list[bigquery.QueryJob] = []
    # TODO(OBT-56632): Submit the loads asynchronously and wait in bulk, like
    # the deletes, so the tables rewrite in parallel and the window where some
    # tables are fresh and others stale lasts seconds rather than the full
    # load phase.
    for address, row_count in row_counts_by_address.items():
        if row_count:
            _load_table_from_staged_file(
                bq_client,
                export_tables_by_address[address],
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
            )
        else:
            delete_jobs.append(bq_client.delete_from_table_async(address))
    bq_client.wait_for_big_query_jobs(delete_jobs)


def _refresh_tenant_rows(
    bq_client: BigQueryClient,
    *,
    export_tables_by_address: dict[BigQueryAddress, "_ExportTable"],
    row_counts_by_address: dict[BigQueryAddress, int],
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
        for address in row_counts_by_address
        if address != _IDENTITIES_ADDRESS
    ]
    bq_client.wait_for_big_query_jobs(child_delete_jobs)

    bq_client.delete_from_table_async(
        _IDENTITIES_ADDRESS, filter_clause=f"WHERE tenant = '{tenant.value}'"
    ).result()

    if row_counts_by_address[_IDENTITIES_ADDRESS]:
        _load_table_from_staged_file(
            bq_client,
            export_tables_by_address[_IDENTITIES_ADDRESS],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    for address, row_count in row_counts_by_address.items():
        if address == _IDENTITIES_ADDRESS or not row_count:
            continue
        _load_table_from_staged_file(
            bq_client,
            export_tables_by_address[address],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )


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
