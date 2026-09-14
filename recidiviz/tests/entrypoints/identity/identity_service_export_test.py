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
"""Tests for the Identity Service export entrypoint."""
import unittest
from unittest.mock import MagicMock, create_autospec

import pytest
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.identity import IdentifierType, PersonType
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.entrypoints.identity.identity_service_export import (
    IdentityServiceExportEntrypoint,
    export_identity_service_to_bigquery,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.generate_primary_key import (
    generate_primary_key_from_external_id_keys,
)
from recidiviz.source_tables.identity_service_export_source_tables import (
    LEGACY_PERSON_ID_COLUMN,
    LEGACY_STAFF_ID_COLUMN,
    build_identity_service_export_source_table_collection,
)
from recidiviz.tests.services.identity.test_utils import (
    NEW_ID,
    RECIDIVIZ_ID,
    RETIRED_ID,
    insert_external_id,
    insert_identity,
    insert_name,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_PROJECT_ID = "recidiviz-456"


@pytest.mark.uses_db
class IdentityServiceExportTest(unittest.TestCase):
    """Tests that the export reads the identity Postgres state and writes the
    expected rows to every table in the identity_service_export dataset."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )
        self.mock_bq_client = create_autospec(BigQueryClientImpl)
        self.mock_bq_client.project_id = _PROJECT_ID
        self.mock_bq_client.load_into_table_async.return_value = MagicMock()
        self.mock_bq_client.delete_from_table_async.return_value = MagicMock()
        self.collection = build_identity_service_export_source_table_collection()

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def _rows_loaded_by_table_id(
        self,
        *,
        expected_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
    ) -> dict[str, list[dict]]:
        """Returns the rows passed to load_into_table_async, keyed by table id,
        after asserting every load used the expected write disposition."""
        rows_by_table_id = {}
        for call in self.mock_bq_client.load_into_table_async.call_args_list:
            self.assertEqual(expected_disposition, call.kwargs["write_disposition"])
            rows_by_table_id[call.kwargs["address"].table_id] = call.kwargs["rows"]
        return rows_by_table_id

    def _deleted_addresses(self) -> list[BigQueryAddress]:
        return [
            call.args[0]
            for call in self.mock_bq_client.delete_from_table_async.call_args_list
        ]

    def test_full_export_covers_every_collection_table(self) -> None:
        """A full export touches every table: a truncating load where there are
        rows, a full delete where there are none."""
        insert_identity(recidiviz_id=RECIDIVIZ_ID)

        export_identity_service_to_bigquery(bq_client=self.mock_bq_client)

        loaded = {
            call.kwargs["address"]
            for call in self.mock_bq_client.load_into_table_async.call_args_list
        }
        deleted = set(self._deleted_addresses())
        self.assertEqual(
            set(self.collection.source_tables_by_address), loaded | deleted
        )
        self.assertFalse(loaded & deleted)
        for call in self.mock_bq_client.delete_from_table_async.call_args_list:
            self.assertEqual({}, call.kwargs)

    def test_identity_row_matches_bq_schema_and_computes_key(self) -> None:
        insert_identity(recidiviz_id=RECIDIVIZ_ID)
        insert_external_id(
            recidiviz_id=RECIDIVIZ_ID,
            external_id="A1234",
            id_type=IdentifierType.US_OZ_KDS_PERSON_ID,
        )
        insert_external_id(
            recidiviz_id=RECIDIVIZ_ID,
            external_id="E99",
            id_type=IdentifierType.US_OZ_LOTR_ID,
        )

        export_identity_service_to_bigquery(bq_client=self.mock_bq_client)

        [identity_row] = self._rows_loaded_by_table_id()["identities"]
        expected_field_names = {
            field.name
            for table in self.collection.source_tables
            if table.address.table_id == "identities"
            for field in table.schema_fields
        }
        self.assertEqual(expected_field_names, set(identity_row))
        self.assertEqual(str(RECIDIVIZ_ID), identity_row["recidiviz_id"])
        self.assertEqual("US_OZ", identity_row["tenant"])
        self.assertEqual("ACTIVE", identity_row["status"])
        self.assertEqual("2026-01-01T00:00:00", identity_row["created_utc"])
        self.assertEqual(
            generate_primary_key_from_external_id_keys(
                {
                    ("A1234", IdentifierType.US_OZ_KDS_PERSON_ID.value),
                    ("E99", IdentifierType.US_OZ_LOTR_ID.value),
                },
                state_code=StateCode.US_OZ,
            ),
            identity_row[LEGACY_PERSON_ID_COLUMN],
        )
        # A JII identity's key lands only in legacy_person_id.
        self.assertIsNone(identity_row[LEGACY_STAFF_ID_COLUMN])

    def test_staff_identity_key_lands_in_legacy_staff_id(self) -> None:
        insert_identity(recidiviz_id=RECIDIVIZ_ID, person_type=PersonType.STAFF)
        insert_external_id(
            recidiviz_id=RECIDIVIZ_ID,
            external_id="A1234",
            id_type=IdentifierType.US_OZ_KDS_PERSON_ID,
        )

        export_identity_service_to_bigquery(bq_client=self.mock_bq_client)

        [identity_row] = self._rows_loaded_by_table_id()["identities"]
        self.assertEqual(
            generate_primary_key_from_external_id_keys(
                {("A1234", IdentifierType.US_OZ_KDS_PERSON_ID.value)},
                state_code=StateCode.US_OZ,
            ),
            identity_row[LEGACY_STAFF_ID_COLUMN],
        )
        self.assertIsNone(identity_row[LEGACY_PERSON_ID_COLUMN])

    def test_no_key_without_active_external_ids(self) -> None:
        insert_identity(recidiviz_id=RECIDIVIZ_ID)
        insert_identity(recidiviz_id=RETIRED_ID)
        insert_external_id(
            recidiviz_id=RETIRED_ID, external_id="A1234", is_active=False
        )

        export_identity_service_to_bigquery(bq_client=self.mock_bq_client)

        rows = self._rows_loaded_by_table_id()["identities"]
        self.assertEqual(
            {str(RECIDIVIZ_ID): (None, None), str(RETIRED_ID): (None, None)},
            {
                row["recidiviz_id"]: (
                    row[LEGACY_PERSON_ID_COLUMN],
                    row[LEGACY_STAFF_ID_COLUMN],
                )
                for row in rows
            },
        )

    def test_no_key_for_non_state_tenant(self) -> None:
        insert_identity(recidiviz_id=NEW_ID, tenant=Tenant.RECIDIVIZ)
        insert_external_id(recidiviz_id=NEW_ID, external_id="EMP-1")

        export_identity_service_to_bigquery(bq_client=self.mock_bq_client)

        [identity_row] = self._rows_loaded_by_table_id()["identities"]
        self.assertIsNone(identity_row[LEGACY_PERSON_ID_COLUMN])
        self.assertIsNone(identity_row[LEGACY_STAFF_ID_COLUMN])

    def test_attribute_rows_serialize(self) -> None:
        insert_identity(recidiviz_id=RECIDIVIZ_ID)
        insert_name(recidiviz_id=RECIDIVIZ_ID, given_name="ARAGORN", surname="ELESSAR")

        export_identity_service_to_bigquery(bq_client=self.mock_bq_client)

        rows_by_table_id = self._rows_loaded_by_table_id()
        [name_row] = rows_by_table_id["names"]
        self.assertEqual(str(RECIDIVIZ_ID), name_row["recidiviz_id"])
        self.assertEqual("ARAGORN", name_row["given_name"])
        self.assertEqual("OFFICIAL", name_row["use"])
        self.assertEqual("EXTERNAL_DATA_SYSTEM", name_row["source_type"])
        self.assertEqual([], name_row["middle_names"])
        self.assertEqual("2026-01-01T00:00:00", name_row["last_updated_utc"])
        # Emails are empty, so the emails table is cleared with a delete rather
        # than loaded.
        self.assertNotIn("emails", rows_by_table_id)
        self.assertIn(
            "emails", {address.table_id for address in self._deleted_addresses()}
        )

    def test_excluded_identities_columns_not_exported(self) -> None:
        insert_identity(recidiviz_id=RECIDIVIZ_ID)

        export_identity_service_to_bigquery(bq_client=self.mock_bq_client)

        [identity_row] = self._rows_loaded_by_table_id()["identities"]
        self.assertNotIn("last_cluster_hash", identity_row)
        self.assertNotIn("skip_demographic_guard", identity_row)

    def test_tenant_refresh_scopes_reads_deletes_and_appends(self) -> None:
        """A tenant-scoped export appends only the tenant's rows and deletes
        with tenant-scoped filters, child tables before identities."""
        insert_identity(recidiviz_id=RECIDIVIZ_ID, tenant=Tenant.US_OZ)
        insert_external_id(recidiviz_id=RECIDIVIZ_ID, external_id="A1234")
        insert_name(recidiviz_id=RECIDIVIZ_ID, given_name="ARAGORN", surname="ELESSAR")
        insert_identity(recidiviz_id=NEW_ID, tenant=Tenant.US_ND)
        insert_external_id(recidiviz_id=NEW_ID, external_id="ND-1")

        export_identity_service_to_bigquery(
            bq_client=self.mock_bq_client, tenant=Tenant.US_OZ
        )

        # Appends contain only the US_OZ rows, and only non-empty tables load.
        rows_by_table_id = self._rows_loaded_by_table_id(
            expected_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        self.assertEqual({"identities", "external_ids", "names"}, set(rows_by_table_id))
        [identity_row] = rows_by_table_id["identities"]
        self.assertEqual(str(RECIDIVIZ_ID), identity_row["recidiviz_id"])

        # Every table is cleared of the tenant's rows: child tables through the
        # identities table, the identities table by its tenant column.
        delete_calls = self.mock_bq_client.delete_from_table_async.call_args_list
        filters_by_table_id = {
            call.args[0].table_id: call.kwargs["filter_clause"] for call in delete_calls
        }
        self.assertEqual(
            {a.table_id for a in self.collection.source_tables_by_address},
            set(filters_by_table_id),
        )
        self.assertEqual("WHERE tenant = 'US_OZ'", filters_by_table_id["identities"])
        expected_child_filter = (
            "WHERE recidiviz_id IN (SELECT recidiviz_id FROM "
            f"`{_PROJECT_ID}.identity_service_export.identities` "
            "WHERE tenant = 'US_OZ')"
        )
        self.assertEqual(expected_child_filter, filters_by_table_id["names"])

        # Ordering: child deletes, then the identities delete, then the
        # identities append, then child appends. The identities append must
        # come first so a re-run's child deletes can find any partially
        # appended child rows through it.
        identities_address = delete_calls[-1].args[0]
        ordered_methods = [
            (name, kwargs["address"] if "address" in kwargs else args[0])
            for name, args, kwargs in self.mock_bq_client.mock_calls
            if name in ("delete_from_table_async", "load_into_table_async")
        ]
        identities_delete_index = ordered_methods.index(
            ("delete_from_table_async", identities_address)
        )
        for index, (method, _) in enumerate(ordered_methods):
            if method == "delete_from_table_async":
                self.assertLessEqual(index, identities_delete_index)
            else:
                self.assertGreater(index, identities_delete_index)
        self.assertEqual(
            ("load_into_table_async", identities_address),
            ordered_methods[identities_delete_index + 1],
        )


class IdentityServiceExportEntrypointTest(unittest.TestCase):
    """Tests for the entrypoint argument parser."""

    def test_parser_defaults_to_no_tenant(self) -> None:
        parser = IdentityServiceExportEntrypoint.get_parser()
        self.assertIsNone(parser.parse_args([]).tenant)

    def test_parser_parses_tenant(self) -> None:
        parser = IdentityServiceExportEntrypoint.get_parser()
        self.assertEqual(Tenant.US_OZ, parser.parse_args(["--tenant", "US_OZ"]).tenant)
