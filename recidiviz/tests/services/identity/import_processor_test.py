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
"""Tests for processing Identity Service import work."""
import datetime
import uuid
from unittest import TestCase
from unittest.mock import patch

import pytest

from recidiviz.common.constants.identity import (
    IdentifierType,
    IdentityStatus,
    PersonType,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterEmail,
    IdentityClusterExternalId,
    IdentityClusterName,
)
from recidiviz.services.identity.bq_snapshot_reader import ClusterSnapshot
from recidiviz.services.identity.import_processor import (
    _IDENTITY_CHILD_TABLES,
    process_import,
)
from recidiviz.tests.services.identity.test_utils import (
    insert_email,
    insert_external_id,
    insert_identity,
    insert_name,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.user_hash import generate_user_hash

_SNAPSHOT = datetime.datetime(2026, 8, 8, tzinfo=datetime.timezone.utc)
_ID_TYPE = IdentifierType.US_OZ_LOTR_ID
_PROCESSOR_MODULE = "recidiviz.services.identity.import_processor"


def _named_snapshot(
    *,
    external_id: str,
    surname: str,
    tenant: Tenant = Tenant.US_OZ,
    email: str | None = None,
) -> ClusterSnapshot:
    """Builds a ClusterSnapshot whose cluster carries one external id and a name,
    so tests can recognize the identity and its child rows after an import."""
    cluster = IdentityCluster(
        tenant=tenant,
        person_type=PersonType.JII,
        external_ids=(
            IdentityClusterExternalId(
                tenant=tenant, external_id=external_id, id_type=_ID_TYPE.value
            ),
        ),
        name=IdentityClusterName(tenant=tenant, given_name="Ada", surname=surname),
        emails=(
            (IdentityClusterEmail(tenant=tenant, address=email),)
            if email is not None
            else ()
        ),
    )
    return ClusterSnapshot(
        cluster=cluster,
        stored_cluster_hash=cluster.cluster_hash,
    )


@pytest.mark.uses_db
class ImportProcessorTestCase(TestCase):
    """Shared Postgres setup for process_import tests. Only the BigQuery
    snapshot read is mocked; the clear and the create pass run for real
    against the test Postgres."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)
        self.engine = local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )
        self.read_patcher = patch(f"{_PROCESSOR_MODULE}.read_cluster_snapshot")
        self.mock_read = self.read_patcher.start()
        self.addCleanup(self.read_patcher.stop)

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def _run(self, *snapshots: ClusterSnapshot, should_clear_first: bool) -> None:
        self.mock_read.return_value = list(snapshots)
        process_import(
            tenant=Tenant.US_OZ,
            snapshot_timestamp=_SNAPSHOT,
            should_clear_first=should_clear_first,
        )

    def _surnames_by_tenant(self) -> dict[str, list[str]]:
        """Returns each tenant's identity name surnames, read straight from the
        schema tables."""
        with SessionFactory.using_database(self.database_key) as session:
            rows = (
                session.query(schema.Identity.tenant, schema.Name.surname)
                .join(
                    schema.Name,
                    schema.Name.recidiviz_id == schema.Identity.recidiviz_id,
                )
                .order_by(schema.Name.id)
                .all()
            )
        surnames_by_tenant: dict[str, list[str]] = {}
        for tenant, surname in rows:
            surnames_by_tenant.setdefault(tenant.value, []).append(surname)
        return surnames_by_tenant

    def _email_count(self) -> int:
        with SessionFactory.using_database(self.database_key) as session:
            return session.query(schema.Email).count()


@pytest.mark.uses_db
class ClearFirstImportTest(ImportProcessorTestCase):
    """Tests for process_import's clear-first mode: the run clears the tenant's
    identities and the create pass rebuilds them from the snapshot."""

    def test_rerunning_an_import_replaces_the_tenants_identities(self) -> None:
        self._run(
            _named_snapshot(external_id="A1", surname="First"),
            should_clear_first=True,
        )
        self.assertEqual({"US_OZ": ["First"]}, self._surnames_by_tenant())

        self._run(
            _named_snapshot(external_id="B2", surname="Second"),
            should_clear_first=True,
        )
        self.assertEqual({"US_OZ": ["Second"]}, self._surnames_by_tenant())

    def test_other_tenants_identities_survive_the_clear(self) -> None:
        other_tenant_id = uuid.uuid4()
        insert_identity(recidiviz_id=other_tenant_id, tenant=Tenant.US_XX)
        insert_name(
            recidiviz_id=other_tenant_id, given_name="Grace", surname="Untouched"
        )

        self._run(
            _named_snapshot(external_id="A1", surname="Imported"),
            should_clear_first=True,
        )

        self.assertEqual(
            {"US_OZ": ["Imported"], "US_XX": ["Untouched"]},
            self._surnames_by_tenant(),
        )

    def test_failing_cluster_leaves_no_partial_rows_and_rest_proceed(self) -> None:
        # The second cluster reuses the first cluster's external id, so writing all
        # three in one chunk violates the unique active (external_id, id_type) index
        # and the chunk rolls back in full. The per-cluster retry then commits the
        # first and third and isolates the duplicate second, which fails alone.
        self._run(
            _named_snapshot(external_id="A1", surname="First"),
            _named_snapshot(external_id="A1", surname="Partial"),
            _named_snapshot(external_id="C3", surname="Third"),
            should_clear_first=True,
        )

        self.assertEqual({"US_OZ": ["First", "Third"]}, self._surnames_by_tenant())

    def test_all_clusters_land_when_the_run_spans_multiple_chunks(self) -> None:
        # A chunk size of two against five clusters forces three commits; every
        # cluster must still land.
        surnames = ["Alpha", "Bravo", "Charlie", "Delta", "Echo"]
        snapshots = [
            _named_snapshot(external_id=f"E{i}", surname=surname)
            for i, surname in enumerate(surnames)
        ]
        with patch(f"{_PROCESSOR_MODULE}._IMPORT_CHUNK_SIZE", 2):
            self._run(*snapshots, should_clear_first=True)

        self.assertEqual({"US_OZ": surnames}, self._surnames_by_tenant())

    def test_email_shared_across_clusters_is_written_once(self) -> None:
        self._run(
            _named_snapshot(external_id="A1", surname="First", email="dup@fake.com"),
            _named_snapshot(external_id="B2", surname="Second", email="dup@fake.com"),
            should_clear_first=True,
        )

        self.assertEqual({"US_OZ": ["First", "Second"]}, self._surnames_by_tenant())
        # Both identities land, but the shared address is attached only once.
        self.assertEqual(1, self._email_count())


@pytest.mark.uses_db
class ImportWithoutClearFirstTest(ImportProcessorTestCase):
    """Tests for process_import without clear-first: existing identities are
    left untouched and only clusters matching no existing identity are created.

    TODO(OBT-37725): The skipped-cluster cases below change once the update
    pass (and then merge detection) processes those clusters instead of
    skipping them.
    """

    def test_existing_identities_survive_and_new_cluster_is_created(self) -> None:
        existing_id = uuid.uuid4()
        insert_identity(recidiviz_id=existing_id)
        insert_name(recidiviz_id=existing_id, given_name="Grace", surname="Untouched")
        insert_external_id(recidiviz_id=existing_id, external_id="A1", id_type=_ID_TYPE)

        self._run(
            _named_snapshot(external_id="B2", surname="Imported"),
            should_clear_first=False,
        )

        self.assertEqual(
            {"US_OZ": ["Untouched", "Imported"]}, self._surnames_by_tenant()
        )

    def test_cluster_matching_an_existing_identity_is_skipped(self) -> None:
        existing_id = uuid.uuid4()
        insert_identity(recidiviz_id=existing_id)
        insert_name(recidiviz_id=existing_id, given_name="Grace", surname="Original")
        insert_external_id(recidiviz_id=existing_id, external_id="A1", id_type=_ID_TYPE)

        self._run(
            _named_snapshot(external_id="A1", surname="Changed"),
            should_clear_first=False,
        )

        # The cluster maps to the existing identity, so no identity is created
        # and the existing one keeps its attributes.
        self.assertEqual({"US_OZ": ["Original"]}, self._surnames_by_tenant())

    def test_cluster_spanning_two_existing_identities_is_skipped(self) -> None:
        first_id = uuid.uuid4()
        insert_identity(recidiviz_id=first_id)
        insert_name(recidiviz_id=first_id, given_name="Ada", surname="First")
        insert_external_id(recidiviz_id=first_id, external_id="A1", id_type=_ID_TYPE)
        second_id = uuid.uuid4()
        insert_identity(recidiviz_id=second_id)
        insert_name(recidiviz_id=second_id, given_name="Ada", surname="Second")
        insert_external_id(recidiviz_id=second_id, external_id="B2", id_type=_ID_TYPE)

        spanning_cluster = IdentityCluster(
            tenant=Tenant.US_OZ,
            person_type=PersonType.JII,
            external_ids=(
                IdentityClusterExternalId(
                    tenant=Tenant.US_OZ, external_id="A1", id_type=_ID_TYPE.value
                ),
                IdentityClusterExternalId(
                    tenant=Tenant.US_OZ, external_id="B2", id_type=_ID_TYPE.value
                ),
            ),
            name=IdentityClusterName(
                tenant=Tenant.US_OZ, given_name="Ada", surname="Spanning"
            ),
        )
        self._run(
            ClusterSnapshot(
                cluster=spanning_cluster,
                stored_cluster_hash=spanning_cluster.cluster_hash,
            ),
            should_clear_first=False,
        )

        self.assertEqual({"US_OZ": ["First", "Second"]}, self._surnames_by_tenant())

    def test_retired_identitys_external_id_does_not_block_creation(self) -> None:
        # A RETIRED identity must point at the identity it was merged into, and
        # its external id row is inactive (the unique index on active external
        # ids allows only one active holder).
        survivor_id = uuid.uuid4()
        insert_identity(recidiviz_id=survivor_id)
        retired_id = uuid.uuid4()
        insert_identity(
            recidiviz_id=retired_id,
            status=IdentityStatus.RETIRED,
            merged_into=survivor_id,
        )
        insert_name(recidiviz_id=retired_id, given_name="Grace", surname="Retired")
        insert_external_id(
            recidiviz_id=retired_id,
            external_id="A1",
            id_type=_ID_TYPE,
            is_active=False,
        )

        self._run(
            _named_snapshot(external_id="A1", surname="Fresh"),
            should_clear_first=False,
        )

        # The retired identity's hold on A1 does not count as an existing
        # association, so the cluster is treated as new.
        self.assertEqual({"US_OZ": ["Retired", "Fresh"]}, self._surnames_by_tenant())

    def test_inactive_external_id_on_an_active_identity_does_not_block_creation(
        self,
    ) -> None:
        # An external id row left inactive by a split stays on its identity for
        # audit history but no longer associates the id with it for lookups.
        existing_id = uuid.uuid4()
        insert_identity(recidiviz_id=existing_id)
        insert_name(
            recidiviz_id=existing_id, given_name="Grace", surname="SplitResidue"
        )
        insert_external_id(
            recidiviz_id=existing_id,
            external_id="A1",
            id_type=_ID_TYPE,
            is_active=False,
        )

        self._run(
            _named_snapshot(external_id="A1", surname="Fresh"),
            should_clear_first=False,
        )

        self.assertEqual(
            {"US_OZ": ["SplitResidue", "Fresh"]}, self._surnames_by_tenant()
        )

    def test_cluster_with_unrecognized_id_type_is_skipped_and_rest_proceed(
        self,
    ) -> None:
        # An unrecognized id_type matches no existing identity in the filter, so
        # the cluster reaches the create pass, which fails it in isolation
        # rather than aborting the run.
        bad_cluster = IdentityCluster(
            tenant=Tenant.US_OZ,
            person_type=PersonType.JII,
            external_ids=(
                IdentityClusterExternalId(
                    tenant=Tenant.US_OZ,
                    external_id="A1",
                    id_type="NOT_A_REAL_ID_TYPE",
                ),
            ),
            name=IdentityClusterName(
                tenant=Tenant.US_OZ, given_name="Ada", surname="Bad"
            ),
        )

        self._run(
            ClusterSnapshot(
                cluster=bad_cluster, stored_cluster_hash=bad_cluster.cluster_hash
            ),
            _named_snapshot(external_id="B2", surname="Good"),
            should_clear_first=False,
        )

        self.assertEqual({"US_OZ": ["Good"]}, self._surnames_by_tenant())

    def test_email_owned_by_an_existing_identity_is_excluded(self) -> None:
        existing_id = uuid.uuid4()
        insert_identity(recidiviz_id=existing_id)
        insert_name(recidiviz_id=existing_id, given_name="Grace", surname="Owner")
        insert_external_id(recidiviz_id=existing_id, external_id="A1", id_type=_ID_TYPE)
        insert_email(
            recidiviz_id=existing_id, address_hash=generate_user_hash("dup@fake.com")
        )

        self._run(
            _named_snapshot(external_id="B2", surname="Imported", email="dup@fake.com"),
            should_clear_first=False,
        )

        # The new identity lands, but the address stays attached only to its
        # existing owner.
        self.assertEqual({"US_OZ": ["Owner", "Imported"]}, self._surnames_by_tenant())
        self.assertEqual(1, self._email_count())


class ClearTenantChildTableCompletenessTest(TestCase):
    """Guards that _clear_tenant_identities clears every table referencing an identity.

    _clear_tenant_identities deletes the tables in _IDENTITY_CHILD_TABLES before the
    identities rows, because those child tables FK identities.recidiviz_id without
    ON DELETE CASCADE. Every table that FKs identities.recidiviz_id must therefore be
    in _IDENTITY_CHILD_TABLES, or be listed here as deliberately left untouched by
    the clear-first import (it stays empty until the pass that populates it exists).
    Without this guard, a new identity child table added later would silently
    FK-fail the identity delete in production instead of failing here."""

    # Tables that FK identities.recidiviz_id but are intentionally NOT cleared by the
    # clear-first import: the candidate and no-merge tables the create pass never
    # writes. Each should join _IDENTITY_CHILD_TABLES once the pass that populates it
    # lands.
    _CLEAR_EXEMPT_TABLES = {
        "update_attribute_candidates",  # TODO(OBT-37725): clear once the update pass writes it.
        "merge_candidate_identities",  # TODO(OBT-37726): clear once merge detection writes it.
        "split_candidates",  # TODO(OBT-37728): clear once split detection writes it.
        "no_merge",  # TODO(OBT-37726): clear once merge detection writes it.
    }

    def _tables_referencing_identities(self) -> set[str]:
        """Returns the names of all tables (other than identities itself) with a
        foreign key to identities.recidiviz_id."""
        identities_table = schema.Identity.__tablename__
        referencing = set()
        for table in schema.IdentityBase.metadata.tables.values():
            if table.name == identities_table:
                # Skip the identities table's own merged_into self-reference.
                continue
            for foreign_key in table.foreign_keys:
                if (
                    foreign_key.column.table.name == identities_table
                    and foreign_key.column.name == "recidiviz_id"
                ):
                    referencing.add(table.name)
        return referencing

    def test_every_identity_child_table_is_cleared_or_exempted(self) -> None:
        cleared_tables = {table.__tablename__ for table in _IDENTITY_CHILD_TABLES}
        referencing_tables = self._tables_referencing_identities()

        unaccounted = referencing_tables - cleared_tables - self._CLEAR_EXEMPT_TABLES
        self.assertEqual(
            set(),
            unaccounted,
            f"Tables {sorted(unaccounted)} reference identities.recidiviz_id but are "
            f"neither cleared by _clear_tenant_identities (add them to "
            f"_IDENTITY_CHILD_TABLES) nor exempted in this test. A table that FKs "
            f"identities without being cleared will FK-fail the identity delete.",
        )

    def test_clear_exemptions_still_reference_identities(self) -> None:
        # Keep the exemption list honest: an exempted table that no longer FKs
        # identities (renamed or dropped) should be removed from _CLEAR_EXEMPT_TABLES.
        stale = self._CLEAR_EXEMPT_TABLES - self._tables_referencing_identities()
        self.assertEqual(
            set(),
            stale,
            f"Exempted tables {sorted(stale)} no longer FK identities.recidiviz_id; "
            f"remove them from _CLEAR_EXEMPT_TABLES.",
        )
