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
"""Tests for the Identity Service querier."""
import datetime
import os
import unittest
import uuid
from typing import Any

import pytest
from sqlalchemy import event, text

from recidiviz.common.constants.identity import (
    AttributeType,
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    NameUse,
    PersonType,
    SplitTrigger,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity.exceptions import IdentityHistoryIntegrityException
from recidiviz.services.identity.querier import IdentityServiceQuerier
from recidiviz.services.identity.types import (
    AttributeConflict,
    Email,
    ExternalId,
    Identity,
    IdentityAttributes,
    IdentityHistory,
    IdentitySearchRequest,
    MergeEvent,
    Name,
    RetiredHandlingMode,
    SplitDestination,
    SplitEvent,
)
from recidiviz.tests.services.identity.test_utils import (
    CREATED,
    NEW_ID,
    RECIDIVIZ_ID,
    RETIRED_ID,
    insert_email,
    insert_external_id,
    insert_identity,
    insert_name,
    make_sourced_attribute,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.tools.services.identity import fixtures as identity_fixtures
from recidiviz.tools.utils.fixture_helpers import reset_fixtures

# The multi-hop resolution tests build on the CSV-fixture-seeded merge chain
# (RECIDIVIZ_ID ACTIVE <- RETIRED_ID RETIRED) by adding one more hop on top.
RETIRED_HEAD_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
UNKNOWN_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
CYCLED_ID = uuid.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
SPLIT_DESTINATION_ID = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
OTHER_TENANT_ID = uuid.UUID("99999999-9999-9999-9999-999999999999")


def _bare_identity() -> Identity:
    """Returns the seeded CSV-fixture identity (recidiviz_id=RECIDIVIZ_ID, no children)."""
    return Identity(
        recidiviz_id=RECIDIVIZ_ID,
        tenant=Tenant.US_OZ,
        person_type=PersonType.JII,
        status=IdentityStatus.ACTIVE,
        merged_into=None,
        last_cluster_hash=None,
        skip_demographic_guard=False,
        created_utc=CREATED,
        last_updated_utc=CREATED,
        external_ids=[],
        attributes=IdentityAttributes(
            names=[],
            dates_of_birth=[],
            genders=[],
            races=[],
            sexes=[],
            ethnicities=[],
            phone_numbers=[],
            emails=[],
        ),
    )


@pytest.mark.uses_db
class IdentityServiceQuerierTest(unittest.TestCase):
    """Tests for IdentityServiceQuerier session-management wiring."""

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

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_database_key_targets_identity_schema(self) -> None:
        querier = IdentityServiceQuerier()
        self.assertEqual(SchemaType.IDENTITY, querier.database_key.schema_type)

    def test_can_open_and_close_session(self) -> None:
        querier = IdentityServiceQuerier()
        with SessionFactory.using_database(querier.database_key) as session:
            self.assertEqual(1, session.execute(text("SELECT 1")).scalar())

    def test_get_identity_returns_seeded_row(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        self.assertEqual(
            _bare_identity(),
            IdentityServiceQuerier().get_identity(
                RECIDIVIZ_ID,
                resolve_retired=False,
            ),
        )

    def test_get_identity_loads_child_records(self) -> None:
        """An identity with external IDs and attributes comes back with each
        child collection attached, loaded via the eager relationships."""
        ts = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity, schema.ExternalId, schema.Name, schema.Email],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        identity = IdentityServiceQuerier().get_identity(
            RECIDIVIZ_ID, resolve_retired=False
        )

        assert identity is not None
        self.assertEqual(RECIDIVIZ_ID, identity.recidiviz_id)
        self.assertEqual(
            [
                ExternalId(
                    external_id="OZ123",
                    id_type=IdentifierType.US_OZ_KDS_PERSON_ID,
                    is_active=True,
                )
            ],
            identity.external_ids,
        )
        self.assertCountEqual(
            [
                make_sourced_attribute(
                    Name(
                        surname="Gale",
                        given_name="Dorothy",
                        middle_names=[],
                        name_suffix=None,
                        use=NameUse.OFFICIAL,
                    ),
                    last_updated_utc=ts,
                ),
                make_sourced_attribute(
                    Name(
                        surname="Gulch",
                        given_name="Dorothy",
                        middle_names=["Q"],
                        name_suffix=None,
                        use=NameUse.FORMER,
                    ),
                    last_updated_utc=ts,
                ),
            ],
            identity.attributes.names,
        )
        self.assertEqual(
            [
                make_sourced_attribute(
                    Email(address="dorothy@fake.com", address_hash="hash123"),
                    last_updated_utc=ts,
                )
            ],
            identity.attributes.emails,
        )
        self.assertEqual([], identity.attributes.dates_of_birth)
        self.assertEqual([], identity.attributes.genders)
        self.assertEqual([], identity.attributes.races)
        self.assertEqual([], identity.attributes.sexes)
        self.assertEqual([], identity.attributes.ethnicities)
        self.assertEqual([], identity.attributes.phone_numbers)

    def test_get_identity_history(self) -> None:
        """get_identity_history pairs the given identity with its merge and split
        audit events, decoding the JSONB attribute snapshots into typed values."""
        ts = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
        reset_fixtures(
            engine=self.engine,
            tables=[
                schema.Identity,
                schema.MergeEvent,
                schema.AttributeConflict,
                schema.SplitEvent,
                schema.SplitEventNewIdentity,
                schema.SplitEventMovedExternalId,
                schema.SplitEventMovedAttribute,
            ],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        identity = _bare_identity()

        self.assertEqual(
            IdentityHistory(
                identity=identity,
                merge_events=[
                    MergeEvent(
                        surviving_id=RECIDIVIZ_ID,
                        retired_id=RETIRED_ID,
                        trigger=MergeTrigger.MERGE_ENDPOINT,
                        requested_by="auditor@fake.com",
                        timestamp_utc=ts,
                        conflicts=[
                            AttributeConflict(
                                attribute_type=AttributeType.NAME,
                                retired_value=make_sourced_attribute(
                                    Name(
                                        surname="Gulch",
                                        given_name="Dorothy",
                                        middle_names=[],
                                        name_suffix=None,
                                        use=NameUse.FORMER,
                                    ),
                                    last_updated_utc=ts,
                                ),
                                surviving_value=make_sourced_attribute(
                                    Name(
                                        surname="Gale",
                                        given_name="Dorothy",
                                        middle_names=["Q"],
                                        name_suffix=None,
                                        use=NameUse.OFFICIAL,
                                    ),
                                    last_updated_utc=ts,
                                ),
                            )
                        ],
                    )
                ],
                split_events=[
                    SplitEvent(
                        original_id=RECIDIVIZ_ID,
                        trigger=SplitTrigger.SPLIT_ENDPOINT,
                        requested_by="auditor@fake.com",
                        timestamp_utc=ts,
                        destinations=[
                            SplitDestination(
                                new_recidiviz_id=NEW_ID,
                                external_ids=[
                                    ExternalId(
                                        external_id="OZ999",
                                        id_type=IdentifierType.US_OZ_LOTR_ID,
                                        is_active=True,
                                    )
                                ],
                                attributes=[
                                    make_sourced_attribute(
                                        Email(
                                            address="dot@fake.com",
                                            address_hash="hashdotfakecom",
                                        ),
                                        last_updated_utc=ts,
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            IdentityServiceQuerier().get_identity_history(identity),
        )

    def test_get_identity_active_returns_record(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        result = IdentityServiceQuerier().get_identity(
            RECIDIVIZ_ID, resolve_retired=True
        )

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_identity_resolves_single_retired_hop(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        result = IdentityServiceQuerier().get_identity(RETIRED_ID, resolve_retired=True)

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_identity_resolves_single_retired_hop_without_redundant_queries(
        self,
    ) -> None:
        """A single-hop retired resolution should query the `identities` table 3
        times total (the input row, the survivor's row, and the final full-row
        fetch) -- the same as a direct chain walk would need -- not 4. It must not
        re-fetch the input row's (status, merged_into) a second time just to
        satisfy the batch resolver's frontier query. (Queries against child tables
        like external_ids/emails/names, loaded via `selectin` relationships, are
        excluded -- they're unrelated to the chain-resolution logic under test.)"""
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        identities_table_selects: list[str] = []

        def record_select(
            _conn: Any,
            _cursor: Any,
            statement: str,
            _parameters: Any,
            _context: Any,
            _executemany: bool,
        ) -> None:
            if statement.lstrip().upper().startswith("SELECT") and (
                "FROM identities" in statement
            ):
                identities_table_selects.append(statement)

        event.listen(self.engine, "before_cursor_execute", record_select)
        try:
            result = IdentityServiceQuerier().get_identity(
                RETIRED_ID, resolve_retired=True
            )
        finally:
            event.remove(self.engine, "before_cursor_execute", record_select)

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(3, len(identities_table_selects))

    def test_get_identity_resolves_multi_hop_chain(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_identity(
            recidiviz_id=RETIRED_HEAD_ID,
            status=IdentityStatus.RETIRED,
            merged_into=RETIRED_ID,
        )

        result = IdentityServiceQuerier().get_identity(
            RETIRED_HEAD_ID, resolve_retired=True
        )

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_identity_returns_none_when_absent(self) -> None:
        self.assertIsNone(
            IdentityServiceQuerier().get_identity(UNKNOWN_ID, resolve_retired=True)
        )
        self.assertIsNone(
            IdentityServiceQuerier().get_identity(UNKNOWN_ID, resolve_retired=False)
        )

    def test_get_identity_literal_returns_retired_record(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        result = IdentityServiceQuerier().get_identity(
            RETIRED_ID, resolve_retired=False
        )

        assert result is not None
        self.assertEqual(RETIRED_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.RETIRED, result.status)
        self.assertEqual(RECIDIVIZ_ID, result.merged_into)

    def test_get_identity_raises_on_cycle(self) -> None:
        # Insert a self-referential cycle: CYCLED_ID's merged_into points to itself.
        # PostgreSQL allows this for self-referential FKs (the row satisfies its own
        # FK after insert). This is the kind of corrupt state we guard against.
        insert_identity(
            recidiviz_id=CYCLED_ID,
            status=IdentityStatus.RETIRED,
            merged_into=CYCLED_ID,
        )

        with self.assertRaisesRegex(
            IdentityHistoryIntegrityException,
            rf"^Cycle detected in merged_into chain starting from \[{CYCLED_ID}\]: "
            rf"revisited \[{CYCLED_ID}\]$",
        ):
            IdentityServiceQuerier().get_identity(CYCLED_ID, resolve_retired=True)


@pytest.mark.uses_db
class GetByExternalIdTest(unittest.TestCase):
    """Tests for IdentityServiceQuerier.get_by_external_id."""

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

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_get_by_external_id_active_returns_identity(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity, schema.ExternalId],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        result = IdentityServiceQuerier().get_by_external_id(
            "OZ123", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_external_id_resolves_single_retired_hop(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_external_id(recidiviz_id=RETIRED_ID, external_id="EXT456")

        result = IdentityServiceQuerier().get_by_external_id(
            "EXT456", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_external_id_resolves_multi_hop_chain(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_identity(
            recidiviz_id=RETIRED_HEAD_ID,
            status=IdentityStatus.RETIRED,
            merged_into=RETIRED_ID,
        )
        insert_external_id(recidiviz_id=RETIRED_HEAD_ID, external_id="EXT789")

        result = IdentityServiceQuerier().get_by_external_id(
            "EXT789", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_external_id_unknown_returns_none(self) -> None:
        self.assertIsNone(
            IdentityServiceQuerier().get_by_external_id(
                "UNKNOWN", IdentifierType.US_OZ_KDS_PERSON_ID
            )
        )

    def test_get_by_external_id_returns_split_destination(self) -> None:
        # After a split, the original identity keeps an inactive row for the moved
        # external_id and the destination identity gets a new active row. The lookup
        # must return the destination (the active owner of the external_id).
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_identity(recidiviz_id=SPLIT_DESTINATION_ID, status=IdentityStatus.ACTIVE)
        insert_external_id(
            recidiviz_id=RECIDIVIZ_ID, external_id="EXT123", is_active=False
        )
        insert_external_id(recidiviz_id=SPLIT_DESTINATION_ID, external_id="EXT123")

        result = IdentityServiceQuerier().get_by_external_id(
            "EXT123", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(SPLIT_DESTINATION_ID, result.recidiviz_id)


@pytest.mark.uses_db
class GetByEmailHashTest(unittest.TestCase):
    """Tests for IdentityServiceQuerier.get_by_email_hash."""

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

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_get_by_email_hash_filters_by_tenant(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_email(recidiviz_id=RECIDIVIZ_ID, address_hash="hash-abc")
        insert_identity(
            recidiviz_id=OTHER_TENANT_ID,
            status=IdentityStatus.ACTIVE,
            tenant=Tenant.US_XX,
        )
        insert_email(recidiviz_id=OTHER_TENANT_ID, address_hash="hash-abc")

        result = IdentityServiceQuerier().get_by_email_hash("hash-abc", Tenant.US_OZ)

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)

        result = IdentityServiceQuerier().get_by_email_hash("hash-abc", Tenant.US_XX)

        assert result is not None
        self.assertEqual(OTHER_TENANT_ID, result.recidiviz_id)

        result = IdentityServiceQuerier().get_by_email_hash("hash-abc", Tenant.US_YY)
        assert result is None

    def test_get_by_email_hash_active_returns_identity(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity, schema.Email],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        result = IdentityServiceQuerier().get_by_email_hash("hash123", Tenant.US_OZ)

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_email_hash_resolves_single_retired_hop(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_email(recidiviz_id=RETIRED_ID, address_hash="hash-abc")

        result = IdentityServiceQuerier().get_by_email_hash("hash-abc", Tenant.US_OZ)

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_email_hash_resolves_multi_hop_chain(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_identity(
            recidiviz_id=RETIRED_HEAD_ID,
            status=IdentityStatus.RETIRED,
            merged_into=RETIRED_ID,
        )
        insert_email(recidiviz_id=RETIRED_HEAD_ID, address_hash="hash-abc")

        result = IdentityServiceQuerier().get_by_email_hash("hash-abc", Tenant.US_OZ)

        assert result is not None
        self.assertEqual(RECIDIVIZ_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_email_hash_unknown_returns_none(self) -> None:
        self.assertIsNone(
            IdentityServiceQuerier().get_by_email_hash("no-such-hash", Tenant.US_OZ)
        )


# IDs chosen in ascending order so search's recidiviz_id ordering is predictable.
SEARCH_ID_1 = uuid.UUID("10000000-0000-0000-0000-000000000000")
SEARCH_ID_2 = uuid.UUID("20000000-0000-0000-0000-000000000000")
SEARCH_ID_3 = uuid.UUID("30000000-0000-0000-0000-000000000000")


@pytest.mark.uses_db
class SearchTest(unittest.TestCase):
    """Tests for IdentityServiceQuerier.search."""

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

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_search_by_name_substring_case_insensitive(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1)
        insert_identity(recidiviz_id=SEARCH_ID_2)
        insert_identity(recidiviz_id=SEARCH_ID_3)
        insert_name(recidiviz_id=SEARCH_ID_1, given_name="Frodo", surname="Baggins")
        insert_name(recidiviz_id=SEARCH_ID_2, given_name="Bilbo", surname="Baggins")
        insert_name(recidiviz_id=SEARCH_ID_3, given_name="Samwise", surname="Gamgee")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(name="bagg", limit=50)
        )

        self.assertCountEqual(
            [SEARCH_ID_1, SEARCH_ID_2], [i.recidiviz_id for i in result.results]
        )

    def test_search_by_name_matches_given_name(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1)
        insert_identity(recidiviz_id=SEARCH_ID_2)
        insert_name(recidiviz_id=SEARCH_ID_1, given_name="Frodo", surname="Baggins")
        insert_name(recidiviz_id=SEARCH_ID_2, given_name="Samwise", surname="Gamgee")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(name="rod", limit=50)
        )

        self.assertEqual([SEARCH_ID_1], [i.recidiviz_id for i in result.results])

    def test_search_by_tenant(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1, tenant=Tenant.US_OZ)
        insert_identity(recidiviz_id=SEARCH_ID_2, tenant=Tenant.RECIDIVIZ)

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(tenant=Tenant.RECIDIVIZ, limit=50)
        )

        self.assertEqual([SEARCH_ID_2], [i.recidiviz_id for i in result.results])

    def test_search_by_person_type(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1, person_type=PersonType.JII)
        insert_identity(recidiviz_id=SEARCH_ID_2, person_type=PersonType.STAFF)

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(person_type=PersonType.STAFF, limit=50)
        )

        self.assertEqual([SEARCH_ID_2], [i.recidiviz_id for i in result.results])

    def test_search_by_external_id_matches_regardless_of_id_type(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1)
        insert_identity(recidiviz_id=SEARCH_ID_2)
        insert_external_id(
            recidiviz_id=SEARCH_ID_1,
            external_id="SHARED123",
            id_type=IdentifierType.US_OZ_LOTR_ID,
        )
        insert_external_id(
            recidiviz_id=SEARCH_ID_2,
            external_id="OTHER456",
            id_type=IdentifierType.US_OZ_KDS_PERSON_ID,
        )

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(external_id="SHARED123", limit=50)
        )

        self.assertEqual([SEARCH_ID_1], [i.recidiviz_id for i in result.results])

    def test_search_by_external_id_ignores_inactive_rows(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1)
        insert_external_id(
            recidiviz_id=SEARCH_ID_1, external_id="OLD123", is_active=False
        )

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(external_id="OLD123", limit=50)
        )

        self.assertEqual([], result.results)

    def test_search_combines_filters_with_and(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1, tenant=Tenant.US_OZ)
        insert_identity(recidiviz_id=SEARCH_ID_2, tenant=Tenant.RECIDIVIZ)
        insert_name(recidiviz_id=SEARCH_ID_1, given_name="Frodo", surname="Baggins")
        insert_name(recidiviz_id=SEARCH_ID_2, given_name="Bilbo", surname="Baggins")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(name="bagg", tenant=Tenant.US_OZ, limit=50)
        )

        self.assertEqual([SEARCH_ID_1], [i.recidiviz_id for i in result.results])

    def test_search_returns_empty_list_when_no_matches(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1, tenant=Tenant.US_OZ)

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(tenant=Tenant.RECIDIVIZ, limit=50)
        )

        self.assertEqual([], result.results)
        self.assertIsNone(result.next_cursor)

    def test_search_default_excludes_retired(self) -> None:
        # The CSV fixtures seed an ACTIVE identity (RECIDIVIZ_ID) and a RETIRED
        # identity (RETIRED_ID) in the same tenant; the default mode returns
        # only the ACTIVE one.
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(tenant=Tenant.US_OZ, limit=50)
        )

        self.assertEqual([RECIDIVIZ_ID], [i.recidiviz_id for i in result.results])
        self.assertEqual(IdentityStatus.ACTIVE, result.results[0].status)

    def test_search_default_returns_empty_when_only_retired_matches(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_name(recidiviz_id=RETIRED_ID, given_name="Frodo", surname="Baggins")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(name="bagg", limit=50)
        )

        self.assertEqual([], result.results)
        self.assertIsNone(result.next_cursor)

    def test_search_resolves_active_identity(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(
                tenant=Tenant.US_OZ,
                limit=50,
                retired_handling=RetiredHandlingMode.RESOLVE,
            )
        )

        self.assertEqual([RECIDIVIZ_ID], [i.recidiviz_id for i in result.results])
        self.assertEqual(IdentityStatus.ACTIVE, result.results[0].status)

    def test_search_resolves_single_retired_hop(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_name(recidiviz_id=RETIRED_ID, given_name="Frodo", surname="Baggins")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(
                name="bagg", limit=50, retired_handling=RetiredHandlingMode.RESOLVE
            )
        )

        self.assertEqual([RECIDIVIZ_ID], [i.recidiviz_id for i in result.results])
        self.assertEqual(IdentityStatus.ACTIVE, result.results[0].status)

    def test_search_resolves_multi_hop_retired_chain(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_identity(
            recidiviz_id=RETIRED_HEAD_ID,
            status=IdentityStatus.RETIRED,
            merged_into=RETIRED_ID,
        )
        insert_name(recidiviz_id=RETIRED_HEAD_ID, given_name="Frodo", surname="Baggins")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(
                name="bagg", limit=50, retired_handling=RetiredHandlingMode.RESOLVE
            )
        )

        self.assertEqual([RECIDIVIZ_ID], [i.recidiviz_id for i in result.results])

    def test_search_as_stored_returns_retired_record_as_stored(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_name(recidiviz_id=RETIRED_ID, given_name="Frodo", surname="Baggins")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(
                name="bagg", limit=50, retired_handling=RetiredHandlingMode.AS_STORED
            )
        )

        self.assertEqual([RETIRED_ID], [i.recidiviz_id for i in result.results])
        self.assertEqual(IdentityStatus.RETIRED, result.results[0].status)

    def test_search_dedupes_retired_and_survivor_matching_same_page(self) -> None:
        # Both the survivor and its retired predecessor match the name filter;
        # resolving should collapse them to a single result.
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )
        insert_name(recidiviz_id=RECIDIVIZ_ID, given_name="Frodo", surname="Baggins")
        insert_name(recidiviz_id=RETIRED_ID, given_name="Frodo", surname="Baggins")

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(
                name="bagg", limit=50, retired_handling=RetiredHandlingMode.RESOLVE
            )
        )

        self.assertEqual([RECIDIVIZ_ID], [i.recidiviz_id for i in result.results])

    def test_search_resolution_batches_queries_across_page(self) -> None:
        """A page with multiple retired matches must resolve them with batched
        queries, not one chain walk + identity fetch per row. Expected SELECTs
        against identities: the page scan, one walk query per chain hop level
        (two here: the retired inputs, then their shared survivor), and one
        batched survivor fetch."""
        insert_identity(recidiviz_id=SEARCH_ID_3, tenant=Tenant.US_OZ)
        insert_identity(
            recidiviz_id=SEARCH_ID_1,
            tenant=Tenant.US_OZ,
            status=IdentityStatus.RETIRED,
            merged_into=SEARCH_ID_3,
        )
        insert_identity(
            recidiviz_id=SEARCH_ID_2,
            tenant=Tenant.US_OZ,
            status=IdentityStatus.RETIRED,
            merged_into=SEARCH_ID_3,
        )
        insert_name(recidiviz_id=SEARCH_ID_1, given_name="Frodo", surname="Baggins")
        insert_name(recidiviz_id=SEARCH_ID_2, given_name="Bilbo", surname="Baggins")

        identity_selects: list[str] = []

        def record_identity_select(
            _conn: Any,
            _cursor: Any,
            statement: str,
            _parameters: Any,
            _context: Any,
            _executemany: bool,
        ) -> None:
            if statement.lstrip().upper().startswith("SELECT") and (
                "FROM identities" in statement
            ):
                identity_selects.append(statement)

        event.listen(self.engine, "before_cursor_execute", record_identity_select)
        self.addCleanup(
            event.remove, self.engine, "before_cursor_execute", record_identity_select
        )

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(
                name="bagg", limit=50, retired_handling=RetiredHandlingMode.RESOLVE
            )
        )

        self.assertEqual([SEARCH_ID_3], [i.recidiviz_id for i in result.results])
        self.assertEqual(4, len(identity_selects))

    def test_search_raises_on_corrupt_retired_chain(self) -> None:
        # A retired match whose merged_into chain is corrupt (self-cycle) fails
        # the whole search loudly rather than being silently dropped.
        insert_identity(recidiviz_id=SEARCH_ID_3, tenant=Tenant.US_OZ)
        insert_identity(
            recidiviz_id=SEARCH_ID_1,
            tenant=Tenant.US_OZ,
            status=IdentityStatus.RETIRED,
            merged_into=SEARCH_ID_1,
        )
        insert_name(recidiviz_id=SEARCH_ID_1, given_name="Frodo", surname="Baggins")

        with self.assertRaisesRegex(
            IdentityHistoryIntegrityException,
            rf"^Cycle detected in merged_into chain starting from \[{SEARCH_ID_1}\]: "
            rf"revisited \[{SEARCH_ID_1}\]$",
        ):
            IdentityServiceQuerier().search(
                IdentitySearchRequest(
                    name="bagg", limit=50, retired_handling=RetiredHandlingMode.RESOLVE
                )
            )

    def test_search_paginates_with_limit_and_cursor(self) -> None:
        insert_identity(recidiviz_id=SEARCH_ID_1, tenant=Tenant.US_OZ)
        insert_identity(recidiviz_id=SEARCH_ID_2, tenant=Tenant.US_OZ)
        insert_identity(recidiviz_id=SEARCH_ID_3, tenant=Tenant.US_OZ)

        querier = IdentityServiceQuerier()
        first_page = querier.search(IdentitySearchRequest(tenant=Tenant.US_OZ, limit=2))

        self.assertEqual(
            [SEARCH_ID_1, SEARCH_ID_2], [i.recidiviz_id for i in first_page.results]
        )
        assert first_page.next_cursor is not None

        second_page = querier.search(
            IdentitySearchRequest(
                tenant=Tenant.US_OZ, limit=2, cursor=first_page.next_cursor
            )
        )

        self.assertEqual([SEARCH_ID_3], [i.recidiviz_id for i in second_page.results])
        self.assertIsNone(second_page.next_cursor)

    def test_search_as_stored_orders_by_recidiviz_id_regardless_of_status(
        self,
    ) -> None:
        # SEARCH_ID_1 is RETIRED but sorts first by recidiviz_id, so it comes
        # first: ordering is by recidiviz_id only, with no ACTIVE-first bias.
        # SEARCH_ID_3 (the merge target) must be inserted before SEARCH_ID_1 to
        # satisfy the merged_into foreign key.
        insert_identity(recidiviz_id=SEARCH_ID_3, tenant=Tenant.US_OZ)
        insert_identity(
            recidiviz_id=SEARCH_ID_1,
            tenant=Tenant.US_OZ,
            status=IdentityStatus.RETIRED,
            merged_into=SEARCH_ID_3,
        )
        insert_identity(recidiviz_id=SEARCH_ID_2, tenant=Tenant.US_OZ)

        result = IdentityServiceQuerier().search(
            IdentitySearchRequest(
                tenant=Tenant.US_OZ,
                limit=50,
                retired_handling=RetiredHandlingMode.AS_STORED,
            )
        )

        self.assertEqual(
            [SEARCH_ID_1, SEARCH_ID_2, SEARCH_ID_3],
            [i.recidiviz_id for i in result.results],
        )

    def test_search_as_stored_paginates_across_statuses(self) -> None:
        # Paginated with limit=1 across a RETIRED row followed by an ACTIVE row
        # (by recidiviz_id) to prove the cursor resumes the id-ordered scan
        # correctly. The merge target (SEARCH_ID_3) is a different tenant so it
        # doesn't itself match the search and change the expected page contents.
        insert_identity(recidiviz_id=SEARCH_ID_3, tenant=Tenant.RECIDIVIZ)
        insert_identity(
            recidiviz_id=SEARCH_ID_1,
            tenant=Tenant.US_OZ,
            status=IdentityStatus.RETIRED,
            merged_into=SEARCH_ID_3,
        )
        insert_identity(recidiviz_id=SEARCH_ID_2, tenant=Tenant.US_OZ)

        querier = IdentityServiceQuerier()
        first_page = querier.search(
            IdentitySearchRequest(
                tenant=Tenant.US_OZ,
                limit=1,
                retired_handling=RetiredHandlingMode.AS_STORED,
            )
        )

        self.assertEqual([SEARCH_ID_1], [i.recidiviz_id for i in first_page.results])
        assert first_page.next_cursor is not None

        second_page = querier.search(
            IdentitySearchRequest(
                tenant=Tenant.US_OZ,
                limit=1,
                cursor=first_page.next_cursor,
                retired_handling=RetiredHandlingMode.AS_STORED,
            )
        )

        self.assertEqual([SEARCH_ID_2], [i.recidiviz_id for i in second_page.results])
        self.assertIsNone(second_page.next_cursor)

    def test_search_invalid_cursor_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"^Invalid search cursor \[garbage\]$"):
            IdentityServiceQuerier().search(
                IdentitySearchRequest(tenant=Tenant.US_OZ, cursor="garbage", limit=50)
            )
