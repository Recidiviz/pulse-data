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

import pytest
from sqlalchemy import text

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
from recidiviz.services.identity.querier import IdentityServiceQuerier
from recidiviz.services.identity.types import (
    AttributeConflict,
    Email,
    ExternalId,
    Identity,
    IdentityAttributes,
    IdentityHistory,
    MergeEvent,
    Name,
    SplitDestination,
    SplitEvent,
)
from recidiviz.tests.services.identity.test_utils import (
    CREATED,
    NEW_ID,
    RECIDIVIZ_ID,
    RETIRED_ID,
    make_sourced_attribute,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.tools.services.identity import fixtures as identity_fixtures
from recidiviz.tools.utils.fixture_helpers import reset_fixtures

RESOLUTION_TS = datetime.datetime(2026, 3, 1, 12, 0, tzinfo=datetime.timezone.utc)
# Merge chain used by the resolution tests: HEAD -> MIDDLE -> SURVIVOR.
ACTIVE_SURVIVOR_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
RETIRED_MIDDLE_ID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
RETIRED_HEAD_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
UNKNOWN_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
CYCLED_ID = uuid.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
SPLIT_DESTINATION_ID = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")


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

    def _insert_identity(
        self,
        recidiviz_id: uuid.UUID,
        status: IdentityStatus,
        merged_into: uuid.UUID | None = None,
    ) -> None:
        """Inserts a bare Identity row (no attributes / external IDs) for the
        merge-chain resolution tests."""
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.Identity(
                    recidiviz_id=recidiviz_id,
                    created_utc=RESOLUTION_TS,
                    last_updated_utc=RESOLUTION_TS,
                    tenant=Tenant.US_OZ,
                    person_type=PersonType.JII,
                    status=status,
                    merged_into=merged_into,
                )
            )

    def test_get_identity_active_returns_record(self) -> None:
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)

        result = IdentityServiceQuerier().get_identity(
            ACTIVE_SURVIVOR_ID, resolve_retired=True
        )

        assert result is not None
        self.assertEqual(ACTIVE_SURVIVOR_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_identity_resolves_single_retired_hop(self) -> None:
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)
        self._insert_identity(
            RETIRED_MIDDLE_ID, IdentityStatus.RETIRED, merged_into=ACTIVE_SURVIVOR_ID
        )

        result = IdentityServiceQuerier().get_identity(
            RETIRED_MIDDLE_ID, resolve_retired=True
        )

        assert result is not None
        self.assertEqual(ACTIVE_SURVIVOR_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_identity_resolves_multi_hop_chain(self) -> None:
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)
        self._insert_identity(
            RETIRED_MIDDLE_ID, IdentityStatus.RETIRED, merged_into=ACTIVE_SURVIVOR_ID
        )
        self._insert_identity(
            RETIRED_HEAD_ID, IdentityStatus.RETIRED, merged_into=RETIRED_MIDDLE_ID
        )

        result = IdentityServiceQuerier().get_identity(
            RETIRED_HEAD_ID, resolve_retired=True
        )

        assert result is not None
        self.assertEqual(ACTIVE_SURVIVOR_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_identity_returns_none_when_absent(self) -> None:
        self.assertIsNone(
            IdentityServiceQuerier().get_identity(UNKNOWN_ID, resolve_retired=True)
        )
        self.assertIsNone(
            IdentityServiceQuerier().get_identity(UNKNOWN_ID, resolve_retired=False)
        )

    def test_get_identity_literal_returns_retired_record(self) -> None:
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)
        self._insert_identity(
            RETIRED_MIDDLE_ID, IdentityStatus.RETIRED, merged_into=ACTIVE_SURVIVOR_ID
        )

        result = IdentityServiceQuerier().get_identity(
            RETIRED_MIDDLE_ID, resolve_retired=False
        )

        assert result is not None
        self.assertEqual(RETIRED_MIDDLE_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.RETIRED, result.status)
        self.assertEqual(ACTIVE_SURVIVOR_ID, result.merged_into)

    def test_get_identity_raises_on_cycle(self) -> None:
        # Insert a self-referential cycle: CYCLED_ID's merged_into points to itself.
        # PostgreSQL allows this for self-referential FKs (the row satisfies its own
        # FK after insert). This is the kind of corrupt state we guard against.
        self._insert_identity(CYCLED_ID, IdentityStatus.RETIRED, merged_into=CYCLED_ID)

        with self.assertRaisesRegex(
            ValueError,
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

    def _insert_identity(
        self,
        recidiviz_id: uuid.UUID,
        status: IdentityStatus,
        merged_into: uuid.UUID | None = None,
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.Identity(
                    recidiviz_id=recidiviz_id,
                    created_utc=RESOLUTION_TS,
                    last_updated_utc=RESOLUTION_TS,
                    tenant=Tenant.US_OZ,
                    person_type=PersonType.JII,
                    status=status,
                    merged_into=merged_into,
                )
            )

    def _insert_external_id(
        self,
        recidiviz_id: uuid.UUID,
        external_id: str,
        id_type: IdentifierType,
        *,
        is_active: bool = True,
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.ExternalId(
                    recidiviz_id=recidiviz_id,
                    external_id=external_id,
                    id_type=id_type,
                    is_active=is_active,
                )
            )

    def test_get_by_external_id_active_returns_identity(self) -> None:
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)
        self._insert_external_id(
            ACTIVE_SURVIVOR_ID, "EXT123", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        result = IdentityServiceQuerier().get_by_external_id(
            "EXT123", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(ACTIVE_SURVIVOR_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_external_id_resolves_single_retired_hop(self) -> None:
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)
        self._insert_identity(
            RETIRED_MIDDLE_ID, IdentityStatus.RETIRED, merged_into=ACTIVE_SURVIVOR_ID
        )
        self._insert_external_id(
            RETIRED_MIDDLE_ID, "EXT456", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        result = IdentityServiceQuerier().get_by_external_id(
            "EXT456", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(ACTIVE_SURVIVOR_ID, result.recidiviz_id)
        self.assertEqual(IdentityStatus.ACTIVE, result.status)

    def test_get_by_external_id_resolves_multi_hop_chain(self) -> None:
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)
        self._insert_identity(
            RETIRED_MIDDLE_ID, IdentityStatus.RETIRED, merged_into=ACTIVE_SURVIVOR_ID
        )
        self._insert_identity(
            RETIRED_HEAD_ID, IdentityStatus.RETIRED, merged_into=RETIRED_MIDDLE_ID
        )
        self._insert_external_id(
            RETIRED_HEAD_ID, "EXT789", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        result = IdentityServiceQuerier().get_by_external_id(
            "EXT789", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(ACTIVE_SURVIVOR_ID, result.recidiviz_id)
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
        self._insert_identity(ACTIVE_SURVIVOR_ID, IdentityStatus.ACTIVE)
        self._insert_identity(SPLIT_DESTINATION_ID, IdentityStatus.ACTIVE)
        self._insert_external_id(
            ACTIVE_SURVIVOR_ID,
            "EXT123",
            IdentifierType.US_OZ_KDS_PERSON_ID,
            is_active=False,
        )
        self._insert_external_id(
            SPLIT_DESTINATION_ID, "EXT123", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        result = IdentityServiceQuerier().get_by_external_id(
            "EXT123", IdentifierType.US_OZ_KDS_PERSON_ID
        )

        assert result is not None
        self.assertEqual(SPLIT_DESTINATION_ID, result.recidiviz_id)
