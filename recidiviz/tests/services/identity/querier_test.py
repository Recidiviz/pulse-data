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
from sqlalchemy.exc import IntegrityError

from recidiviz.common.constants.identity import (
    AttributeType,
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    NameUse,
    PersonType,
    SourceType,
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
    SourcedAttributeValue,
    SplitDestination,
    SplitEvent,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.tools.services.identity import fixtures as identity_fixtures
from recidiviz.tools.utils.fixture_helpers import reset_fixtures


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

    def test_get_all_identities_returns_seeded_row(self) -> None:
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        self.assertEqual(
            [
                Identity(
                    recidiviz_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                    tenant=Tenant.US_OZ,
                    person_type=PersonType.JII,
                    status=IdentityStatus.ACTIVE,
                    merged_into=None,
                    last_cluster_hash=None,
                    skip_demographic_guard=False,
                    created_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    last_updated_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
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
            ],
            IdentityServiceQuerier().get_all_identities(),
        )

    def test_get_all_identities_loads_child_records(self) -> None:
        """An identity with external IDs and attributes comes back with each
        child collection attached, loaded via the eager relationships."""
        recidiviz_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
        ts = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
        reset_fixtures(
            engine=self.engine,
            tables=[schema.Identity, schema.ExternalId, schema.Name, schema.Email],
            fixture_directory=os.path.dirname(identity_fixtures.__file__),
            csv_headers=True,
        )

        [identity] = IdentityServiceQuerier().get_all_identities()

        self.assertEqual(recidiviz_id, identity.recidiviz_id)
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
                SourcedAttributeValue(
                    value=Name(
                        surname="Gale",
                        given_name="Dorothy",
                        middle_names=[],
                        name_suffix=None,
                        use=NameUse.OFFICIAL,
                    ),
                    source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                    source_product_app=None,
                    last_updated_utc=ts,
                ),
                SourcedAttributeValue(
                    value=Name(
                        surname="Gulch",
                        given_name="Dorothy",
                        middle_names=["Q"],
                        name_suffix=None,
                        use=NameUse.FORMER,
                    ),
                    source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                    source_product_app=None,
                    last_updated_utc=ts,
                ),
            ],
            identity.attributes.names,
        )
        self.assertEqual(
            [
                SourcedAttributeValue(
                    value=Email(address="dorothy@fake.com", address_hash="hash123"),
                    source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                    source_product_app=None,
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

    def test_child_without_parent_identity_raises(self) -> None:
        """A child row whose recidiviz_id has no parent Identity must fail with a
        foreign-key violation rather than the ORM auto-creating an Identity."""
        with self.assertRaises(IntegrityError):
            with SessionFactory.using_database(self.database_key) as session:
                session.add(
                    schema.Name(
                        recidiviz_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
                        surname="Nobody",
                        given_name=None,
                        middle_names=[],
                        name_suffix=None,
                        use=None,
                        source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                        source_product_app=None,
                        last_updated_utc=datetime.datetime(
                            2026, 2, 2, tzinfo=datetime.timezone.utc
                        ),
                    )
                )

    def test_get_identity_history(self) -> None:
        """get_identity_history pairs the identity with its merge and split audit
        events, decoding the JSONB attribute snapshots into typed values."""
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

        def sourced_name(
            surname: str, use: NameUse, middle_names: list[str]
        ) -> SourcedAttributeValue:
            return SourcedAttributeValue(
                value=Name(
                    surname=surname,
                    given_name="Dorothy",
                    middle_names=middle_names,
                    name_suffix=None,
                    use=use,
                ),
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=ts,
            )

        self.assertEqual(
            IdentityHistory(
                identity=Identity(
                    recidiviz_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                    tenant=Tenant.US_OZ,
                    person_type=PersonType.JII,
                    status=IdentityStatus.ACTIVE,
                    merged_into=None,
                    last_cluster_hash=None,
                    skip_demographic_guard=False,
                    created_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    last_updated_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
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
                ),
                merge_events=[
                    MergeEvent(
                        timestamp_utc=ts,
                        trigger=MergeTrigger.MERGE_ENDPOINT,
                        requested_by="auditor@fake.com",
                        retired_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
                        surviving_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                        conflicts=[
                            AttributeConflict(
                                attribute_type=AttributeType.NAME,
                                retired_value=sourced_name("Gulch", NameUse.FORMER, []),
                                surviving_value=sourced_name(
                                    "Gale", NameUse.OFFICIAL, ["Q"]
                                ),
                            )
                        ],
                    )
                ],
                split_events=[
                    SplitEvent(
                        timestamp_utc=ts,
                        trigger=SplitTrigger.SPLIT_ENDPOINT,
                        original_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                        requested_by="auditor@fake.com",
                        destinations=[
                            SplitDestination(
                                new_recidiviz_id=uuid.UUID(
                                    "33333333-3333-3333-3333-333333333333"
                                ),
                                external_ids=[
                                    ExternalId(
                                        external_id="OZ999",
                                        id_type=IdentifierType.US_OZ_LOTR_ID,
                                        is_active=True,
                                    )
                                ],
                                attributes=[
                                    SourcedAttributeValue(
                                        value=Email(
                                            address="dot@fake.com",
                                            address_hash="hashdotfakecom",
                                        ),
                                        source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                                        source_product_app=None,
                                        last_updated_utc=ts,
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            IdentityServiceQuerier().get_identity_history(
                uuid.UUID("11111111-1111-1111-1111-111111111111")
            ),
        )

    def test_get_identity_history_returns_none_when_absent(self) -> None:
        self.assertIsNone(
            IdentityServiceQuerier().get_identity_history(
                uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
            )
        )
