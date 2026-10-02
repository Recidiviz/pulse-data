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
"""Test helpers for the Identity Service."""
import datetime
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Iterator
from unittest import TestCase
from unittest.mock import patch

from recidiviz.common import demographics
from recidiviz.common.constants.identity import (
    AttributeType,
    CandidateStatus,
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    NameUse,
    PersonType,
    PhoneType,
    ProductApp,
    SourceType,
    SplitBlocker,
    SplitTrigger,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity import types
from recidiviz.services.identity.authorization import CallerRole, IdentityServiceCaller
from recidiviz.services.identity.constants import (
    DEV_EDITOR_SERVICE_ACCOUNT,
    DEV_IMPORTER_SERVICE_ACCOUNT,
    DEV_READER_SERVICE_ACCOUNT,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

TEST_CALLER_BY_SERVICE_ACCOUNT: dict[str, IdentityServiceCaller] = {
    DEV_READER_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.READER, source_product_app=None
    ),
    DEV_EDITOR_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.EDITOR, source_product_app=ProductApp.ADMIN_PANEL
    ),
    DEV_IMPORTER_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.IMPORTER, source_product_app=None
    ),
}

STRANGER_SERVICE_ACCOUNT = "stranger_service_account"
READER_SERVICE_ACCOUNT = "reader_service_account"
EDITOR_SERVICE_ACCOUNT = "editor_service_account"
IMPORTER_SERVICE_ACCOUNT = "importer_service_account"
DEFAULT_MAPPING = {
    READER_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.READER, source_product_app=None
    ),
    EDITOR_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.EDITOR, source_product_app=ProductApp.ADMIN_PANEL
    ),
    IMPORTER_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.IMPORTER, source_product_app=None
    ),
}


@contextmanager
def mock_iap_environment(
    mapping: Mapping[str, IdentityServiceCaller] | None = None,
    authenticated_as: str | None = None,
    invalid_jwt: bool = False,
    in_development: bool = False,
) -> Iterator[None]:
    """Patch the Identity Service IAP auth dependencies for the duration of the block.

    `mapping` replaces the static caller-to-authorization-config mapping.
    `authenticated_as` makes the JWT validator report that email as the
    authenticated caller. `invalid_jwt` makes the validator reject any header.
    `in_development` activates the decorator's dev bypass (no JWT validation),
    letting each request select a dev caller via the dev caller header.
    """
    if authenticated_as is not None and invalid_jwt:
        raise ValueError("authenticated_as and invalid_jwt are mutually exclusive")

    if invalid_jwt:
        jwt_return: tuple[str | None, str | None, str | None] = (
            None,
            None,
            "INVALID TOKEN",
        )
    else:
        jwt_return = ("sub-id", authenticated_as, None)

    with patch(
        "recidiviz.utils.auth.gce.in_development",
        return_value=in_development,
    ), patch(
        "recidiviz.services.identity.server.in_development",
        return_value=in_development,
    ), patch(
        "recidiviz.utils.metadata.project_number",
        return_value="123456789",
    ), patch(
        "recidiviz.utils.auth.gce.get_secret",
        return_value="987654321",
    ), patch(
        "recidiviz.utils.validate_jwt.validate_iap_jwt_from_compute_engine",
        return_value=jwt_return,
    ), patch.dict(
        "recidiviz.services.identity.authorization.CALLER_BY_SERVICE_ACCOUNT",
        mapping or TEST_CALLER_BY_SERVICE_ACCOUNT,
        clear=True,
    ):
        yield


# ---------------------------------------------------------------------------
# Domain object builders shared across the identity service test modules.
# ---------------------------------------------------------------------------

AttributeValue = (
    types.Name
    | types.DateOfBirth
    | types.Gender
    | types.Race
    | types.Sex
    | types.Ethnicity
    | types.PhoneNumber
    | types.Email
)

RECIDIVIZ_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
RETIRED_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
NEW_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
CREATED = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
UPDATED = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


def make_sourced_attribute(
    value: AttributeValue,
    *,
    source_type: SourceType = SourceType.EXTERNAL_DATA_SYSTEM,
    source_product_app: ProductApp | None = None,
    last_updated_utc: datetime.datetime = UPDATED,
) -> types.SourcedAttributeValue:
    """Wrap a value type in a SourcedAttributeValue with sensible defaults."""
    return types.SourcedAttributeValue(
        value=value,
        source_type=source_type,
        source_product_app=source_product_app,
        last_updated_utc=last_updated_utc,
    )


def build_full_identity() -> types.IdentityHistory:
    """A fully-populated IdentityHistory exercising every domain type."""
    identity = types.Identity(
        recidiviz_id=RECIDIVIZ_ID,
        tenant=Tenant.US_OZ,
        person_type=PersonType.JII,
        status=IdentityStatus.ACTIVE,
        merged_into=None,
        last_cluster_hash="cluster-hash-abc",
        skip_demographic_guard=False,
        created_utc=CREATED,
        last_updated_utc=UPDATED,
        external_ids=[
            types.ExternalId(
                external_id="A123", id_type=IdentifierType.US_OZ_LOTR_ID, is_active=True
            ),
            types.ExternalId(
                external_id="OLD9",
                id_type=IdentifierType.US_OZ_LOTR_ID,
                is_active=False,
            ),
        ],
        attributes=types.IdentityAttributes(
            names=[
                make_sourced_attribute(
                    types.Name(
                        surname="Baggins",
                        given_name="Frodo",
                        middle_names=["R"],
                        name_suffix=None,
                        use=NameUse.OFFICIAL,
                    )
                )
            ],
            dates_of_birth=[
                make_sourced_attribute(
                    types.DateOfBirth(
                        date=datetime.date(1990, 9, 22),
                        canonical=False,
                        canonical_locked=False,
                    )
                ),
                make_sourced_attribute(
                    types.DateOfBirth(
                        date=datetime.date(1990, 1, 1),
                        canonical=True,
                        canonical_locked=False,
                    ),
                    source_type=SourceType.PRODUCT_APP,
                    source_product_app=ProductApp.ADMIN_PANEL,
                ),
            ],
            genders=[
                make_sourced_attribute(
                    types.Gender(
                        gender=demographics.Gender.MALE,
                        canonical=True,
                        canonical_locked=False,
                    )
                )
            ],
            races=[make_sourced_attribute(types.Race(race=demographics.Race.WHITE))],
            sexes=[
                make_sourced_attribute(
                    types.Sex(
                        sex=demographics.Sex.MALE,
                        canonical=True,
                        canonical_locked=False,
                    )
                )
            ],
            ethnicities=[
                make_sourced_attribute(
                    types.Ethnicity(
                        ethnicity=demographics.Ethnicity.NOT_HISPANIC,
                        canonical=True,
                        canonical_locked=False,
                    )
                )
            ],
            phone_numbers=[
                make_sourced_attribute(
                    types.PhoneNumber(
                        number="5551234567", type=PhoneType.CELL, preferred=True
                    )
                )
            ],
            emails=[
                make_sourced_attribute(
                    types.Email(address="test@fake.com", address_hash="hashtestfakecom")
                )
            ],
        ),
    )
    return types.IdentityHistory(
        identity=identity,
        merge_events=[
            types.MergeEvent(
                surviving_id=RECIDIVIZ_ID,
                retired_id=RETIRED_ID,
                trigger=MergeTrigger.MERGE_ENDPOINT,
                requested_by="auditor@fake.com",
                timestamp_utc=UPDATED,
                conflicts=[
                    types.AttributeConflict(
                        attribute_type=AttributeType.NAME,
                        retired_value=make_sourced_attribute(
                            types.Name(
                                surname="Baggins",
                                given_name="Frodo",
                                middle_names=[],
                                name_suffix=None,
                                use=NameUse.OFFICIAL,
                            )
                        ),
                        surviving_value=make_sourced_attribute(
                            types.Name(
                                surname="Baggins",
                                given_name="Mr. Frodo",
                                middle_names=[],
                                name_suffix=None,
                                use=NameUse.OFFICIAL,
                            )
                        ),
                    )
                ],
            )
        ],
        split_events=[
            types.SplitEvent(
                original_id=RECIDIVIZ_ID,
                trigger=SplitTrigger.SPLIT_ENDPOINT,
                requested_by=None,
                timestamp_utc=UPDATED,
                destinations=[
                    types.SplitDestination(
                        new_recidiviz_id=NEW_ID,
                        external_ids=[
                            types.ExternalId(
                                external_id="OLD9",
                                id_type=IdentifierType.US_OZ_LOTR_ID,
                                is_active=True,
                            )
                        ],
                        attributes=[
                            make_sourced_attribute(
                                types.Email(
                                    address="test@fake.com",
                                    address_hash="hashtestfakecom",
                                )
                            )
                        ],
                    )
                ],
            )
        ],
    )


# ---------------------------------------------------------------------------
# Database test case shared across the identity service test modules.
# ---------------------------------------------------------------------------


class IdentityDatabaseTestCase(TestCase):
    """Base test case that runs each test against a fresh identity schema in an
    on-disk Postgres started once per test class."""

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


# ---------------------------------------------------------------------------
# Database row builders shared across the identity service test modules.
# Prefer the CSV fixtures in recidiviz/tools/services/identity/fixtures/
# (RECIDIVIZ_ID as an ACTIVE identity, RETIRED_ID as a RETIRED identity merged
# into it) loaded via `reset_fixtures`; use these to layer on the one
# additional property a given test is specifically exercising.
# ---------------------------------------------------------------------------

_IDENTITY_DATABASE_KEY = SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)


def insert_identity(
    *,
    recidiviz_id: uuid.UUID,
    status: IdentityStatus = IdentityStatus.ACTIVE,
    tenant: Tenant = Tenant.US_OZ,
    person_type: PersonType = PersonType.JII,
    merged_into: uuid.UUID | None = None,
    last_cluster_hash: str | None = None,
) -> None:
    """Inserts a bare Identity row (no attributes / external IDs) directly into
    the identity schema, for tests that need a scenario beyond the CSV fixtures
    (e.g. an extra merge-chain hop, a second tenant, a corrupt cycle)."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.Identity(
                recidiviz_id=recidiviz_id,
                created_utc=CREATED,
                last_updated_utc=CREATED,
                tenant=tenant,
                person_type=person_type,
                status=status,
                merged_into=merged_into,
                last_cluster_hash=last_cluster_hash,
            )
        )


def insert_external_id(
    *,
    recidiviz_id: uuid.UUID,
    external_id: str,
    id_type: IdentifierType = IdentifierType.US_OZ_KDS_PERSON_ID,
    is_active: bool = True,
) -> None:
    """Inserts an ExternalId row directly into the identity schema."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.ExternalId(
                recidiviz_id=recidiviz_id,
                external_id=external_id,
                id_type=id_type,
                is_active=is_active,
            )
        )


def insert_name(
    *,
    recidiviz_id: uuid.UUID,
    given_name: str,
    surname: str,
    source_type: SourceType = SourceType.EXTERNAL_DATA_SYSTEM,
    source_product_app: ProductApp | None = None,
) -> None:
    """Inserts a Name row directly into the identity schema."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.Name(
                recidiviz_id=recidiviz_id,
                given_name=given_name,
                surname=surname,
                middle_names=[],
                name_suffix=None,
                use=NameUse.OFFICIAL,
                source_type=source_type,
                source_product_app=source_product_app,
                last_updated_utc=CREATED,
            )
        )


def insert_merge_event(
    *,
    surviving_id: uuid.UUID,
    retired_id: uuid.UUID,
    trigger: MergeTrigger,
    requested_by: str | None = None,
) -> None:
    """Inserts a MergeEvent audit row directly into the identity schema."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.MergeEvent(
                surviving_id=surviving_id,
                retired_id=retired_id,
                trigger=trigger,
                requested_by=requested_by,
                timestamp_utc=CREATED,
            )
        )


def insert_date_of_birth(
    *,
    recidiviz_id: uuid.UUID,
    date: datetime.date,
    canonical: bool = True,
    canonical_locked: bool = False,
) -> None:
    """Inserts a DateOfBirth row directly into the identity schema."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.DateOfBirth(
                recidiviz_id=recidiviz_id,
                date=date,
                canonical=canonical,
                canonical_locked=canonical_locked,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=CREATED,
            )
        )


def insert_phone_number(*, recidiviz_id: uuid.UUID, number: str) -> None:
    """Inserts a PhoneNumber row directly into the identity schema."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.PhoneNumber(
                recidiviz_id=recidiviz_id,
                number=number,
                type=None,
                preferred=None,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=CREATED,
            )
        )


def insert_email(
    *,
    recidiviz_id: uuid.UUID,
    address_hash: str,
    source_type: SourceType = SourceType.EXTERNAL_DATA_SYSTEM,
    source_product_app: ProductApp | None = None,
) -> None:
    """Inserts an Email row directly into the identity schema."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.Email(
                recidiviz_id=recidiviz_id,
                address="test@example.com",
                address_hash=address_hash,
                source_type=source_type,
                source_product_app=source_product_app,
                last_updated_utc=CREATED,
            )
        )


def insert_merge_candidate(
    *,
    recidiviz_ids: list[uuid.UUID],
    tenant: Tenant = Tenant.US_OZ,
    status: CandidateStatus = CandidateStatus.PENDING,
    detected_at_utc: datetime.datetime = CREATED,
    review_context: dict[str, object] | None = None,
) -> None:
    """Inserts a MergeCandidate row directly into the identity schema, with one
    MergeCandidateIdentity link row per given recidiviz_id."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        candidate = schema.MergeCandidate(
            tenant=tenant,
            detected_at_utc=detected_at_utc,
            review_context=review_context or {},
            status=status,
        )
        session.add(candidate)
        # Assigns candidate.id so the link rows can reference it.
        session.flush()
        session.add_all(
            [
                schema.MergeCandidateIdentity(
                    merge_candidate_id=candidate.id, recidiviz_id=recidiviz_id
                )
                for recidiviz_id in recidiviz_ids
            ]
        )


def insert_split_candidate(
    *,
    recidiviz_id: uuid.UUID,
    tenant: Tenant = Tenant.US_OZ,
    status: CandidateStatus = CandidateStatus.PENDING,
    detected_at_utc: datetime.datetime = CREATED,
    cluster_groups: dict[str, object] | None = None,
    blockers: list[str] | None = None,
) -> None:
    """Inserts a SplitCandidate row directly into the identity schema."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.SplitCandidate(
                tenant=tenant,
                recidiviz_id=recidiviz_id,
                cluster_groups=cluster_groups or {},
                blockers=blockers or [SplitBlocker.HUMAN_EDITED_ATTRIBUTES.value],
                detected_at_utc=detected_at_utc,
                status=status,
            )
        )


def insert_no_merge(*, recidiviz_id_a: uuid.UUID, recidiviz_id_b: uuid.UUID) -> None:
    """Inserts a NoMerge row directly into the identity schema, putting the
    pair in the table's canonical (lower UUID first) order."""
    first_id, second_id = sorted([recidiviz_id_a, recidiviz_id_b])
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        session.add(
            schema.NoMerge(
                recidiviz_id_a=first_id,
                recidiviz_id_b=second_id,
                reason=None,
                created_by="test@recidiviz.org",
                created_utc=CREATED,
            )
        )
