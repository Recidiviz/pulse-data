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
"""Tests for the Identity Service import's create pass."""
import datetime
from unittest import TestCase

import pytest
from freezegun import freeze_time

from recidiviz.common import demographics
from recidiviz.common.constants.identity import (
    IdentifierType,
    IdentityStatus,
    NameUse,
    PersonType,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterAlias,
    IdentityClusterEmail,
    IdentityClusterEthnicity,
    IdentityClusterExternalId,
    IdentityClusterGender,
    IdentityClusterName,
    IdentityClusterPhoneNumber,
    IdentityClusterRace,
    IdentityClusterSex,
)
from recidiviz.services.identity import types
from recidiviz.services.identity.import_processing.bq_snapshot_reader import (
    ClusterSnapshot,
)
from recidiviz.services.identity.import_processing.create_pass import (
    build_identity_rows,
)
from recidiviz.services.identity.querier import IdentityServiceQuerier
from recidiviz.tests.services.identity.test_utils import make_sourced_attribute
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.user_hash import normalized_email_hash

_TENANT = Tenant.US_OZ
_ID_TYPE = IdentifierType.US_OZ_LOTR_ID
_NOW = datetime.datetime(2026, 9, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _snapshot_for(cluster: IdentityCluster) -> ClusterSnapshot:
    return ClusterSnapshot(
        cluster=cluster,
        stored_cluster_hash=cluster.cluster_hash,
    )


def _email_rows_in(rows: list[schema.IdentityBase]) -> list[schema.Email]:
    return [row for row in rows if isinstance(row, schema.Email)]


class BuildIdentityRowsEmailExclusionTest(TestCase):
    """Tests for build_identity_rows' email exclusion, which needs no database."""

    def _snapshot_with_email(
        self, *, external_id: str, address: str = "shared@fake.com"
    ) -> ClusterSnapshot:
        return _snapshot_for(
            IdentityCluster(
                tenant=_TENANT,
                person_type=PersonType.JII,
                external_ids=(
                    IdentityClusterExternalId(
                        tenant=_TENANT, external_id=external_id, id_type=_ID_TYPE.value
                    ),
                ),
                emails=(IdentityClusterEmail(tenant=_TENANT, address=address),),
            )
        )

    def test_email_already_committed_is_excluded_and_logged(self) -> None:
        snapshot = self._snapshot_with_email(external_id="A1")
        committed = {normalized_email_hash("shared@fake.com")}

        with self.assertLogs(level="WARNING") as logs:
            rows = build_identity_rows(
                snapshot=snapshot,
                now=_NOW,
                committed_email_hashes=committed,
                staged_email_hashes=set(),
            )

        self.assertEqual([], _email_rows_in(rows))
        self.assertEqual(1, len(logs.records))
        # The log names the cluster but never the address.
        self.assertIn(
            snapshot.cluster.identity_cluster_id, logs.records[0].getMessage()
        )
        self.assertNotIn("shared@fake.com", logs.records[0].getMessage())

    def test_email_differing_only_in_case_from_committed_is_excluded(self) -> None:
        snapshot = self._snapshot_with_email(
            external_id="A1", address="SHARED@Fake.com"
        )
        committed = {normalized_email_hash("shared@fake.com")}

        with self.assertLogs(level="WARNING"):
            rows = build_identity_rows(
                snapshot=snapshot,
                now=_NOW,
                committed_email_hashes=committed,
                staged_email_hashes=set(),
            )

        self.assertEqual([], _email_rows_in(rows))

    def test_email_staged_earlier_in_transaction_is_excluded(self) -> None:
        first = self._snapshot_with_email(external_id="A1")
        second = self._snapshot_with_email(external_id="B2")
        committed: set[str] = set()
        staged: set[str] = set()

        first_rows = build_identity_rows(
            snapshot=first,
            now=_NOW,
            committed_email_hashes=committed,
            staged_email_hashes=staged,
        )
        with self.assertLogs(level="WARNING"):
            second_rows = build_identity_rows(
                snapshot=second,
                now=_NOW,
                committed_email_hashes=committed,
                staged_email_hashes=staged,
            )

        self.assertEqual(1, len(_email_rows_in(first_rows)))
        self.assertEqual([], _email_rows_in(second_rows))
        # The first cluster's email hash is staged for the rest of the transaction;
        # the literal is normalized_email_hash("shared@fake.com").
        self.assertEqual({"1H4+9uzAJodib5XgFW2La+HgK3PeQtU8CVUl4ziIlJs="}, staged)


@pytest.mark.uses_db
class BuildIdentityRowsMappingTest(TestCase):
    """Tests that build_identity_rows' output persists into a queryable identity."""

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
        self.querier = IdentityServiceQuerier()

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def _persist(self, snapshot: ClusterSnapshot) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                build_identity_rows(
                    snapshot=snapshot,
                    now=_NOW,
                    committed_email_hashes=set(),
                    staged_email_hashes=set(),
                )
            )

    def _get_created_identity(self, external_id: str) -> types.Identity:
        identity = self.querier.get_by_external_id(external_id, _ID_TYPE)
        if identity is None:
            self.fail(f"No identity was created for external id [{external_id}]")
        return identity

    @freeze_time(_NOW)
    def test_rich_cluster_maps_to_full_identity(self) -> None:
        cluster = IdentityCluster(
            tenant=_TENANT,
            person_type=PersonType.JII,
            birthdate=datetime.date(1990, 9, 22),
            external_ids=(
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="A123", id_type=_ID_TYPE.value
                ),
            ),
            name=IdentityClusterName(
                tenant=_TENANT,
                given_name="Frodo",
                preferred_name="Fro",
                surname="Baggins",
                middle_name="Drogo",
                name_suffix="Jr.",
            ),
            gender=IdentityClusterGender(
                tenant=_TENANT, gender=demographics.Gender.MALE
            ),
            sex=IdentityClusterSex(tenant=_TENANT, sex=demographics.Sex.MALE),
            races=(
                IdentityClusterRace(tenant=_TENANT, race=demographics.Race.WHITE),
                IdentityClusterRace(tenant=_TENANT, race=demographics.Race.ASIAN),
            ),
            ethnicity=IdentityClusterEthnicity(
                tenant=_TENANT, ethnicity=demographics.Ethnicity.NOT_HISPANIC
            ),
            phone_numbers=(
                IdentityClusterPhoneNumber(tenant=_TENANT, number="5551234567"),
            ),
            emails=(IdentityClusterEmail(tenant=_TENANT, address="frodo@fake.com"),),
            aliases=(
                IdentityClusterAlias(
                    tenant=_TENANT,
                    given_name="Mr",
                    surname="Underhill",
                    name_use=NameUse.ALIAS,
                ),
            ),
        )

        self._persist(_snapshot_for(cluster))

        identity = self._get_created_identity("A123")
        self.assertEqual(
            types.Identity(
                recidiviz_id=identity.recidiviz_id,
                created_utc=_NOW,
                last_updated_utc=_NOW,
                tenant=_TENANT,
                person_type=PersonType.JII,
                status=IdentityStatus.ACTIVE,
                merged_into=None,
                last_cluster_hash=cluster.cluster_hash,
                skip_demographic_guard=False,
                external_ids=[
                    types.ExternalId(
                        external_id="A123", id_type=_ID_TYPE, is_active=True
                    )
                ],
                attributes=types.IdentityAttributes(
                    names=[
                        make_sourced_attribute(
                            types.Name(
                                given_name="Frodo",
                                surname="Baggins",
                                middle_names=["Drogo"],
                                name_suffix="Jr.",
                                use=NameUse.OFFICIAL,
                            ),
                            last_updated_utc=_NOW,
                        ),
                        make_sourced_attribute(
                            types.Name(
                                given_name="Fro",
                                surname="Baggins",
                                middle_names=[],
                                name_suffix=None,
                                use=NameUse.PREFERRED,
                            ),
                            last_updated_utc=_NOW,
                        ),
                        make_sourced_attribute(
                            types.Name(
                                given_name="Mr",
                                surname="Underhill",
                                middle_names=[],
                                name_suffix=None,
                                use=NameUse.ALIAS,
                            ),
                            last_updated_utc=_NOW,
                        ),
                    ],
                    dates_of_birth=[
                        make_sourced_attribute(
                            types.DateOfBirth(
                                date=datetime.date(1990, 9, 22),
                                canonical=True,
                                canonical_locked=False,
                            ),
                            last_updated_utc=_NOW,
                        )
                    ],
                    genders=[
                        make_sourced_attribute(
                            types.Gender(
                                gender=demographics.Gender.MALE,
                                canonical=True,
                                canonical_locked=False,
                            ),
                            last_updated_utc=_NOW,
                        )
                    ],
                    races=[
                        make_sourced_attribute(
                            types.Race(race=demographics.Race.WHITE),
                            last_updated_utc=_NOW,
                        ),
                        make_sourced_attribute(
                            types.Race(race=demographics.Race.ASIAN),
                            last_updated_utc=_NOW,
                        ),
                    ],
                    sexes=[
                        make_sourced_attribute(
                            types.Sex(
                                sex=demographics.Sex.MALE,
                                canonical=True,
                                canonical_locked=False,
                            ),
                            last_updated_utc=_NOW,
                        )
                    ],
                    ethnicities=[
                        make_sourced_attribute(
                            types.Ethnicity(
                                ethnicity=demographics.Ethnicity.NOT_HISPANIC,
                                canonical=True,
                                canonical_locked=False,
                            ),
                            last_updated_utc=_NOW,
                        )
                    ],
                    phone_numbers=[
                        make_sourced_attribute(
                            types.PhoneNumber(
                                number="5551234567", type=None, preferred=None
                            ),
                            last_updated_utc=_NOW,
                        )
                    ],
                    emails=[
                        make_sourced_attribute(
                            types.Email(
                                address="frodo@fake.com",
                                address_hash="bP+aE9fr5aEDYU9byX+Dcerca+uB2+Y3tUTygk8DInY=",
                            ),
                            last_updated_utc=_NOW,
                        )
                    ],
                ),
            ),
            identity,
        )

    @freeze_time(_NOW)
    def test_cluster_with_only_external_ids_creates_bare_identity(self) -> None:
        cluster = IdentityCluster(
            tenant=_TENANT,
            person_type=PersonType.STAFF,
            external_ids=(
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="S9", id_type=_ID_TYPE.value
                ),
            ),
        )

        self._persist(_snapshot_for(cluster))

        identity = self._get_created_identity("S9")
        self.assertEqual(
            types.Identity(
                recidiviz_id=identity.recidiviz_id,
                created_utc=_NOW,
                last_updated_utc=_NOW,
                tenant=_TENANT,
                person_type=PersonType.STAFF,
                status=IdentityStatus.ACTIVE,
                merged_into=None,
                last_cluster_hash=cluster.cluster_hash,
                skip_demographic_guard=False,
                external_ids=[
                    types.ExternalId(external_id="S9", id_type=_ID_TYPE, is_active=True)
                ],
                attributes=types.IdentityAttributes(
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
            identity,
        )
