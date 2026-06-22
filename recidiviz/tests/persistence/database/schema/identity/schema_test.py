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
"""Tests for the schema defined in identity/schema.py.

These tests exercise the DB-level constraints the design doc calls out
specifically: identity status/merged_into consistency, the partial unique
index on active external_ids, source provenance on attribute rows, and the
canonical-order CHECK on no_merge."""
import datetime
import unittest
import uuid

import pytest
from sqlalchemy.exc import IntegrityError

from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


def _new_identity(
    *,
    recidiviz_id: uuid.UUID | None = None,
    status: str = "ACTIVE",
    merged_into: uuid.UUID | None = None,
    tenant: str = "US_OZ",
    person_type: str = "JII",
) -> schema.Identity:
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    return schema.Identity(
        recidiviz_id=recidiviz_id or uuid.uuid4(),
        created_utc=now,
        last_updated_utc=now,
        tenant=tenant,
        person_type=person_type,
        status=status,
        merged_into=merged_into,
    )


@pytest.mark.uses_db
class IdentitySchemaTest(unittest.TestCase):
    """Constraint-level tests for identity/schema.py."""

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

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_active_identity_cannot_have_merged_into(self) -> None:
        surviving = _new_identity()
        bad_active = _new_identity(status="ACTIVE", merged_into=surviving.recidiviz_id)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(surviving)
            session.commit()

            session.add(bad_active)
            with self.assertRaisesRegex(
                IntegrityError, "ck_identities_merged_into_requires_retired"
            ):
                session.commit()

    def test_retired_identity_requires_merged_into(self) -> None:
        bad_retired = _new_identity(status="RETIRED", merged_into=None)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(bad_retired)
            with self.assertRaisesRegex(
                IntegrityError, "ck_identities_retired_requires_merged_into"
            ):
                session.commit()

    def test_retired_with_merged_into_is_valid(self) -> None:
        surviving = _new_identity()
        retired = _new_identity(status="RETIRED", merged_into=surviving.recidiviz_id)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(surviving)
            session.add(retired)
            session.commit()

    def test_external_ids_partial_unique_only_applies_to_active(self) -> None:
        identity_a = _new_identity()
        identity_b = _new_identity()
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add_all([identity_a, identity_b])
            session.commit()

            # Two active rows with the same (external_id, id_type) — rejected.
            session.add(
                schema.ExternalId(
                    external_id="ABC",
                    id_type="US_OZ_LOTR_ID",
                    recidiviz_id=identity_a.recidiviz_id,
                    is_active=True,
                )
            )
            session.add(
                schema.ExternalId(
                    external_id="ABC",
                    id_type="US_OZ_LOTR_ID",
                    recidiviz_id=identity_b.recidiviz_id,
                    is_active=True,
                )
            )
            with self.assertRaisesRegex(IntegrityError, "uq_external_ids_active"):
                session.commit()
            session.rollback()

            # An inactive duplicate is fine.
            session.add(
                schema.ExternalId(
                    external_id="ABC",
                    id_type="US_OZ_LOTR_ID",
                    recidiviz_id=identity_a.recidiviz_id,
                    is_active=True,
                )
            )
            session.add(
                schema.ExternalId(
                    external_id="ABC",
                    id_type="US_OZ_LOTR_ID",
                    recidiviz_id=identity_b.recidiviz_id,
                    is_active=False,
                )
            )
            session.commit()

    def test_name_source_provenance_check(self) -> None:
        identity = _new_identity()
        bad_name = schema.Name(
            recidiviz_id=identity.recidiviz_id,
            surname="Doe",
            source_type="PRODUCT_APP",
            source_product_app=None,
            last_updated_utc=datetime.datetime(
                2026, 1, 1, tzinfo=datetime.timezone.utc
            ),
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(identity)
            session.add(bad_name)
            with self.assertRaisesRegex(IntegrityError, "ck_names_source_provenance"):
                session.commit()

    def test_no_merge_canonical_order_check(self) -> None:
        id_a = _new_identity()
        id_b = _new_identity()
        low, high = sorted([id_a.recidiviz_id, id_b.recidiviz_id])
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add_all([id_a, id_b])
            session.commit()

            # Wrong order: high < low is false, so this row violates the check.
            session.add(
                schema.NoMerge(
                    recidiviz_id_a=high,
                    recidiviz_id_b=low,
                    created_by="test@recidiviz.org",
                    created_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                )
            )
            with self.assertRaisesRegex(IntegrityError, "ck_no_merge_canonical_order"):
                session.commit()
            session.rollback()

            # Correct order is accepted.
            session.add(
                schema.NoMerge(
                    recidiviz_id_a=low,
                    recidiviz_id_b=high,
                    created_by="test@recidiviz.org",
                    created_utc=datetime.datetime(
                        2026, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                )
            )
            session.commit()
