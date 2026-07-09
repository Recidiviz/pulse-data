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
"""Tests for batched merged_into chain resolution."""
import unittest
import uuid
from typing import Any

import pytest
from sqlalchemy import event, text

from recidiviz.common.constants.identity import IdentityStatus
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity.exceptions import IdentityHistoryIntegrityException
from recidiviz.services.identity.resolution_helpers import resolve_surviving_ids
from recidiviz.tests.services.identity.test_utils import insert_identity
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

SURVIVING_ID = uuid.UUID("a0000000-0000-0000-0000-000000000000")
MID_CHAIN_ID = uuid.UUID("b0000000-0000-0000-0000-000000000000")
RETIRED_ID_1 = uuid.UUID("c1000000-0000-0000-0000-000000000000")
RETIRED_ID_2 = uuid.UUID("c2000000-0000-0000-0000-000000000000")
OTHER_ACTIVE_ID = uuid.UUID("d0000000-0000-0000-0000-000000000000")
UNKNOWN_ID = uuid.UUID("e0000000-0000-0000-0000-000000000000")
CYCLE_ID_A = uuid.UUID("f1000000-0000-0000-0000-000000000000")
CYCLE_ID_B = uuid.UUID("f2000000-0000-0000-0000-000000000000")


@pytest.mark.uses_db
class ResolveSurvivingIdsTest(unittest.TestCase):
    """Tests for resolve_surviving_ids."""

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

    def _resolve(
        self, recidiviz_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, uuid.UUID | None]:
        with SessionFactory.using_database(self.database_key) as session:
            return resolve_surviving_ids(session, recidiviz_ids)

    def test_empty_input_returns_empty_dict(self) -> None:
        self.assertEqual({}, self._resolve([]))

    def test_active_id_resolves_to_itself(self) -> None:
        insert_identity(recidiviz_id=SURVIVING_ID)

        self.assertEqual({SURVIVING_ID: SURVIVING_ID}, self._resolve([SURVIVING_ID]))

    def test_resolves_single_hop(self) -> None:
        insert_identity(recidiviz_id=SURVIVING_ID)
        insert_identity(
            recidiviz_id=RETIRED_ID_1,
            status=IdentityStatus.RETIRED,
            merged_into=SURVIVING_ID,
        )

        self.assertEqual({RETIRED_ID_1: SURVIVING_ID}, self._resolve([RETIRED_ID_1]))

    def test_resolves_multi_hop_chain(self) -> None:
        insert_identity(recidiviz_id=SURVIVING_ID)
        insert_identity(
            recidiviz_id=MID_CHAIN_ID,
            status=IdentityStatus.RETIRED,
            merged_into=SURVIVING_ID,
        )
        insert_identity(
            recidiviz_id=RETIRED_ID_1,
            status=IdentityStatus.RETIRED,
            merged_into=MID_CHAIN_ID,
        )

        self.assertEqual({RETIRED_ID_1: SURVIVING_ID}, self._resolve([RETIRED_ID_1]))

    def test_batch_resolves_mixed_inputs_with_shared_survivor(self) -> None:
        insert_identity(recidiviz_id=SURVIVING_ID)
        insert_identity(recidiviz_id=OTHER_ACTIVE_ID)
        insert_identity(
            recidiviz_id=RETIRED_ID_1,
            status=IdentityStatus.RETIRED,
            merged_into=SURVIVING_ID,
        )
        insert_identity(
            recidiviz_id=RETIRED_ID_2,
            status=IdentityStatus.RETIRED,
            merged_into=SURVIVING_ID,
        )

        self.assertEqual(
            {
                RETIRED_ID_1: SURVIVING_ID,
                RETIRED_ID_2: SURVIVING_ID,
                SURVIVING_ID: SURVIVING_ID,
                OTHER_ACTIVE_ID: OTHER_ACTIVE_ID,
            },
            self._resolve([RETIRED_ID_1, RETIRED_ID_2, SURVIVING_ID, OTHER_ACTIVE_ID]),
        )

    def test_batch_issues_one_query_per_chain_hop_level(self) -> None:
        """Resolving a batch must not query per id: two 1-hop chains sharing a
        survivor plus an unrelated active id take exactly two SELECTs (one for
        the input frontier, one for the shared survivor)."""
        insert_identity(recidiviz_id=SURVIVING_ID)
        insert_identity(recidiviz_id=OTHER_ACTIVE_ID)
        insert_identity(
            recidiviz_id=RETIRED_ID_1,
            status=IdentityStatus.RETIRED,
            merged_into=SURVIVING_ID,
        )
        insert_identity(
            recidiviz_id=RETIRED_ID_2,
            status=IdentityStatus.RETIRED,
            merged_into=SURVIVING_ID,
        )

        select_statements: list[str] = []

        def record_select(
            _conn: Any,
            _cursor: Any,
            statement: str,
            _parameters: Any,
            _context: Any,
            _executemany: bool,
        ) -> None:
            if statement.lstrip().upper().startswith("SELECT"):
                select_statements.append(statement)

        with SessionFactory.using_database(self.database_key) as session:
            event.listen(self.engine, "before_cursor_execute", record_select)
            try:
                result = resolve_surviving_ids(
                    session, [RETIRED_ID_1, RETIRED_ID_2, OTHER_ACTIVE_ID]
                )
            finally:
                event.remove(self.engine, "before_cursor_execute", record_select)

        self.assertEqual(
            {
                RETIRED_ID_1: SURVIVING_ID,
                RETIRED_ID_2: SURVIVING_ID,
                OTHER_ACTIVE_ID: OTHER_ACTIVE_ID,
            },
            result,
        )
        self.assertEqual(2, len(select_statements))

    def test_missing_input_id_resolves_to_none(self) -> None:
        insert_identity(recidiviz_id=SURVIVING_ID)

        result = self._resolve([SURVIVING_ID, UNKNOWN_ID])

        self.assertEqual(
            {SURVIVING_ID: SURVIVING_ID, UNKNOWN_ID: None},
            result,
        )
        self.assertIsNone(result[UNKNOWN_ID])

    def test_broken_chain_raises(self) -> None:
        insert_identity(recidiviz_id=SURVIVING_ID)
        insert_identity(
            recidiviz_id=RETIRED_ID_1,
            status=IdentityStatus.RETIRED,
            merged_into=SURVIVING_ID,
        )
        # Delete the chain target with FK enforcement disabled to simulate the
        # corrupt state the resolver guards against.
        with SessionFactory.using_database(self.database_key) as session:
            session.execute(text("SET session_replication_role = 'replica'"))
            session.execute(
                text("DELETE FROM identities WHERE recidiviz_id = :rid"),
                {"rid": str(SURVIVING_ID)},
            )

        with self.assertRaisesRegex(
            IdentityHistoryIntegrityException,
            rf"^Identity \[{SURVIVING_ID}\] referenced via merged_into chain "
            rf"from \[{RETIRED_ID_1}\] does not exist\.$",
        ):
            self._resolve([RETIRED_ID_1])

    def test_self_cycle_raises(self) -> None:
        # PostgreSQL allows a self-referential merged_into (the row satisfies
        # its own FK after insert). This is corrupt state the resolver guards
        # against.
        insert_identity(
            recidiviz_id=CYCLE_ID_A,
            status=IdentityStatus.RETIRED,
            merged_into=CYCLE_ID_A,
        )

        with self.assertRaisesRegex(
            IdentityHistoryIntegrityException,
            rf"^Cycle detected in merged_into chain starting from \[{CYCLE_ID_A}\]: "
            rf"revisited \[{CYCLE_ID_A}\]$",
        ):
            self._resolve([CYCLE_ID_A])

    def test_two_node_cycle_raises(self) -> None:
        insert_identity(
            recidiviz_id=CYCLE_ID_A,
            status=IdentityStatus.RETIRED,
            merged_into=CYCLE_ID_A,
        )
        insert_identity(
            recidiviz_id=CYCLE_ID_B,
            status=IdentityStatus.RETIRED,
            merged_into=CYCLE_ID_A,
        )
        # Repoint A at B to close the two-node cycle A -> B -> A.
        with SessionFactory.using_database(self.database_key) as session:
            session.query(schema.Identity).filter(
                schema.Identity.recidiviz_id == CYCLE_ID_A
            ).update({"merged_into": CYCLE_ID_B})

        with self.assertRaisesRegex(
            IdentityHistoryIntegrityException,
            rf"^Cycle detected in merged_into chain starting from \[{CYCLE_ID_A}\]: "
            rf"revisited \[{CYCLE_ID_A}\]$",
        ):
            self._resolve([CYCLE_ID_A])
