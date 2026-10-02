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
"""Tests for the import's auto-split pass. apply_identity_split and
find_split_blockers are called directly with hand-built cluster_groups
payloads; the import-level orchestration that produces them is covered in
import_processor_test.py."""
import datetime
import uuid

import pytest
from more_itertools import one

from recidiviz.common.constants.identity import (
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    ProductApp,
    SourceType,
    SplitBlocker,
    SplitTrigger,
)
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.services.identity.import_processing.split_pass import (
    IMPORT_AUTO_SPLIT_NO_MERGE_REASON,
    apply_identity_split,
    find_split_blockers,
)
from recidiviz.tests.services.identity.test_utils import (
    IdentityDatabaseTestCase,
    insert_email,
    insert_external_id,
    insert_identity,
    insert_merge_event,
    insert_name,
)

_ID_TYPE = IdentifierType.US_OZ_LOTR_ID
_NOW = datetime.datetime(2026, 9, 17, 12, 0, tzinfo=datetime.timezone.utc)


def _external_id_json(external_id: str) -> dict[str, str]:
    """Returns one external id as it appears in a cluster_groups payload."""
    return {"id_type": _ID_TYPE.value, "external_id": external_id}


def _cluster_groups(
    groups: list[list[str]], unclustered: list[str]
) -> dict[str, object]:
    """Returns a cluster_groups payload with one group per inner id list."""
    return {
        "cluster_groups": [
            {
                "identity_cluster_id": f"cluster-{index}",
                "external_ids": [
                    _external_id_json(external_id) for external_id in group
                ],
            }
            for index, group in enumerate(groups)
        ],
        "unclustered_external_ids": [
            _external_id_json(external_id) for external_id in unclustered
        ],
    }


@pytest.mark.uses_db
class ApplyIdentitySplitTest(IdentityDatabaseTestCase):
    """Tests for apply_identity_split: each non-kept cluster group's external
    ids move to a new empty identity, everything else except import-sourced
    emails stays with the original, and the split is audited."""

    def _insert_identity_with_ids(self, *, external_ids: list[str]) -> uuid.UUID:
        recidiviz_id = uuid.uuid4()
        insert_identity(recidiviz_id=recidiviz_id)
        for external_id in external_ids:
            insert_external_id(
                recidiviz_id=recidiviz_id, external_id=external_id, id_type=_ID_TYPE
            )
        return recidiviz_id

    def _apply(
        self, *, recidiviz_id: uuid.UUID, cluster_groups: dict[str, object]
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            identity = (
                session.query(schema.Identity)
                .filter(schema.Identity.recidiviz_id == recidiviz_id)
                .one()
            )
            apply_identity_split(
                session=session,
                identity=identity,
                cluster_groups=cluster_groups,
                now=_NOW,
            )

    def _active_ids_by_identity(self) -> dict[uuid.UUID, set[str]]:
        """Returns each identity's active external ids."""
        with SessionFactory.using_database(self.database_key) as session:
            rows = session.query(
                schema.ExternalId.recidiviz_id,
                schema.ExternalId.external_id,
                schema.ExternalId.is_active,
            ).all()
        active_ids_by_identity: dict[uuid.UUID, set[str]] = {}
        for recidiviz_id, external_id, is_active in rows:
            if is_active:
                active_ids_by_identity.setdefault(recidiviz_id, set()).add(external_id)
        return active_ids_by_identity

    def test_non_kept_group_moves_to_a_new_empty_identity(self) -> None:
        original_id = self._insert_identity_with_ids(
            external_ids=["A1", "A2", "B2", "C9"]
        )
        insert_name(recidiviz_id=original_id, given_name="Ada", surname="Original")
        insert_external_id(
            recidiviz_id=original_id,
            external_id="OLD9",
            id_type=_ID_TYPE,
            is_active=False,
        )

        # The A1+A2 group is largest, so the original keeps it; C9 is in no
        # cluster and stays; B2 moves to the new identity.
        self._apply(
            recidiviz_id=original_id,
            cluster_groups=_cluster_groups([["A1", "A2"], ["B2"]], ["C9"]),
        )

        active_ids_by_identity = self._active_ids_by_identity()
        self.assertEqual({"A1", "A2", "C9"}, active_ids_by_identity[original_id])
        new_id = one(
            recidiviz_id
            for recidiviz_id in active_ids_by_identity
            if recidiviz_id != original_id
        )
        self.assertEqual({"B2"}, active_ids_by_identity[new_id])
        with SessionFactory.using_database(self.database_key) as session:
            original = (
                session.query(schema.Identity)
                .filter(schema.Identity.recidiviz_id == original_id)
                .one()
            )
            self.assertTrue(original.skip_demographic_guard)
            new_identity = (
                session.query(schema.Identity)
                .filter(schema.Identity.recidiviz_id == new_id)
                .one()
            )
            self.assertIsNone(new_identity.last_cluster_hash)
            self.assertEqual(IdentityStatus.ACTIVE, new_identity.status)
            # The original's B2 row survives as inactive residue, like OLD9.
            inactive_ids = {
                external_id
                for (external_id,) in session.query(
                    schema.ExternalId.external_id
                ).filter(
                    schema.ExternalId.recidiviz_id == original_id,
                    schema.ExternalId.is_active.is_(False),
                )
            }
            self.assertEqual({"OLD9", "B2"}, inactive_ids)
            # Every attribute row stays with the original; the new identity
            # starts empty.
            name_owner_ids = [
                recidiviz_id
                for (recidiviz_id,) in session.query(schema.Name.recidiviz_id)
            ]
        self.assertEqual([original_id], name_owner_ids)

    def test_import_sourced_emails_are_deleted_and_cluster_hash_cleared(self) -> None:
        """An email may live on at most one identity and may belong to a
        split-off group's person, so the split deletes the original's
        import-sourced emails and clears last_cluster_hash; the same run's
        update pass then re-adds each address to whichever identity's cluster
        carries it. Human-entered emails stay on the original."""
        original_id = uuid.uuid4()
        insert_identity(recidiviz_id=original_id, last_cluster_hash="stale-hash")
        insert_external_id(recidiviz_id=original_id, external_id="A1", id_type=_ID_TYPE)
        insert_external_id(recidiviz_id=original_id, external_id="B2", id_type=_ID_TYPE)
        insert_email(recidiviz_id=original_id, address_hash="import-sourced-hash")
        insert_email(
            recidiviz_id=original_id,
            address_hash="human-entered-hash",
            source_type=SourceType.PRODUCT_APP,
            source_product_app=ProductApp.ADMIN_PANEL,
        )

        self._apply(
            recidiviz_id=original_id,
            cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
        )

        with SessionFactory.using_database(self.database_key) as session:
            surviving_emails = [
                (row.recidiviz_id, row.address_hash)
                for row in session.query(schema.Email)
            ]
            original_last_cluster_hash = (
                session.query(schema.Identity.last_cluster_hash)
                .filter(schema.Identity.recidiviz_id == original_id)
                .one()[0]
            )
        self.assertEqual([(original_id, "human-entered-hash")], surviving_emails)
        self.assertIsNone(original_last_cluster_hash)

    def test_kept_group_tie_breaks_to_lexicographically_smallest_id(self) -> None:
        original_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])

        # Both groups hold one id, so the group containing A1 (smaller than
        # B2) is kept regardless of group order in the payload.
        self._apply(
            recidiviz_id=original_id,
            cluster_groups=_cluster_groups([["B2"], ["A1"]], []),
        )

        active_ids_by_identity = self._active_ids_by_identity()
        self.assertEqual({"A1"}, active_ids_by_identity[original_id])
        new_id = one(
            recidiviz_id
            for recidiviz_id in active_ids_by_identity
            if recidiviz_id != original_id
        )
        self.assertEqual({"B2"}, active_ids_by_identity[new_id])

    def test_split_event_is_recorded(self) -> None:
        original_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])

        self._apply(
            recidiviz_id=original_id,
            cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
        )

        new_id = one(
            recidiviz_id
            for recidiviz_id in self._active_ids_by_identity()
            if recidiviz_id != original_id
        )
        with SessionFactory.using_database(self.database_key) as session:
            split_event = one(session.query(schema.SplitEvent).all())
            self.assertEqual(original_id, split_event.original_id)
            self.assertEqual(SplitTrigger.IMPORT, split_event.trigger)
            self.assertIsNone(split_event.requested_by)
            self.assertEqual(_NOW, split_event.timestamp_utc)
            new_identity_row = one(session.query(schema.SplitEventNewIdentity).all())
            self.assertEqual(new_id, new_identity_row.new_recidiviz_id)
            moved_row = one(session.query(schema.SplitEventMovedExternalId).all())
            self.assertEqual("B2", moved_row.external_id)
            self.assertEqual(new_id, moved_row.new_recidiviz_id)
            # Attributes stay with the original, so no moved-attribute rows.
            self.assertEqual(0, session.query(schema.SplitEventMovedAttribute).count())

    def test_no_merge_pairs_are_recorded_pairwise(self) -> None:
        original_id = self._insert_identity_with_ids(
            external_ids=["A1", "A2", "B2", "C3"]
        )

        # Three groups: the original keeps A1+A2 and two new identities are
        # created, so three no_merge pairs cover every pairing.
        self._apply(
            recidiviz_id=original_id,
            cluster_groups=_cluster_groups([["A1", "A2"], ["B2"], ["C3"]], []),
        )

        with SessionFactory.using_database(self.database_key) as session:
            no_merge_rows = session.query(schema.NoMerge).all()
            pairs = {(row.recidiviz_id_a, row.recidiviz_id_b) for row in no_merge_rows}
            self.assertEqual(3, len(pairs))
            for row in no_merge_rows:
                self.assertLess(row.recidiviz_id_a, row.recidiviz_id_b)
                # NULL created_by marks a system-recorded pair.
                self.assertIsNone(row.created_by)
                self.assertEqual(IMPORT_AUTO_SPLIT_NO_MERGE_REASON, row.reason)
            all_identity_ids = {
                recidiviz_id
                for (recidiviz_id,) in session.query(schema.Identity.recidiviz_id)
            }
        self.assertEqual(
            all_identity_ids,
            {identity_id for pair in pairs for identity_id in pair},
        )

    def test_payload_missing_an_active_id_raises(self) -> None:
        original_id = self._insert_identity_with_ids(external_ids=["A1", "B2", "C3"])

        # C3 is active on the identity but absent from the payload, as a
        # stale stored candidate's payload would be after the identity
        # gained an id.
        with self.assertRaisesRegex(
            ValueError, "must cover exactly its active external ids"
        ):
            self._apply(
                recidiviz_id=original_id,
                cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
            )

    def test_payload_listing_an_id_twice_raises(self) -> None:
        original_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])

        with self.assertRaisesRegex(ValueError, "lists external ids more than once"):
            self._apply(
                recidiviz_id=original_id,
                cluster_groups=_cluster_groups([["A1", "B2"], ["B2"]], []),
            )


@pytest.mark.uses_db
class FindSplitBlockersTest(IdentityDatabaseTestCase):
    """Tests for find_split_blockers: a human merge whose contribution the
    split would tear apart, or human-edited attributes, block the auto-split."""

    def _insert_identity_with_ids(self, *, external_ids: list[str]) -> uuid.UUID:
        recidiviz_id = uuid.uuid4()
        insert_identity(recidiviz_id=recidiviz_id)
        for external_id in external_ids:
            insert_external_id(
                recidiviz_id=recidiviz_id, external_id=external_id, id_type=_ID_TYPE
            )
        return recidiviz_id

    def _insert_retired_contributor(
        self,
        *,
        surviving_id: uuid.UUID,
        contributed_ids: list[str],
        trigger: MergeTrigger,
    ) -> None:
        """Inserts a RETIRED identity merged into the survivor, holding the
        ids it contributed as inactive rows, plus the merge's audit row."""
        retired_id = uuid.uuid4()
        insert_identity(
            recidiviz_id=retired_id,
            status=IdentityStatus.RETIRED,
            merged_into=surviving_id,
        )
        for external_id in contributed_ids:
            insert_external_id(
                recidiviz_id=retired_id,
                external_id=external_id,
                id_type=_ID_TYPE,
                is_active=False,
            )
        insert_merge_event(
            surviving_id=surviving_id, retired_id=retired_id, trigger=trigger
        )

    def _blockers(
        self, *, recidiviz_id: uuid.UUID, cluster_groups: dict[str, object]
    ) -> list[SplitBlocker]:
        with SessionFactory.using_database(self.database_key) as session:
            return find_split_blockers(
                session=session,
                recidiviz_id=recidiviz_id,
                cluster_groups=cluster_groups,
            )

    def test_moving_a_human_merges_contributed_id_blocks(self) -> None:
        recidiviz_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])
        self._insert_retired_contributor(
            surviving_id=recidiviz_id,
            contributed_ids=["A1", "B2"],
            trigger=MergeTrigger.MERGE_ENDPOINT,
        )

        self.assertEqual(
            [SplitBlocker.HUMAN_MERGE],
            self._blockers(
                recidiviz_id=recidiviz_id,
                cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
            ),
        )

    def test_splitting_off_exactly_the_human_merged_ids_blocks(self) -> None:
        """The cleanest way to undo a human merge is to move the merged-in
        ids wholesale to a new identity; that must block too."""
        recidiviz_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])
        self._insert_retired_contributor(
            surviving_id=recidiviz_id,
            contributed_ids=["B2"],
            trigger=MergeTrigger.MERGE_ENDPOINT,
        )

        self.assertEqual(
            [SplitBlocker.HUMAN_MERGE],
            self._blockers(
                recidiviz_id=recidiviz_id,
                cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
            ),
        )

    def test_human_merge_contribution_kept_by_the_original_does_not_block(
        self,
    ) -> None:
        recidiviz_id = self._insert_identity_with_ids(external_ids=["A1", "A2", "B2"])
        self._insert_retired_contributor(
            surviving_id=recidiviz_id,
            contributed_ids=["A1", "A2"],
            trigger=MergeTrigger.MERGE_ENDPOINT,
        )

        self.assertEqual(
            [],
            self._blockers(
                recidiviz_id=recidiviz_id,
                cluster_groups=_cluster_groups([["A1", "A2"], ["B2"]], []),
            ),
        )

    def test_import_triggered_merge_does_not_block(self) -> None:
        recidiviz_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])
        self._insert_retired_contributor(
            surviving_id=recidiviz_id,
            contributed_ids=["A1", "B2"],
            trigger=MergeTrigger.IMPORT,
        )

        self.assertEqual(
            [],
            self._blockers(
                recidiviz_id=recidiviz_id,
                cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
            ),
        )

    def test_product_app_attribute_blocks(self) -> None:
        recidiviz_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])
        insert_name(
            recidiviz_id=recidiviz_id,
            given_name="Ada",
            surname="HumanEdited",
            source_type=SourceType.PRODUCT_APP,
            source_product_app=ProductApp.ADMIN_PANEL,
        )

        self.assertEqual(
            [SplitBlocker.HUMAN_EDITED_ATTRIBUTES],
            self._blockers(
                recidiviz_id=recidiviz_id,
                cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
            ),
        )

    def test_admin_override_attribute_blocks(self) -> None:
        recidiviz_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])
        insert_name(
            recidiviz_id=recidiviz_id,
            given_name="Ada",
            surname="Overridden",
            source_type=SourceType.ADMIN_OVERRIDE,
        )

        self.assertEqual(
            [SplitBlocker.HUMAN_EDITED_ATTRIBUTES],
            self._blockers(
                recidiviz_id=recidiviz_id,
                cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
            ),
        )

    def test_external_data_system_attribute_does_not_block(self) -> None:
        recidiviz_id = self._insert_identity_with_ids(external_ids=["A1", "B2"])
        insert_name(
            recidiviz_id=recidiviz_id,
            given_name="Ada",
            surname="Imported",
            source_type=SourceType.EXTERNAL_DATA_SYSTEM,
        )

        self.assertEqual(
            [],
            self._blockers(
                recidiviz_id=recidiviz_id,
                cluster_groups=_cluster_groups([["A1"], ["B2"]], []),
            ),
        )
