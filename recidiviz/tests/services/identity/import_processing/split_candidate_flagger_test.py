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
"""Tests for the import's split candidate flagger. flag_split_candidates is
called directly with hand-built SplitToConsider; the import-level step that
builds SplitToConsider from clusters and routes them here is covered in
import_processor_test.py."""
import datetime
import re
import unittest
import uuid

import pytest
from more_itertools import one

from recidiviz.common.constants.identity import (
    CandidateStatus,
    IdentifierType,
    PersonType,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
    IdentityClusterName,
)
from recidiviz.services.identity.import_processing.bq_snapshot_reader import (
    ClusterSnapshot,
)
from recidiviz.services.identity.import_processing.split_candidate_flagger import (
    SplitToConsider,
    flag_split_candidates,
)
from recidiviz.tests.services.identity.test_utils import (
    CREATED,
    IdentityDatabaseTestCase,
    insert_external_id,
    insert_identity,
    insert_split_candidate,
)
from recidiviz.utils.types import assert_type

_ID_TYPE = IdentifierType.US_OZ_LOTR_ID
_RUN_1_TIME = datetime.datetime(2026, 9, 15, 12, 0, tzinfo=datetime.timezone.utc)
_RUN_2_TIME = datetime.datetime(2026, 9, 16, 12, 0, tzinfo=datetime.timezone.utc)


def _matching_snapshot(*, external_ids: list[str]) -> ClusterSnapshot:
    """Builds a ClusterSnapshot whose cluster carries the given external ids,
    so a test can make several clusters match one existing identity."""
    cluster = IdentityCluster(
        tenant=Tenant.US_OZ,
        person_type=PersonType.JII,
        external_ids=tuple(
            IdentityClusterExternalId(
                tenant=Tenant.US_OZ, external_id=external_id, id_type=_ID_TYPE.value
            )
            for external_id in external_ids
        ),
        name=IdentityClusterName(
            tenant=Tenant.US_OZ, given_name="Ada", surname="Fractured"
        ),
    )
    return ClusterSnapshot(cluster=cluster, stored_cluster_hash=cluster.cluster_hash)


def _external_id_json(external_id: str) -> dict[str, str]:
    """Returns one external id as it appears in the cluster_groups JSONB."""
    return {"id_type": _ID_TYPE.value, "external_id": external_id}


class SplitToConsiderTest(unittest.TestCase):
    """Tests for SplitToConsider validation."""

    def test_requires_at_least_two_clusters(self) -> None:
        recidiviz_id = uuid.uuid4()
        expected_message = (
            f"SplitToConsider requires at least two clusters; identity "
            f"[{recidiviz_id}] has [1]."
        )
        with self.assertRaisesRegex(ValueError, f"^{re.escape(expected_message)}$"):
            SplitToConsider(
                recidiviz_id=recidiviz_id,
                snapshots=(_matching_snapshot(external_ids=["A1"]),),
            )


@pytest.mark.uses_db
class FlagSplitCandidatesTest(IdentityDatabaseTestCase):
    """Tests for flag_split_candidates: one PENDING split_candidates row per
    identity to consider splitting, created or refreshed."""

    def _flag(
        self,
        *splits_to_consider: SplitToConsider,
        detected_at: datetime.datetime,
    ) -> None:
        """Calls flag_split_candidates the way one import run does: a single
        session, the US_OZ tenant, and the run's start time."""
        with SessionFactory.using_database(self.database_key) as session:
            flag_split_candidates(
                session=session,
                tenant=Tenant.US_OZ,
                detected_at=detected_at,
                splits_to_consider=list(splits_to_consider),
            )

    def _candidate_rows(self) -> list[dict[str, object]]:
        """Returns every split candidate, ordered by insertion."""
        with SessionFactory.using_database(self.database_key) as session:
            candidates = (
                session.query(schema.SplitCandidate)
                .order_by(schema.SplitCandidate.id)
                .all()
            )
            return [
                {
                    "tenant": candidate.tenant,
                    "status": candidate.status,
                    "detected_at_utc": candidate.detected_at_utc,
                    "recidiviz_id": candidate.recidiviz_id,
                    "cluster_groups": candidate.cluster_groups,
                }
                for candidate in candidates
            ]

    def _insert_fractured_identity(self, *, external_ids: list[str]) -> uuid.UUID:
        """Inserts an identity holding the given external ids as active,
        returning its recidiviz_id."""
        recidiviz_id = uuid.uuid4()
        insert_identity(recidiviz_id=recidiviz_id)
        for external_id in external_ids:
            insert_external_id(
                recidiviz_id=recidiviz_id, external_id=external_id, id_type=_ID_TYPE
            )
        return recidiviz_id

    def test_fractured_identity_files_a_pending_candidate(self) -> None:
        recidiviz_id = self._insert_fractured_identity(external_ids=["A1", "B2"])
        # F9 belongs to no identity here; a real cluster can mix one identity's
        # ids with others', and the foreign id must not appear in the group.
        first_snapshot = _matching_snapshot(external_ids=["A1", "F9"])
        second_snapshot = _matching_snapshot(external_ids=["B2"])
        sorted_first, sorted_second = sorted(
            [first_snapshot, second_snapshot],
            key=lambda snapshot: snapshot.cluster.identity_cluster_id,
        )

        self._flag(
            SplitToConsider(
                recidiviz_id=recidiviz_id,
                snapshots=(first_snapshot, second_snapshot),
            ),
            detected_at=_RUN_1_TIME,
        )

        expected_ids_by_cluster_id = {
            first_snapshot.cluster.identity_cluster_id: [_external_id_json("A1")],
            second_snapshot.cluster.identity_cluster_id: [_external_id_json("B2")],
        }
        self.assertEqual(
            [
                {
                    "tenant": Tenant.US_OZ,
                    "status": CandidateStatus.PENDING,
                    "detected_at_utc": _RUN_1_TIME,
                    "recidiviz_id": recidiviz_id,
                    "cluster_groups": {
                        "cluster_groups": [
                            {
                                "identity_cluster_id": sorted_first.cluster.identity_cluster_id,
                                "external_ids": expected_ids_by_cluster_id[
                                    sorted_first.cluster.identity_cluster_id
                                ],
                            },
                            {
                                "identity_cluster_id": sorted_second.cluster.identity_cluster_id,
                                "external_ids": expected_ids_by_cluster_id[
                                    sorted_second.cluster.identity_cluster_id
                                ],
                            },
                        ],
                        "unclustered_external_ids": [],
                    },
                }
            ],
            self._candidate_rows(),
        )

    def test_unclustered_active_external_id_is_recorded(self) -> None:
        recidiviz_id = self._insert_fractured_identity(external_ids=["A1", "B2", "C3"])

        self._flag(
            SplitToConsider(
                recidiviz_id=recidiviz_id,
                snapshots=(
                    _matching_snapshot(external_ids=["A1"]),
                    _matching_snapshot(external_ids=["B2"]),
                ),
            ),
            detected_at=_RUN_1_TIME,
        )

        row = one(self._candidate_rows())
        self.assertEqual(
            [_external_id_json("C3")],
            assert_type(row["cluster_groups"], dict)["unclustered_external_ids"],
        )

    def test_inactive_external_id_appears_in_no_group(self) -> None:
        recidiviz_id = self._insert_fractured_identity(external_ids=["A1", "B2"])
        # Inactive rows are split residue; they no longer tie the identity to
        # the id, so a cluster carrying one contributes nothing to its group.
        insert_external_id(
            recidiviz_id=recidiviz_id,
            external_id="OLD9",
            id_type=_ID_TYPE,
            is_active=False,
        )

        self._flag(
            SplitToConsider(
                recidiviz_id=recidiviz_id,
                snapshots=(
                    _matching_snapshot(external_ids=["A1", "OLD9"]),
                    _matching_snapshot(external_ids=["B2"]),
                ),
            ),
            detected_at=_RUN_1_TIME,
        )

        row = one(self._candidate_rows())
        cluster_groups = assert_type(row["cluster_groups"], dict)
        all_group_external_ids = [
            external_id
            for group in cluster_groups["cluster_groups"]
            for external_id in group["external_ids"]
        ]
        self.assertEqual(
            [_external_id_json("A1"), _external_id_json("B2")],
            sorted(all_group_external_ids, key=lambda e: e["external_id"]),
        )
        self.assertEqual([], cluster_groups["unclustered_external_ids"])

    def test_redetected_identity_keeps_its_row_and_refreshes_detected_at(self) -> None:
        recidiviz_id = self._insert_fractured_identity(external_ids=["A1", "B2"])
        split_to_consider = SplitToConsider(
            recidiviz_id=recidiviz_id,
            snapshots=(
                _matching_snapshot(external_ids=["A1"]),
                _matching_snapshot(external_ids=["B2"]),
            ),
        )

        self._flag(split_to_consider, detected_at=_RUN_1_TIME)
        self._flag(split_to_consider, detected_at=_RUN_2_TIME)

        row = one(self._candidate_rows())
        self.assertEqual(CandidateStatus.PENDING, row["status"])
        self.assertEqual(_RUN_2_TIME, row["detected_at_utc"])

    def test_redetection_overwrites_cluster_groups(self) -> None:
        recidiviz_id = self._insert_fractured_identity(external_ids=["A1", "B2", "C3"])
        self._flag(
            SplitToConsider(
                recidiviz_id=recidiviz_id,
                snapshots=(
                    _matching_snapshot(external_ids=["A1"]),
                    _matching_snapshot(external_ids=["B2"]),
                ),
            ),
            detected_at=_RUN_1_TIME,
        )
        row = one(self._candidate_rows())
        self.assertEqual(
            [_external_id_json("C3")],
            assert_type(row["cluster_groups"], dict)["unclustered_external_ids"],
        )

        # The next run fractures the same identity differently: C3,
        # unclustered in the first run, now clusters with A1.
        self._flag(
            SplitToConsider(
                recidiviz_id=recidiviz_id,
                snapshots=(
                    _matching_snapshot(external_ids=["A1", "C3"]),
                    _matching_snapshot(external_ids=["B2"]),
                ),
            ),
            detected_at=_RUN_2_TIME,
        )

        row = one(self._candidate_rows())
        cluster_groups = assert_type(row["cluster_groups"], dict)
        self.assertEqual(
            [
                [_external_id_json("B2")],
                [_external_id_json("A1"), _external_id_json("C3")],
            ],
            sorted(
                (group["external_ids"] for group in cluster_groups["cluster_groups"]),
                key=len,
            ),
        )
        self.assertEqual([], cluster_groups["unclustered_external_ids"])

    def test_resolved_candidate_is_not_reused_for_a_redetection(self) -> None:
        """Refresh considers only PENDING candidates: once a reviewer resolves
        a candidate, a re-detection of the same identity files a fresh row."""
        recidiviz_id = self._insert_fractured_identity(external_ids=["A1", "B2"])
        insert_split_candidate(
            recidiviz_id=recidiviz_id, status=CandidateStatus.RESOLVED
        )

        self._flag(
            SplitToConsider(
                recidiviz_id=recidiviz_id,
                snapshots=(
                    _matching_snapshot(external_ids=["A1"]),
                    _matching_snapshot(external_ids=["B2"]),
                ),
            ),
            detected_at=_RUN_1_TIME,
        )

        rows = self._candidate_rows()
        self.assertEqual(
            [
                (CandidateStatus.RESOLVED, CREATED),
                (CandidateStatus.PENDING, _RUN_1_TIME),
            ],
            [(row["status"], row["detected_at_utc"]) for row in rows],
        )

    def test_two_fractured_identities_get_separate_candidates(self) -> None:
        first_id = self._insert_fractured_identity(external_ids=["A1", "A2"])
        second_id = self._insert_fractured_identity(external_ids=["B1", "B2"])

        self._flag(
            SplitToConsider(
                recidiviz_id=first_id,
                snapshots=(
                    _matching_snapshot(external_ids=["A1"]),
                    _matching_snapshot(external_ids=["A2"]),
                ),
            ),
            SplitToConsider(
                recidiviz_id=second_id,
                snapshots=(
                    _matching_snapshot(external_ids=["B1"]),
                    _matching_snapshot(external_ids=["B2"]),
                ),
            ),
            detected_at=_RUN_1_TIME,
        )

        self.assertEqual(
            [first_id, second_id],
            [row["recidiviz_id"] for row in self._candidate_rows()],
        )
