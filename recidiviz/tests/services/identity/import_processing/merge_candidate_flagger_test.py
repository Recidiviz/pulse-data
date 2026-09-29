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
"""Tests for the import's merge candidate flagger. flag_merge_candidates is
called directly with hand-built MergeToConsider; the import-level step that
builds MergeToConsider from clusters and routes them here is covered in
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
from recidiviz.services.identity.import_processing.merge_candidate_flagger import (
    MergeToConsider,
    flag_merge_candidates,
)
from recidiviz.tests.services.identity.test_utils import (
    CREATED,
    IdentityDatabaseTestCase,
    insert_identity,
    insert_merge_candidate,
    insert_no_merge,
)
from recidiviz.utils.types import assert_type

_ID_TYPE = IdentifierType.US_OZ_LOTR_ID
_RUN_1_TIME = datetime.datetime(2026, 9, 10, 12, 0, tzinfo=datetime.timezone.utc)
_RUN_2_TIME = datetime.datetime(2026, 9, 11, 12, 0, tzinfo=datetime.timezone.utc)


def _proposing_snapshot(*, external_ids: list[str]) -> ClusterSnapshot:
    """Builds a ClusterSnapshot for the cluster whose external ids matched the
    identities to consider merging, recorded in the candidate's review
    context."""
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
            tenant=Tenant.US_OZ, given_name="Ada", surname="Spanning"
        ),
    )
    return ClusterSnapshot(cluster=cluster, stored_cluster_hash=cluster.cluster_hash)


class MergeToConsiderTest(unittest.TestCase):
    """Tests for MergeToConsider validation."""

    def test_requires_at_least_two_identities(self) -> None:
        snapshot = _proposing_snapshot(external_ids=["A1"])
        expected_message = (
            f"MergeToConsider requires at least two identities; cluster "
            f"[{snapshot.cluster.identity_cluster_id}] has [1]."
        )
        with self.assertRaisesRegex(ValueError, f"^{re.escape(expected_message)}$"):
            MergeToConsider(recidiviz_ids=frozenset({uuid.uuid4()}), snapshot=snapshot)


@pytest.mark.uses_db
class FlagMergeCandidatesTest(IdentityDatabaseTestCase):
    """Tests for flag_merge_candidates: one PENDING merge_candidates row per
    distinct set of identities to consider merging, created or refreshed."""

    def _flag(
        self,
        *merges_to_consider: MergeToConsider,
        detected_at: datetime.datetime,
    ) -> None:
        """Calls flag_merge_candidates the way one import run does: a single
        session, the US_OZ tenant, and the run's start time."""
        with SessionFactory.using_database(self.database_key) as session:
            flag_merge_candidates(
                session=session,
                tenant=Tenant.US_OZ,
                detected_at=detected_at,
                merges_to_consider=list(merges_to_consider),
            )

    def _candidate_rows(self) -> list[dict[str, object]]:
        """Returns every merge candidate with the set of recidiviz_ids it
        would merge, ordered by insertion."""
        with SessionFactory.using_database(self.database_key) as session:
            candidates = (
                session.query(schema.MergeCandidate)
                .order_by(schema.MergeCandidate.id)
                .all()
            )
            rows = []
            for candidate in candidates:
                linked_recidiviz_ids = {
                    recidiviz_id
                    for (recidiviz_id,) in session.query(
                        schema.MergeCandidateIdentity.recidiviz_id
                    ).filter(
                        schema.MergeCandidateIdentity.merge_candidate_id == candidate.id
                    )
                }
                rows.append(
                    {
                        "tenant": candidate.tenant,
                        "status": candidate.status,
                        "detected_at_utc": candidate.detected_at_utc,
                        "recidiviz_ids": linked_recidiviz_ids,
                        "review_context": candidate.review_context,
                    }
                )
        return rows

    def test_new_identity_set_files_a_pending_candidate(self) -> None:
        first_id = uuid.uuid4()
        second_id = uuid.uuid4()
        insert_identity(recidiviz_id=first_id)
        insert_identity(recidiviz_id=second_id)
        snapshot = _proposing_snapshot(external_ids=["A1", "B2"])

        self._flag(
            MergeToConsider(
                recidiviz_ids=frozenset({first_id, second_id}), snapshot=snapshot
            ),
            detected_at=_RUN_1_TIME,
        )

        self.assertEqual(
            [
                {
                    "tenant": Tenant.US_OZ,
                    "status": CandidateStatus.PENDING,
                    "detected_at_utc": _RUN_1_TIME,
                    "recidiviz_ids": {first_id, second_id},
                    "review_context": {
                        "person_type_by_recidiviz_id": {
                            str(first_id): "JII",
                            str(second_id): "JII",
                        },
                        "no_merge_pair_recidiviz_ids": [],
                        "clusters": [
                            {
                                "identity_cluster_id": snapshot.cluster.identity_cluster_id,
                                "external_ids": [
                                    {
                                        "id_type": _ID_TYPE.value,
                                        "external_id": "A1",
                                    },
                                    {
                                        "id_type": _ID_TYPE.value,
                                        "external_id": "B2",
                                    },
                                ],
                            }
                        ],
                    },
                }
            ],
            self._candidate_rows(),
        )

    def test_redetected_set_keeps_its_row_and_refreshes_detected_at(self) -> None:
        first_id = uuid.uuid4()
        second_id = uuid.uuid4()
        insert_identity(recidiviz_id=first_id)
        insert_identity(recidiviz_id=second_id)
        to_merge = MergeToConsider(
            recidiviz_ids=frozenset({first_id, second_id}),
            snapshot=_proposing_snapshot(external_ids=["A1", "B2"]),
        )

        self._flag(to_merge, detected_at=_RUN_1_TIME)
        self._flag(to_merge, detected_at=_RUN_2_TIME)

        row = one(self._candidate_rows())
        self.assertEqual(CandidateStatus.PENDING, row["status"])
        self.assertEqual(_RUN_2_TIME, row["detected_at_utc"])

    def test_resolved_candidate_is_not_reused_for_a_redetection(self) -> None:
        """Dedupe considers only PENDING candidates: once a reviewer resolves a
        candidate, a re-detection of the same identity set files a fresh row."""
        first_id = uuid.uuid4()
        second_id = uuid.uuid4()
        insert_identity(recidiviz_id=first_id)
        insert_identity(recidiviz_id=second_id)
        insert_merge_candidate(
            recidiviz_ids=[first_id, second_id],
            status=CandidateStatus.RESOLVED,
        )

        self._flag(
            MergeToConsider(
                recidiviz_ids=frozenset({first_id, second_id}),
                snapshot=_proposing_snapshot(external_ids=["A1", "B2"]),
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
        self.assertEqual({first_id, second_id}, rows[1]["recidiviz_ids"])

    def test_two_clusters_proposing_the_same_set_share_one_candidate(self) -> None:
        first_id = uuid.uuid4()
        second_id = uuid.uuid4()
        insert_identity(recidiviz_id=first_id)
        insert_identity(recidiviz_id=second_id)
        first_snapshot = _proposing_snapshot(external_ids=["A1", "B1"])
        second_snapshot = _proposing_snapshot(external_ids=["A2", "B2"])

        self._flag(
            MergeToConsider(
                recidiviz_ids=frozenset({first_id, second_id}),
                snapshot=first_snapshot,
            ),
            MergeToConsider(
                recidiviz_ids=frozenset({first_id, second_id}),
                snapshot=second_snapshot,
            ),
            detected_at=_RUN_1_TIME,
        )

        row = one(self._candidate_rows())
        self.assertEqual({first_id, second_id}, row["recidiviz_ids"])
        # The candidate's context names both proposing clusters.
        self.assertEqual(
            [
                {
                    "identity_cluster_id": first_snapshot.cluster.identity_cluster_id,
                    "external_ids": [
                        {"id_type": _ID_TYPE.value, "external_id": "A1"},
                        {"id_type": _ID_TYPE.value, "external_id": "B1"},
                    ],
                },
                {
                    "identity_cluster_id": second_snapshot.cluster.identity_cluster_id,
                    "external_ids": [
                        {"id_type": _ID_TYPE.value, "external_id": "A2"},
                        {"id_type": _ID_TYPE.value, "external_id": "B2"},
                    ],
                },
            ],
            assert_type(row["review_context"], dict)["clusters"],
        )

    def test_different_identity_sets_get_separate_candidates(self) -> None:
        first_id = uuid.uuid4()
        second_id = uuid.uuid4()
        third_id = uuid.uuid4()
        insert_identity(recidiviz_id=first_id)
        insert_identity(recidiviz_id=second_id)
        insert_identity(recidiviz_id=third_id)

        self._flag(
            MergeToConsider(
                recidiviz_ids=frozenset({first_id, second_id}),
                snapshot=_proposing_snapshot(external_ids=["A1", "B1"]),
            ),
            MergeToConsider(
                recidiviz_ids=frozenset({first_id, third_id}),
                snapshot=_proposing_snapshot(external_ids=["A2", "C1"]),
            ),
            detected_at=_RUN_1_TIME,
        )

        self.assertEqual(
            [{first_id, second_id}, {first_id, third_id}],
            [row["recidiviz_ids"] for row in self._candidate_rows()],
        )

    def test_no_merge_pair_among_the_identities_is_recorded(self) -> None:
        first_id = uuid.uuid4()
        second_id = uuid.uuid4()
        insert_identity(recidiviz_id=first_id)
        insert_identity(recidiviz_id=second_id)
        insert_no_merge(recidiviz_id_a=first_id, recidiviz_id_b=second_id)

        self._flag(
            MergeToConsider(
                recidiviz_ids=frozenset({first_id, second_id}),
                snapshot=_proposing_snapshot(external_ids=["A1", "B2"]),
            ),
            detected_at=_RUN_1_TIME,
        )

        row = one(self._candidate_rows())
        lower_id, higher_id = sorted([first_id, second_id])
        self.assertEqual(
            [[str(lower_id), str(higher_id)]],
            assert_type(row["review_context"], dict)["no_merge_pair_recidiviz_ids"],
        )

    def test_each_identitys_person_type_is_recorded(self) -> None:
        jii_id = uuid.uuid4()
        staff_id = uuid.uuid4()
        insert_identity(recidiviz_id=jii_id, person_type=PersonType.JII)
        insert_identity(recidiviz_id=staff_id, person_type=PersonType.STAFF)

        self._flag(
            MergeToConsider(
                recidiviz_ids=frozenset({jii_id, staff_id}),
                snapshot=_proposing_snapshot(external_ids=["A1", "B2"]),
            ),
            detected_at=_RUN_1_TIME,
        )

        row = one(self._candidate_rows())
        self.assertEqual(
            {str(jii_id): "JII", str(staff_id): "STAFF"},
            assert_type(row["review_context"], dict)["person_type_by_recidiviz_id"],
        )
