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
"""Merge-candidate flagging pass of the Identity Service import: records each
cluster whose external ids matched two or more existing identities as a merge
candidate for human review.

Detecting those clusters happens upstream, in the import's planning step
(_plan_cluster_actions in import_processor.py); this pass only records what
the planner found. It creates or refreshes a PENDING merge_candidates row for
each detected cluster, and the import takes no further action on the cluster.

TODO(OBT-52611): This pass reads and writes the schema directly (the candidate
add plus the person_type, no_merge, and pending-candidate reads) instead of
going through IdentityServiceQuerier, the intended boundary between business
logic and the DB schema. Move these behind the querier, or an IdentityMergeQuerier
if the main querier grows unwieldy. Part of moving all of import_processing behind
the querier (OBT-52610); OBT-50191 tracks the same migration for the create pass.
"""
import datetime
import logging
import uuid
from collections import defaultdict

import attr
from sqlalchemy.orm import Session

from recidiviz.common.constants.identity import CandidateStatus, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.services.identity.import_processing.bq_snapshot_reader import (
    ClusterSnapshot,
)


@attr.define(frozen=True, kw_only=True)
class MergeToConsider:
    """An object that stores a set of Recidiviz ids that match a single
    cluster's external ids, and thus may represent the same person.

    Not to be confused with a MergeCandidate: several MergeToConsider
    objects can name the same set of Recidiviz ids, and flag_merge_candidates
    records one MergeCandidate per distinct set.
    """

    recidiviz_ids: frozenset[uuid.UUID] = attr.ib(
        validator=attr.validators.deep_iterable(
            member_validator=attr.validators.instance_of(uuid.UUID),
            iterable_validator=attr.validators.instance_of(frozenset),
        )
    )
    """The recidiviz_ids of the identities to consider merging. A frozenset
    because flag_merge_candidates dedupes merges to consider by set equality,
    using this set as a dict key."""

    snapshot: ClusterSnapshot = attr.ib(
        validator=attr.validators.instance_of(ClusterSnapshot)
    )
    """The cluster whose external ids match the identities."""

    def __attrs_post_init__(self) -> None:
        if len(self.recidiviz_ids) < 2:
            raise ValueError(
                f"MergeToConsider requires at least two identities; cluster "
                f"[{self.snapshot.cluster.identity_cluster_id}] has "
                f"[{len(self.recidiviz_ids)}]."
            )


def flag_merge_candidates(
    *,
    session: Session,
    tenant: Tenant,
    detected_at: datetime.datetime,
    merges_to_consider: list[MergeToConsider],
) -> None:
    """Creates or refreshes one PENDING merge_candidate row per distinct
    set of identities to consider merging.

    Args:
        session: Session the candidate writes are staged on.
        tenant: The tenant whose import run detected the merges.
        detected_at: The run's start time, written to detected_at_utc.
        merges_to_consider: One entry per cluster whose external ids matched
            two or more existing identities, suggesting those identities are
            the same person.
    """
    # There may be multiple MergeToConsider objects in merges_to_consider with
    # the same set of recidiviz_ids. Group the ClusterSnapshot objects they
    # contain by those sets.
    snapshots_by_recidiviz_ids: dict[
        frozenset[uuid.UUID], list[ClusterSnapshot]
    ] = defaultdict(list)
    for merge_to_consider in merges_to_consider:
        snapshots_by_recidiviz_ids[merge_to_consider.recidiviz_ids].append(
            merge_to_consider.snapshot
        )

    all_recidiviz_ids = frozenset().union(*snapshots_by_recidiviz_ids.keys())
    person_types_by_recidiviz_id = _person_types_by_recidiviz_id(
        session=session, recidiviz_ids=all_recidiviz_ids
    )

    no_merge_recidiviz_id_pairs = _no_merge_recidiviz_id_pairs(
        session=session, recidiviz_ids=all_recidiviz_ids
    )

    pending_merge_candidates_by_recidiviz_ids = (
        _pending_merge_candidates_by_recidiviz_ids(session=session, tenant=tenant)
    )

    created_count = 0
    refreshed_count = 0
    for snapshots_recidiviz_ids, snapshots in snapshots_by_recidiviz_ids.items():
        review_context = _build_review_context(
            snapshots=snapshots,
            recidiviz_ids=snapshots_recidiviz_ids,
            person_types_by_recidiviz_id=person_types_by_recidiviz_id,
            no_merge_pairs=no_merge_recidiviz_id_pairs,
        )
        pending_merge_candidate = pending_merge_candidates_by_recidiviz_ids.get(
            snapshots_recidiviz_ids
        )
        if pending_merge_candidate is not None:
            # An earlier run filed this same question and no reviewer has
            # resolved it yet. Refreshing detected_at_utc marks the candidate
            # as still proposed this run; the stale-candidate cleanup
            # (TODO(OBT-37730)) will retire candidates that stop being
            # refreshed because no current cluster proposes them anymore.
            # Overwriting review_context replaces the stored context with this
            # run's, rather than accumulating past runs'.
            pending_merge_candidate.detected_at_utc = detected_at
            pending_merge_candidate.review_context = review_context
            refreshed_count += 1
            continue
        new_merge_candidate = schema.MergeCandidate(
            tenant=tenant,
            detected_at_utc=detected_at,
            review_context=review_context,
            status=CandidateStatus.PENDING,
        )
        session.add(new_merge_candidate)
        # Assigns candidate.id so the identity link rows can reference it.
        session.flush()
        session.add_all(
            [
                schema.MergeCandidateIdentity(
                    merge_candidate_id=new_merge_candidate.id, recidiviz_id=recidiviz_id
                )
                for recidiviz_id in sorted(snapshots_recidiviz_ids)
            ]
        )
        created_count += 1
    logging.info(
        "Flagged [%s] merge candidates for tenant [%s]: [%s] new, [%s] "
        "re-detected with detected_at_utc refreshed.",
        len(snapshots_by_recidiviz_ids),
        tenant.value,
        created_count,
        refreshed_count,
    )


def _person_types_by_recidiviz_id(
    *, session: Session, recidiviz_ids: frozenset[uuid.UUID]
) -> dict[uuid.UUID, PersonType]:
    """Returns the person_type of every identity represented by the
    given recidiviz_ids, keyed by recidiviz_id."""
    rows = (
        session.query(schema.Identity.recidiviz_id, schema.Identity.person_type)
        .filter(schema.Identity.recidiviz_id.in_(recidiviz_ids))
        .all()
    )
    return dict(rows)


def _no_merge_recidiviz_id_pairs(
    *, session: Session, recidiviz_ids: frozenset[uuid.UUID]
) -> set[tuple[uuid.UUID, uuid.UUID]]:
    """Returns the no_merge pairs whose two recidiviz_ids are both in the
    given set.

    These pairs are the record of a human decision that two identities are
    different people and must never be merged.
    """
    rows = (
        session.query(schema.NoMerge.recidiviz_id_a, schema.NoMerge.recidiviz_id_b)
        .filter(
            schema.NoMerge.recidiviz_id_a.in_(recidiviz_ids),
            schema.NoMerge.recidiviz_id_b.in_(recidiviz_ids),
        )
        .all()
    )
    return set(rows)


def _pending_merge_candidates_by_recidiviz_ids(
    *, session: Session, tenant: Tenant
) -> dict[frozenset[uuid.UUID], schema.MergeCandidate]:
    """Returns the tenant's PENDING merge candidates keyed by the set of
    recidiviz_ids each would merge.

    Only PENDING candidates are eligible to be refreshed: once a reviewer
    resolves a candidate, a later re-detection of the same identity set files
    a fresh row rather than reopening the decided one."""
    pending_merge_candidates = (
        session.query(schema.MergeCandidate)
        .filter(
            schema.MergeCandidate.tenant == tenant,
            schema.MergeCandidate.status == CandidateStatus.PENDING,
        )
        .all()
    )

    if not pending_merge_candidates:
        return {}

    pending_merge_candidate_identity_rows = (
        session.query(
            schema.MergeCandidateIdentity.merge_candidate_id,
            schema.MergeCandidateIdentity.recidiviz_id,
        )
        .filter(
            schema.MergeCandidateIdentity.merge_candidate_id.in_(
                [candidate.id for candidate in pending_merge_candidates]
            )
        )
        .all()
    )
    recidiviz_ids_by_merge_candidate_id: dict[int, set[uuid.UUID]] = defaultdict(set)
    for merge_candidate_id, recidiviz_id in pending_merge_candidate_identity_rows:
        recidiviz_ids_by_merge_candidate_id[merge_candidate_id].add(recidiviz_id)
    return {
        frozenset(recidiviz_ids_by_merge_candidate_id[candidate.id]): candidate
        for candidate in pending_merge_candidates
    }


def _build_review_context(
    *,
    snapshots: list[ClusterSnapshot],
    recidiviz_ids: frozenset[uuid.UUID],
    person_types_by_recidiviz_id: dict[uuid.UUID, PersonType],
    no_merge_pairs: set[tuple[uuid.UUID, uuid.UUID]],
) -> dict[str, object]:
    """Returns the JSONB review context stored on a merge candidate: each
    identity's person_type, the explicit no-merge pairs among the identities,
    and the external ids of the clusters that proposed the merge this run."""
    no_merge_pairs_in_set = sorted(
        (recidiviz_id_a, recidiviz_id_b)
        for recidiviz_id_a, recidiviz_id_b in no_merge_pairs
        if recidiviz_id_a in recidiviz_ids and recidiviz_id_b in recidiviz_ids
    )
    return {
        "person_type_by_recidiviz_id": {
            str(recidiviz_id): person_types_by_recidiviz_id[recidiviz_id].value
            for recidiviz_id in sorted(recidiviz_ids)
        },
        "no_merge_pair_recidiviz_ids": [
            [str(recidiviz_id_a), str(recidiviz_id_b)]
            for recidiviz_id_a, recidiviz_id_b in no_merge_pairs_in_set
        ],
        "clusters": [
            {
                "identity_cluster_id": snapshot.cluster.identity_cluster_id,
                "external_ids": [
                    {
                        "id_type": external_id.id_type,
                        "external_id": external_id.external_id,
                    }
                    for external_id in snapshot.cluster.external_ids
                ],
            }
            for snapshot in snapshots
        ],
    }
