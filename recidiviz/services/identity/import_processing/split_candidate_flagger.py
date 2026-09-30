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
"""Split-candidate flagging pass of the Identity Service import: records each
existing identity whose active external ids appear in two or more of the run's
clusters as a split candidate for human review.

Detecting those identities happens upstream, in the import's orchestration
(_splits_to_consider in import_processor.py); this pass only records what was
detected. It creates or refreshes a PENDING split_candidates row for each
detected identity, and the import takes no further action on the identity's
clusters. Each candidate's cluster_groups payload records the partition of
the identity's external ids that the proposed split would produce (defined in
_build_cluster_groups).
"""
import datetime
import logging
import uuid
from collections import defaultdict

import attr
from sqlalchemy.orm import Session

from recidiviz.common.constants.identity import CandidateStatus
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.services.identity.import_processing.bq_snapshot_reader import (
    ClusterSnapshot,
)


@attr.define(frozen=True, kw_only=True)
class SplitToConsider:
    """An object that stores a Recidiviz id that matches multiple different
    clusters' external ids, and thus may represent more than one person.

    Not to be confused with a SplitCandidate: a SplitToConsider is one run's
    in-memory detection result, and flag_split_candidates records it as (or
    refreshes) the identity's one PENDING SplitCandidate row.
    """

    recidiviz_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))
    """The recidiviz_id of the identity to consider splitting."""

    snapshots: tuple[ClusterSnapshot, ...] = attr.ib(
        validator=attr.validators.deep_iterable(
            member_validator=attr.validators.instance_of(ClusterSnapshot),
            iterable_validator=attr.validators.instance_of(tuple),
        )
    )
    """The clusters whose external ids matched the identity."""

    def __attrs_post_init__(self) -> None:
        if len(self.snapshots) < 2:
            raise ValueError(
                f"SplitToConsider requires at least two clusters; identity "
                f"[{self.recidiviz_id}] has [{len(self.snapshots)}]."
            )


def flag_split_candidates(
    *,
    session: Session,
    tenant: Tenant,
    detected_at: datetime.datetime,
    splits_to_consider: list[SplitToConsider],
) -> None:
    """Creates or refreshes one PENDING split_candidate row per identity to
    consider splitting.

    Args:
        session: Session the candidate writes are staged on.
        tenant: The tenant whose import run detected the splits.
        detected_at: The run's start time, written to detected_at_utc.
        splits_to_consider: One entry per identity whose active external ids
            appear in two or more of the run's clusters.
    """
    all_recidiviz_ids = frozenset(
        split_to_consider.recidiviz_id for split_to_consider in splits_to_consider
    )

    active_external_ids_by_recidiviz_id = _active_external_ids_by_recidiviz_id(
        session=session, recidiviz_ids=all_recidiviz_ids
    )

    pending_split_candidates_by_recidiviz_id = (
        _pending_split_candidates_by_recidiviz_id(
            session=session, tenant=tenant, recidiviz_ids=all_recidiviz_ids
        )
    )

    created_count = 0
    refreshed_count = 0
    for split_to_consider in splits_to_consider:
        cluster_groups = _build_cluster_groups(
            snapshots=split_to_consider.snapshots,
            active_external_ids=active_external_ids_by_recidiviz_id[
                split_to_consider.recidiviz_id
            ],
        )
        pending_split_candidate = pending_split_candidates_by_recidiviz_id.get(
            split_to_consider.recidiviz_id
        )
        if pending_split_candidate is not None:
            # An earlier run filed this same question and no reviewer has
            # resolved it yet. Refreshing detected_at_utc marks the candidate
            # as still proposed this run; the stale-candidate cleanup
            # (TODO(OBT-37730)) will retire candidates that stop being
            # refreshed because the identity is no longer fractured.
            # Overwriting cluster_groups replaces the stored grouping with
            # this run's, rather than accumulating past runs'.
            pending_split_candidate.detected_at_utc = detected_at
            pending_split_candidate.cluster_groups = cluster_groups
            refreshed_count += 1
            continue
        session.add(
            schema.SplitCandidate(
                tenant=tenant,
                recidiviz_id=split_to_consider.recidiviz_id,
                cluster_groups=cluster_groups,
                detected_at_utc=detected_at,
                status=CandidateStatus.PENDING,
            )
        )
        created_count += 1
    logging.info(
        "Flagged [%s] split candidates for tenant [%s]: [%s] new, [%s] "
        "re-detected with detected_at_utc refreshed.",
        len(splits_to_consider),
        tenant.value,
        created_count,
        refreshed_count,
    )


def _active_external_ids_by_recidiviz_id(
    *, session: Session, recidiviz_ids: frozenset[uuid.UUID]
) -> dict[uuid.UUID, set[tuple[str, str]]]:
    """Returns each given identity's active external ids as
    (id_type value, external_id) pairs, keyed by recidiviz_id."""
    rows = (
        session.query(
            schema.ExternalId.recidiviz_id,
            schema.ExternalId.id_type,
            schema.ExternalId.external_id,
        )
        .filter(
            schema.ExternalId.recidiviz_id.in_(recidiviz_ids),
            schema.ExternalId.is_active.is_(True),
        )
        .all()
    )
    active_external_ids_by_recidiviz_id: dict[
        uuid.UUID, set[tuple[str, str]]
    ] = defaultdict(set)
    for recidiviz_id, id_type, external_id in rows:
        active_external_ids_by_recidiviz_id[recidiviz_id].add(
            (id_type.value, external_id)
        )
    # A plain dict, so looking up an identity the query did not find fails
    # loudly instead of defaulting to an empty set: detection matched every
    # identity here through its active external ids, so a miss is a bug.
    return dict(active_external_ids_by_recidiviz_id)


def _pending_split_candidates_by_recidiviz_id(
    *, session: Session, tenant: Tenant, recidiviz_ids: frozenset[uuid.UUID]
) -> dict[uuid.UUID, schema.SplitCandidate]:
    """Returns the tenant's PENDING split candidates for the given
    recidiviz_ids, keyed by recidiviz_id.

    Only PENDING candidates are eligible to be refreshed: once a reviewer
    resolves a candidate, a later re-detection of the same identity files a
    fresh row rather than reopening the decided one."""
    pending_split_candidates = (
        session.query(schema.SplitCandidate)
        .filter(
            schema.SplitCandidate.tenant == tenant,
            schema.SplitCandidate.status == CandidateStatus.PENDING,
            schema.SplitCandidate.recidiviz_id.in_(recidiviz_ids),
        )
        .all()
    )
    return {
        pending_split_candidate.recidiviz_id: pending_split_candidate
        for pending_split_candidate in pending_split_candidates
    }


def _build_cluster_groups(
    *,
    snapshots: tuple[ClusterSnapshot, ...],
    active_external_ids: set[tuple[str, str]],
) -> dict[str, object]:
    """Returns the JSONB cluster_groups stored on a split candidate: the
    partition of the identity's active external ids that the proposed split
    would produce.

    A cluster is the clustering pipeline's claim that a set of external ids
    belongs to one person, so one identity's ids landing in two or more
    clusters is a claim that the identity is two or more people. Each cluster
    group is one side of that claim: the identity's own active external ids
    that landed together in one cluster. A split would give each group to its
    own identity.

    For example, suppose the identity holds active ids A1, A2, B2, and C9,
    and its matching clusters this run are K1 = {A1, A2, F5} and K2 = {B2}:

    - K1's group is {A1, A2}. F5 is omitted: it belongs to some other
      identity (or to none yet), so it is not this identity's to move.
    - K2's group is {B2}.
    - C9 appears in no current cluster. That is not evidence it belongs to
      no one, so a split would leave it with the identity; it is recorded
      under unclustered_external_ids so a reviewer sees that.

    Groups are sorted by identity_cluster_id and ids by (id_type,
    external_id), so re-detection of unchanged state produces an identical
    value.
    """
    clustered_external_ids: set[tuple[str, str]] = set()
    cluster_groups = []
    for snapshot in sorted(
        snapshots, key=lambda snapshot: snapshot.cluster.identity_cluster_id
    ):
        group_external_ids = sorted(
            (external_id.id_type, external_id.external_id)
            for external_id in snapshot.cluster.external_ids
            if (external_id.id_type, external_id.external_id) in active_external_ids
        )
        clustered_external_ids.update(group_external_ids)
        cluster_groups.append(
            {
                "identity_cluster_id": snapshot.cluster.identity_cluster_id,
                "external_ids": [
                    {"id_type": id_type, "external_id": external_id}
                    for id_type, external_id in group_external_ids
                ],
            }
        )
    unclustered_external_ids = sorted(active_external_ids - clustered_external_ids)
    return {
        "cluster_groups": cluster_groups,
        "unclustered_external_ids": [
            {"id_type": id_type, "external_id": external_id}
            for id_type, external_id in unclustered_external_ids
        ],
    }
