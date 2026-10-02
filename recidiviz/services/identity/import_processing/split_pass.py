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
"""Auto-split pass of the Identity Service import: splits a fractured identity
(one whose active external ids appear in two or more of the run's clusters)
into one identity per cluster group, unless a human decision blocks it.

The pass runs on one identity at a time. find_split_blockers reports the human
decisions that make an auto-split unsafe; when there are none,
apply_identity_split moves each non-kept cluster group's external ids to a new
empty identity, keeps every attribute except import-sourced emails on the
original (the same run's update pass re-adds each address to whichever
identity's cluster carries it), and records the split in the split_events
audit tables and as no_merge pairs so clustering results alone can never
re-merge what was split. A blocked identity is flagged as a
PENDING split candidate instead (see split_candidate_flagger.py).
"""
import datetime
import itertools
import uuid

from sqlalchemy.orm import Session

from recidiviz.common.constants.identity import (
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    SourceType,
    SplitBlocker,
    SplitTrigger,
)
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.services.identity.import_processing.identity_child_rows import (
    ATTRIBUTE_ROW_TYPES,
)

# Written to no_merge.reason for pairs the auto-split records.
IMPORT_AUTO_SPLIT_NO_MERGE_REASON = (
    "Recorded by the import's auto-split: clustering results alone must not "
    "re-merge identities a split created."
)


def find_split_blockers(
    *,
    session: Session,
    recidiviz_id: uuid.UUID,
    cluster_groups: dict[str, object],
) -> list[SplitBlocker]:
    """Returns the human decisions that make auto-splitting the given identity
    unsafe; an empty list means apply_identity_split may proceed.

    The two blockers:

    - HUMAN_MERGE: a human deliberately merged records into this identity (a
      MergeEvent with trigger MERGE_ENDPOINT), and the split would move ids
      that merge contributed off this identity, undoing the human's decision.
    - HUMAN_EDITED_ATTRIBUTES: the identity has PRODUCT_APP or ADMIN_OVERRIDE
      attribute values, and the import cannot know which side of the split a
      human's values belong to.
    """
    blockers = []
    if _would_undo_human_merge(
        session=session, recidiviz_id=recidiviz_id, cluster_groups=cluster_groups
    ):
        blockers.append(SplitBlocker.HUMAN_MERGE)
    if _has_human_edited_attributes(session=session, recidiviz_id=recidiviz_id):
        blockers.append(SplitBlocker.HUMAN_EDITED_ATTRIBUTES)
    return blockers


def apply_identity_split(
    *,
    session: Session,
    identity: schema.Identity,
    cluster_groups: dict[str, object],
    now: datetime.datetime,
) -> None:
    """Stages the given identity's auto-split on the caller's session, per its
    cluster_groups (the payload build_cluster_groups produces and a split
    candidate stores).

    The original identity keeps the kept group's external ids (see
    _kept_group_index), its unclustered active ids, its inactive residue rows,
    and every attribute row except import-sourced emails. An email may live on
    at most one identity and may belong to a split-off group's person, so the
    split deletes the original's EXTERNAL_DATA_SYSTEM email rows and clears
    last_cluster_hash; the same run's update pass then re-applies the kept
    group's cluster, restoring any emails it carries, while each other
    identity's cluster supplies its own. skip_demographic_guard is set so that
    corrective update can change attributes without the guard blocking. Each
    other group's ids move to a new identity with empty attributes (an empty
    identity cannot conflict demographically, so the update pass fills it from
    its cluster in the same run). The split is recorded as one SplitEvent with
    trigger IMPORT (no moved-attribute rows: non-email attributes stay, and
    the deleted emails are rebuilt by the update pass rather than moved), and
    as pairwise no_merge rows among the original and new identities, so
    clustering results alone can never re-merge them.

    Raises ValueError when the payload does not exactly partition the
    identity's active external ids, e.g. a stale stored candidate.
    """
    groups = _groups(cluster_groups)
    kept_group_index = _kept_group_index(cluster_groups)
    active_rows_by_key = {
        (row.id_type.value, row.external_id): row
        for row in session.query(schema.ExternalId).filter(
            schema.ExternalId.recidiviz_id == identity.recidiviz_id,
            schema.ExternalId.is_active.is_(True),
        )
    }
    _check_partitions_active_external_ids(
        cluster_groups=cluster_groups,
        active_external_ids=set(active_rows_by_key),
        recidiviz_id=identity.recidiviz_id,
    )

    # Deactivate every moving id first and flush, so the new active rows
    # inserted below never coexist with the old ones under the unique index
    # on active (external_id, id_type).
    for group_index, group in enumerate(groups):
        if group_index == kept_group_index:
            continue
        for external_id_json in _group_external_ids(group):
            key = (external_id_json["id_type"], external_id_json["external_id"])
            active_rows_by_key[key].is_active = False
    session.flush()

    split_event = schema.SplitEvent(
        original_id=identity.recidiviz_id,
        trigger=SplitTrigger.IMPORT,
        requested_by=None,
        timestamp_utc=now,
    )
    session.add(split_event)
    # Assigns split_event.id so the audit child rows can reference it.
    session.flush()

    new_recidiviz_ids = []
    for group_index, group in enumerate(groups):
        if group_index == kept_group_index:
            continue
        new_recidiviz_ids.append(
            _create_new_identity_for_group(
                session=session,
                original=identity,
                split_event_id=split_event.id,
                group=group,
                now=now,
            )
        )

    # An email may live on at most one identity, and an email on the original
    # may belong to a split-off group's person, so emails are the one
    # attribute the original does not keep. Deleting the import-sourced rows
    # lets the same run's update pass re-add each address to whichever
    # identity's cluster carries it; human-entered emails stay, since the
    # import cannot know which side of the split they belong to.
    session.query(schema.Email).filter(
        schema.Email.recidiviz_id == identity.recidiviz_id,
        schema.Email.source_type == SourceType.EXTERNAL_DATA_SYSTEM,
    ).delete(synchronize_session=False)

    identity.skip_demographic_guard = True
    # None, so the same run's update pass always re-applies the kept group's
    # cluster, restoring its emails and clearing skip_demographic_guard, even
    # when that cluster's hash matches the one the identity last applied.
    identity.last_cluster_hash = None
    identity.last_updated_utc = now

    for recidiviz_id_a, recidiviz_id_b in itertools.combinations(
        sorted([identity.recidiviz_id, *new_recidiviz_ids]), 2
    ):
        session.add(
            schema.NoMerge(
                recidiviz_id_a=recidiviz_id_a,
                recidiviz_id_b=recidiviz_id_b,
                reason=IMPORT_AUTO_SPLIT_NO_MERGE_REASON,
                # NULL created_by marks a system-recorded pair, matching
                # requested_by on the merge and split event tables.
                created_by=None,
                created_utc=now,
            )
        )


def _groups(cluster_groups: dict[str, object]) -> list[dict[str, object]]:
    """Returns the cluster group list from a cluster_groups payload."""
    groups = cluster_groups["cluster_groups"]
    if not isinstance(groups, list):
        raise ValueError(f"Expected a list of cluster groups, found [{groups}]")
    return groups


def _group_external_ids(group: dict[str, object]) -> list[dict[str, str]]:
    """Returns one cluster group's external ids from its payload entry."""
    external_ids = group["external_ids"]
    if not isinstance(external_ids, list):
        raise ValueError(f"Expected a list of external ids, found [{external_ids}]")
    return external_ids


def _unclustered_external_ids(
    cluster_groups: dict[str, object]
) -> list[dict[str, str]]:
    """Returns the unclustered external ids from a cluster_groups payload."""
    unclustered_external_ids = cluster_groups["unclustered_external_ids"]
    if not isinstance(unclustered_external_ids, list):
        raise ValueError(
            f"Expected a list of external ids, found [{unclustered_external_ids}]"
        )
    return unclustered_external_ids


def _check_partitions_active_external_ids(
    *,
    cluster_groups: dict[str, object],
    active_external_ids: set[tuple[str, str]],
    recidiviz_id: uuid.UUID,
) -> None:
    """Raises unless the payload's groups plus unclustered ids exactly
    partition the given active external ids.

    The import builds the payload fresh, so this always holds on that path;
    apply_identity_split will also be fed stored candidate payloads by the
    future resolve-candidate endpoint, and those can be stale. Checking up
    front turns every such case into one loud failure instead of a partial or
    silently wrong split.
    """
    payload_external_ids: list[tuple[str, str]] = []
    for group in _groups(cluster_groups):
        for external_id_json in _group_external_ids(group):
            payload_external_ids.append(
                (external_id_json["id_type"], external_id_json["external_id"])
            )
    for external_id_json in _unclustered_external_ids(cluster_groups):
        payload_external_ids.append(
            (external_id_json["id_type"], external_id_json["external_id"])
        )
    duplicated_external_ids = {
        external_id
        for external_id in payload_external_ids
        if payload_external_ids.count(external_id) > 1
    }
    if duplicated_external_ids:
        raise ValueError(
            f"cluster_groups for identity [{recidiviz_id}] lists external ids "
            f"more than once: [{sorted(duplicated_external_ids)}]."
        )
    if set(payload_external_ids) != active_external_ids:
        raise ValueError(
            f"cluster_groups for identity [{recidiviz_id}] must cover exactly "
            f"its active external ids; the payload has "
            f"[{sorted(set(payload_external_ids))}], the identity has "
            f"[{sorted(active_external_ids)}]."
        )


def _create_new_identity_for_group(
    *,
    session: Session,
    original: schema.Identity,
    split_event_id: int,
    group: dict[str, object],
    now: datetime.datetime,
) -> uuid.UUID:
    """Creates a new identity holding the given cluster group's external ids,
    recording each moved id on the split event; returns the new identity's
    recidiviz_id.

    The new identity starts with empty attributes (an empty identity cannot
    conflict demographically, so the update pass fills it from its cluster in
    the same run) and takes person_type from the original (that same update
    corrects it from the cluster).
    """
    # TODO(OBT-50191): Build this through IdentityServiceQuerier.create_identity
    # once it can stage on a caller-controlled session.
    new_recidiviz_id = uuid.uuid4()
    session.add(
        schema.Identity(
            recidiviz_id=new_recidiviz_id,
            created_utc=now,
            last_updated_utc=now,
            tenant=original.tenant,
            person_type=original.person_type,
            status=IdentityStatus.ACTIVE,
            merged_into=None,
            # None, so the same run's update pass sees the group's cluster
            # as changed and fills the empty identity from it.
            last_cluster_hash=None,
        )
    )
    session.add(
        schema.SplitEventNewIdentity(
            split_event_id=split_event_id, new_recidiviz_id=new_recidiviz_id
        )
    )
    for external_id_json in _group_external_ids(group):
        session.add(
            schema.ExternalId(
                recidiviz_id=new_recidiviz_id,
                external_id=external_id_json["external_id"],
                id_type=IdentifierType(external_id_json["id_type"]),
                is_active=True,
            )
        )
        session.add(
            schema.SplitEventMovedExternalId(
                split_event_id=split_event_id,
                external_id=external_id_json["external_id"],
                id_type=IdentifierType(external_id_json["id_type"]),
                new_recidiviz_id=new_recidiviz_id,
            )
        )
    return new_recidiviz_id


def _kept_group_index(cluster_groups: dict[str, object]) -> int:
    """Returns the index of the cluster group the original identity keeps.

    The group holding the most of the identity's external ids wins, ties
    broken by the lexicographically smallest (id_type, external_id) in the
    group. The recidiviz_id is the stable person key downstream products hold,
    so the original identity should stay closest to the person it has been
    representing; group size is deterministic and does not depend on cluster
    ids, which are hash-derived and change when a cluster's membership does.
    """

    def sort_key(
        indexed_group: tuple[int, dict[str, object]]
    ) -> tuple[int, tuple[str, str]]:
        _, group = indexed_group
        external_ids = _group_external_ids(group)
        return (
            -len(external_ids),
            min(
                (external_id["id_type"], external_id["external_id"])
                for external_id in external_ids
            ),
        )

    indexed_groups = list(enumerate(_groups(cluster_groups)))
    return min(indexed_groups, key=sort_key)[0]


def _would_undo_human_merge(
    *,
    session: Session,
    recidiviz_id: uuid.UUID,
    cluster_groups: dict[str, object],
) -> bool:
    """Returns whether the proposed split would move external ids that a
    human-requested merge contributed off this identity.

    A human merge (a MergeEvent with trigger MERGE_ENDPOINT) asserted that the
    retired identity's records belong to this one. Moving any id that merge
    contributed to a different identity would undo that assertion; a split
    that keeps the whole contribution on the original leaves the merge
    intact. A retired identity keeps the ids it contributed as inactive rows
    pointing at itself, which is how the contribution is recovered here; the
    future merge endpoint must preserve that contract, and this check is its
    first consumer.

    Only merges directly onto this identity are checked. A human merge buried
    under a later IMPORT-trigger merge cannot happen until auto-merge exists;
    TODO(OBT-37729) must extend this check to trace through IMPORT merges.
    """
    retired_ids = [
        retired_id
        for (retired_id,) in session.query(schema.MergeEvent.retired_id).filter(
            schema.MergeEvent.surviving_id == recidiviz_id,
            schema.MergeEvent.trigger == MergeTrigger.MERGE_ENDPOINT,
        )
    ]
    if not retired_ids:
        return False

    kept_group_index = _kept_group_index(cluster_groups)
    moving_external_ids: set[tuple[str, str]] = set()
    for group_index, group in enumerate(_groups(cluster_groups)):
        if group_index == kept_group_index:
            continue
        for external_id_json in _group_external_ids(group):
            moving_external_ids.add(
                (external_id_json["id_type"], external_id_json["external_id"])
            )

    contributed_rows = session.query(
        schema.ExternalId.id_type, schema.ExternalId.external_id
    ).filter(schema.ExternalId.recidiviz_id.in_(retired_ids))
    return any(
        (id_type.value, external_id) in moving_external_ids
        for id_type, external_id in contributed_rows
    )


def _has_human_edited_attributes(*, session: Session, recidiviz_id: uuid.UUID) -> bool:
    """Returns whether any of the identity's attribute rows carry a
    PRODUCT_APP or ADMIN_OVERRIDE source."""
    for attribute_row_type in ATTRIBUTE_ROW_TYPES:
        human_edited_row = (
            session.query(attribute_row_type)
            .filter(
                attribute_row_type.recidiviz_id == recidiviz_id,
                attribute_row_type.source_type.in_(
                    (SourceType.PRODUCT_APP, SourceType.ADMIN_OVERRIDE)
                ),
            )
            .first()
        )
        if human_edited_row is not None:
            return True
    return False
