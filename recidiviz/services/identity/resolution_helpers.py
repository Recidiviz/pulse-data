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
"""Batched resolution of Identity merged_into chains to their surviving ACTIVE
records."""
import uuid

import attr
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

from recidiviz.common import attr_validators
from recidiviz.common.constants.identity import IdentityStatus
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.services.identity.exceptions import IdentityHistoryIntegrityException


@attr.define(frozen=True)
class _IdentityResolutionStatus:
    """Progress of resolving one input id through its merged_into chain."""

    input_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))

    current_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))

    visited: set[uuid.UUID] = attr.ib(
        validator=attr_validators.is_set_of(uuid.UUID), default=set()
    )

    finished: bool = attr.ib(validator=attr_validators.is_bool, default=False)

    surviving_id: uuid.UUID | None = attr.ib(
        validator=attr_validators.is_opt(uuid.UUID), default=None
    )

    @classmethod
    def starting_at(cls, recidiviz_id: uuid.UUID) -> "_IdentityResolutionStatus":
        """Returns the initial, unfinished resolution state for one input id."""
        return cls(
            input_id=recidiviz_id,
            current_id=recidiviz_id,
        )


def _fetch_identity_rows(
    session: Session, recidiviz_ids: set[uuid.UUID]
) -> dict[uuid.UUID, Row]:
    """Returns the (status, merged_into) row for each of the given ids that
    exists, keyed by recidiviz_id, using a single batched query."""
    if not recidiviz_ids:
        return {}
    rows = (
        session.query(
            schema.Identity.recidiviz_id,
            schema.Identity.status,
            schema.Identity.merged_into,
        )
        .filter(schema.Identity.recidiviz_id.in_(recidiviz_ids))
        .all()
    )
    return {row.recidiviz_id: row for row in rows}


def _advance_one_hop(
    resolution: _IdentityResolutionStatus, rows_by_id: dict[uuid.UUID, Row]
) -> _IdentityResolutionStatus:
    """Advances an unfinished resolution one hop along its merged_into chain,
    returning the evolved state.
    """
    if resolution.current_id in resolution.visited:
        raise IdentityHistoryIntegrityException(
            f"Cycle detected in merged_into chain starting from "
            f"[{resolution.input_id}]: revisited [{resolution.current_id}]"
        )
    visited = resolution.visited | {resolution.current_id}
    if resolution.current_id not in rows_by_id:
        if resolution.current_id == resolution.input_id:
            # The input id does not exist, so it finishes with surviving_id=None
            return attr.evolve(resolution, visited=visited, finished=True)
        raise IdentityHistoryIntegrityException(
            f"Identity [{resolution.current_id}] referenced via merged_into "
            f"chain from [{resolution.input_id}] does not exist."
        )
    row = rows_by_id[resolution.current_id]
    if row.status is IdentityStatus.ACTIVE:
        return attr.evolve(
            resolution,
            visited=visited,
            finished=True,
            surviving_id=resolution.current_id,
        )
    if row.merged_into is None:
        raise IdentityHistoryIntegrityException(
            f"RETIRED identity [{resolution.current_id}] in merged_into chain "
            f"from [{resolution.input_id}] has no merged_into target."
        )
    return attr.evolve(resolution, visited=visited, current_id=row.merged_into)


def resolve_surviving_ids(
    session: Session, recidiviz_ids: list[uuid.UUID]
) -> dict[uuid.UUID, uuid.UUID | None]:
    """Returns a mapping from each id in `recidiviz_ids` to the recidiviz_id of
    the surviving ACTIVE identity at the end of its merged_into chain. An
    ACTIVE input maps to itself. Input ids that are not found map to None.

    All chains are walked together, issuing one batched query per chain hop
    level, so resolving N ids costs O(max chain depth).

    Raises IdentityHistoryIntegrityException if a merged_into hop references a
    nonexistent record, or if a chain contains a cycle.
    """
    # Every row fetched so far, shared across all chains so chains through
    # already-fetched records cost no further queries.
    rows_by_id: dict[uuid.UUID, Row] = {}
    resolutions_in_progress = [
        _IdentityResolutionStatus.starting_at(recidiviz_id)
        for recidiviz_id in dict.fromkeys(recidiviz_ids)
    ]
    resolved_ids: list[_IdentityResolutionStatus] = []
    while resolutions_in_progress:
        rows_by_id.update(
            _fetch_identity_rows(
                session,
                {r.current_id for r in resolutions_in_progress} - rows_by_id.keys(),
            )
        )
        advanced = [_advance_one_hop(r, rows_by_id) for r in resolutions_in_progress]
        resolved_ids.extend(r for r in advanced if r.finished)
        resolutions_in_progress = [r for r in advanced if not r.finished]
    return {r.input_id: r.surviving_id for r in resolved_ids}
