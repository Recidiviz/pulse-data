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
"""Stale-candidate cleanup pass of the Identity Service import: marks STALE the
tenant's PENDING candidate rows that the current run did not re-detect, so the
review queues hold only proposals the latest clustering results still support.

Each flagger stamps a candidate it files or refreshes with the run's start time
(see merge_candidate_flagger.py and split_candidate_flagger.py), so once every
pass has run, a PENDING candidate still carrying an earlier detected_at_utc is
one no cluster proposed this run. This pass marks those STALE, across the
update-attribute, merge, and split candidate tables, logging the retired ids
and stamping each row's status_updated_at_utc so an accidental mass retirement
can be traced to the exact rows.
"""
import datetime
import logging

from sqlalchemy.orm import Session

from recidiviz.common.constants.identity import CandidateStatus
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema


def retire_stale_candidates(
    *,
    session: Session,
    tenant: Tenant,
    run_started_at: datetime.datetime,
) -> None:
    """Marks STALE the tenant's PENDING candidate rows the current run did not
    re-detect: any whose detected_at_utc predates the run's start.

    Call after every pass that could refresh or file a candidate has finished,
    so a PENDING candidate still carrying an earlier detected_at_utc is known to
    be one no cluster proposed this run.

    Args:
        session: Session the retirement writes are staged on.
        tenant: The tenant whose candidates to clean up.
        run_started_at: The current run's start time. A flagger stamped every
            candidate it filed or refreshed this run with this value, so rows
            with an earlier detected_at_utc are the ones to retire.
    """
    stale_update_count = _retire_stale_candidates_in_table(
        session=session,
        candidate_table=schema.UpdateAttributeCandidate,
        tenant=tenant,
        run_started_at=run_started_at,
    )
    stale_merge_count = _retire_stale_candidates_in_table(
        session=session,
        candidate_table=schema.MergeCandidate,
        tenant=tenant,
        run_started_at=run_started_at,
    )
    stale_split_count = _retire_stale_candidates_in_table(
        session=session,
        candidate_table=schema.SplitCandidate,
        tenant=tenant,
        run_started_at=run_started_at,
    )
    logging.info(
        "Retired stale candidates for tenant [%s]: [%s] update, [%s] merge, and "
        "[%s] split candidates marked STALE.",
        tenant.value,
        stale_update_count,
        stale_merge_count,
        stale_split_count,
    )


def _retire_stale_candidates_in_table(
    *,
    session: Session,
    candidate_table: type[schema.UpdateAttributeCandidate]
    | type[schema.MergeCandidate]
    | type[schema.SplitCandidate],
    tenant: Tenant,
    run_started_at: datetime.datetime,
) -> int:
    """Marks STALE the table's PENDING rows whose detected_at_utc predates the
    run's start and returns how many.

    Selects the stale ids first and logs them, so an accidental mass
    retirement can be traced to the exact rows, then updates those ids,
    stamping status_updated_at_utc with the run's start time, the same value
    every other write this run carries. The update re-checks that each row is
    still PENDING, so a candidate a reviewer resolves between the select and
    the update keeps its resolution.
    """
    stale_ids = [
        candidate_id
        for (candidate_id,) in session.query(candidate_table.id).filter(
            candidate_table.tenant == tenant,
            candidate_table.status == CandidateStatus.PENDING,
            candidate_table.detected_at_utc < run_started_at,
        )
    ]
    if not stale_ids:
        return 0
    logging.info(
        "Marking [%s] rows of [%s] STALE for tenant [%s]: ids %s.",
        len(stale_ids),
        candidate_table.__tablename__,
        tenant.value,
        sorted(stale_ids),
    )
    updated_count = (
        session.query(candidate_table)
        .filter(
            candidate_table.id.in_(stale_ids),
            candidate_table.status == CandidateStatus.PENDING,
        )
        .update(
            {
                candidate_table.status: CandidateStatus.STALE,
                candidate_table.status_updated_at_utc: run_started_at,
            },
            synchronize_session=False,
        )
    )
    return updated_count
