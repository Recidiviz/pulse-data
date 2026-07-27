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
"""The CO Edovo earned-time sentence-credit writeback flow.

Loads per-person proposed credit from the source view and tops each person's
Recidiviz-entered pending credit up to that total, writing only the difference
and verifying by read-back. How the proposed total is pooled is the source
view's concern, not this flow's.

The eOMIS Sentence Credit/Debit screen read/POST and the CO instance URLs are
stubbed pending the screen-capture spike (TODO(OBT-22951)); everything else is
complete and tested.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.eomis.client import EomisClient
from recidiviz.eomis.flow import (
    SKIP_ACTION,
    Candidate,
    EomisWritebackFlow,
    ResultStatus,
    WriteResult,
)

# TODO(OBT-22951): placeholder hostnames; the real CO eOMIS URLs come from the
# screen-capture spike. Do not use for a real run.
TEST_BASE_URL = "https://PLACEHOLDER-co-eomis-test.invalid"
PROD_BASE_URL = "https://PLACEHOLDER-co-eomis-prod.invalid"

# A path (like the AR flow) so this package doesn't import calculator view
# builders; derived per-project so it works in staging and prod. From OBT-39333.
DEFAULT_VIEW_PATH = "earned_time.us_co_edovo_proposed_credit_materialized"


def default_view(project_id: str) -> str:
    """Returns the default candidate source view in the given project."""
    return f"{project_id}.{DEFAULT_VIEW_PATH}"


MAX_WRITES_PER_RUN = 50

# TODO(OBT-22951): confirm with CDOC — SENTCREDITDEBITTYPE 18 (Earned Time) vs
# 19 (Provisional). Pending credit points to Provisional.
CREDIT_TYPE_PROVISIONAL_EARNED_TIME = "19"

WRITE_ACTION = "write"

OFFENDER_ID_COLUMN = "OFFENDERID"
PROPOSED_CREDIT_DAYS_COLUMN = "proposed_credit_days"
COURSE_IDS_COLUMN = "course_ids"


@dataclass(frozen=True)
class CoSentenceCreditCandidate(Candidate):
    """One person's proposed earned-time credit target, from the source view."""

    target_credit_days: int
    course_ids: tuple[str, ...]

    def display_fields(self) -> dict[str, str]:
        return {
            "Proposed credit (days)": str(self.target_credit_days),
            "Courses": str(len(self.course_ids)),
        }


class CoSentenceCreditFlow(EomisWritebackFlow[CoSentenceCreditCandidate]):
    """Writes Edovo-earned pending sentence credits into CO eOMIS."""

    def __init__(self, *, bq_view: str, project_id: str, limit: int | None) -> None:
        self._bq_view = bq_view
        self._project_id = project_id
        self._limit = limit

    @property
    def state_code(self) -> StateCode:
        return StateCode.US_CO

    @property
    def flow_name(self) -> str:
        return "CO eOMIS Edovo earned-time credit writeback"

    @property
    def max_writes_per_run(self) -> int:
        return MAX_WRITES_PER_RUN

    def load_candidates(self) -> list[CoSentenceCreditCandidate]:
        return load_bq_candidates(self._bq_view, self._project_id, self._limit)

    def run_candidate(
        self,
        client: EomisClient,
        candidate: CoSentenceCreditCandidate,
        *,
        commit: bool,
    ) -> WriteResult:
        """Reads the person's current Recidiviz-entered pending credit, writes
        only the difference up to the proposed target, and verifies by read-back."""
        if candidate.action != WRITE_ACTION:
            raise ValueError(f"unexpected action [{candidate.action}]")

        client.select_offender(candidate.offender_id)
        already_written = self._recidiviz_pending_credit_days(
            client, candidate.offender_id
        )
        delta = candidate.target_credit_days - already_written
        if delta < 0:
            # More Recidiviz-entered credit than we now propose; we never remove
            # credit automatically, so surface it rather than skip silently.
            return WriteResult(
                candidate,
                ResultStatus.ERROR,
                f"eOMIS holds [{already_written}] day(s) of Recidiviz-entered "
                f"pending credit, above the proposed target "
                f"[{candidate.target_credit_days}]; needs manual review",
            )
        if delta == 0:
            return WriteResult(
                candidate,
                ResultStatus.SKIPPED,
                f"already synced ([{already_written}] day(s) present)",
            )

        if not commit:
            return WriteResult(
                candidate,
                ResultStatus.DRY_RUN,
                f"would write [{delta}] day(s) of pending earned-time credit "
                f"(target [{candidate.target_credit_days}], existing "
                f"[{already_written}])",
            )

        # TODO(OBT-22951): confirm write granularity with CDOC — this writes one
        # lump entry for the delta, but the spec describes per-course awards.
        self._post_pending_credit(client, candidate, delta)
        new_total = self._recidiviz_pending_credit_days(client, candidate.offender_id)
        if new_total != candidate.target_credit_days:
            return WriteResult(
                candidate,
                ResultStatus.ERROR,
                f"write not confirmed on read-back (read [{new_total}] day(s), "
                f"expected [{candidate.target_credit_days}])",
            )
        return WriteResult(
            candidate,
            ResultStatus.SUCCESS,
            f"read back as [{new_total}] day(s) of pending earned-time credit",
        )

    def _recidiviz_pending_credit_days(
        self, client: EomisClient, offender_id: str
    ) -> int:
        """Whole days of Recidiviz-entered *pending* earned-time credit eOMIS
        holds for the selected offender.

        TODO(OBT-22951): implement against the captured Sentence Credit/Debit
        listing screen; must isolate Recidiviz-entered rows from CDOC-entered ones.
        """
        raise NotImplementedError(
            "TODO(OBT-22951): CO eOMIS Sentence Credit read-back not yet "
            f"implemented (offender [{offender_id}])"
        )

    def _post_pending_credit(
        self, client: EomisClient, candidate: CoSentenceCreditCandidate, days: int
    ) -> None:
        """POSTs |days| of pending earned-time credit to the CO eOMIS Sentence
        Credit/Debit add screen for the selected offender.

        TODO(OBT-22951): implement against the captured add screen. Success is
        confirmed only by reading the new total back, never by the POST response.
        """
        raise NotImplementedError(
            "TODO(OBT-22951): CO eOMIS Sentence Credit write not yet implemented "
            f"(offender [{candidate.offender_id}], [{days}] day(s))"
        )


def classify_view_row(row: dict[str, Any]) -> CoSentenceCreditCandidate:
    """Turns one source-view row into a candidate; no whole day of proposed
    credit is a skip."""
    offender_id = str(row[OFFENDER_ID_COLUMN] or "").strip()
    target_credit_days = int(row[PROPOSED_CREDIT_DAYS_COLUMN] or 0)
    course_ids = tuple(row[COURSE_IDS_COLUMN] or ())
    if target_credit_days <= 0:
        return CoSentenceCreditCandidate(
            offender_id=offender_id,
            action=SKIP_ACTION,
            reason="no whole day of proposed credit",
            target_credit_days=target_credit_days,
            course_ids=course_ids,
        )
    return CoSentenceCreditCandidate(
        offender_id=offender_id,
        action=WRITE_ACTION,
        reason=f"sync to {target_credit_days} day(s) of pending earned-time credit",
        target_credit_days=target_credit_days,
        course_ids=course_ids,
    )


def load_bq_candidates(
    view: str, project_id: str, limit: int | None
) -> list[CoSentenceCreditCandidate]:
    """Loads and classifies candidates. The view is CO-specific and OFFENDERID is
    CO's eOMIS id, so no cross-state scoping is needed here."""
    query = f"""
        SELECT
            {OFFENDER_ID_COLUMN},
            {PROPOSED_CREDIT_DAYS_COLUMN},
            {COURSE_IDS_COLUMN}
        FROM `{view}`
        WHERE {OFFENDER_ID_COLUMN} IS NOT NULL
        ORDER BY {OFFENDER_ID_COLUMN}
    """
    if limit:
        query += f"\nLIMIT {limit}"
    client = bigquery.Client(project=project_id)
    rows = [dict(row.items()) for row in client.query(query)]
    return [classify_view_row(row) for row in rows]
