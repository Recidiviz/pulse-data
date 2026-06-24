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
"""The AR GED Program Referral writeback flow.

Owns everything specific to this use case: the source view and its
classification rules, the Program Referral screens (IJPS001A/B), the create /
update / release-and-create payloads, the screen's success markers, and
read-back verification. The shared client and runner know none of this.
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlencode

import requests
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.tools.eomis.client import EomisClient
from recidiviz.tools.eomis.flow import (
    READ_ACTION,
    SKIP_ACTION,
    Candidate,
    EomisWritebackFlow,
    ResultStatus,
    WriteResult,
)
from recidiviz.tools.eomis.parsing import (
    clean_bool,
    clean_date,
    clean_optional,
    input_values,
    to_eomis_date,
    today_eomis,
    wrap_comment,
)

TEST_BASE_URL = "https://eomistest.doc.arkansas.gov"
PROD_BASE_URL = "https://eomis.adc.arkansas.gov"
DEFAULT_VIEW = "recidiviz-staging.arushi_scratch.us_ar_completion_writeback"
DEFAULT_PROJECT_ID = "recidiviz-staging"

GED_PROGRAM_LABEL = "School (GED)"
REFERRAL_LISTING_HEADERS = ("Date Referred", "Status")
MAX_WRITES_PER_RUN = 25

PROGRAM_TYPE_GED = "SC"
REFERRAL_SOURCE_SCHOOL = "07"
PRIORITY_HIGH = "1"
PRIORITY_UNASSIGNED = "9"
STATUS_REFERRED_NOT_SCREENED = "01"
STATUS_COMPLETED = "49"
STATUS_RELEASED_FROM_CUSTODY = "59"
STAFF_DEFAULT = "0999998"
SUGGESTED_FACILITY_DEFAULT = "0099999"
WORK_PROGRAM_DEFAULT = "99.99"

HELP_PRIORITY = '{"id":"ProgramReferralPriority", "codeTable":"CIREFPRIORITY"}'
HELP_STATUS = '{"id":"ProgramApplicationStatus", "codeTable":"CIAPPLST2"}'

CREATE_ACTION = "create"
UPDATE_ACTION = "update"
RELEASE_AND_CREATE_ACTION = "release-and-create"

VALID_CSV_ACTIONS = {
    SKIP_ACTION,
    READ_ACTION,
    CREATE_ACTION,
    UPDATE_ACTION,
    RELEASE_AND_CREATE_ACTION,
}


@dataclass(frozen=True)
class ArProgramReferralCandidate(Candidate):
    referral_date: str | None
    referral_status: str | None
    # Only for RELEASE_AND_CREATE: date of the prior referral to close out.
    prior_referral_date: str | None = None

    def display_fields(self) -> dict[str, str]:
        return {
            "Referral date": self.referral_date or "",
            "Status": self.referral_status or "",
        }


@dataclass(frozen=True)
class ExistingReferral:
    seq: str
    referral_date: str
    status_label: str
    detail_url: str
    date_last_update: str
    time_last_update: str
    staff_last_update: str
    priority: str


class ArProgramReferralFlow(EomisWritebackFlow[ArProgramReferralCandidate]):
    """Marks GED completions from the reconciliation view as completed
    Program Referrals in AR eOMIS."""

    def __init__(
        self,
        *,
        bq_view: str,
        project_id: str,
        limit: int | None,
        comment: str,
        add_comment: str,
    ) -> None:
        self._bq_view = bq_view
        self._project_id = project_id
        self._limit = limit
        self._comment = comment
        self._add_comment = add_comment

    @property
    def state_code(self) -> StateCode:
        return StateCode.US_AR

    @property
    def flow_name(self) -> str:
        return "AR eOMIS GED referral writeback"

    @property
    def max_writes_per_run(self) -> int:
        return MAX_WRITES_PER_RUN

    def load_candidates(self) -> list[ArProgramReferralCandidate]:
        return load_bq_candidates(self._bq_view, self._project_id, self._limit)

    def run_candidate(
        self,
        client: EomisClient,
        candidate: ArProgramReferralCandidate,
        *,
        commit: bool,
    ) -> WriteResult:
        client.select_offender(candidate.offender_id)
        if candidate.action == READ_ACTION:
            return _run_read(client, candidate)
        if candidate.action == CREATE_ACTION:
            return self._run_create(client, candidate, commit)
        if candidate.action == UPDATE_ACTION:
            return self._run_update(client, candidate, commit)
        if candidate.action == RELEASE_AND_CREATE_ACTION:
            return self._run_release_and_create(client, candidate, commit)
        raise ValueError(f"unexpected action {candidate.action!r}")

    def _run_create(
        self,
        client: EomisClient,
        candidate: ArProgramReferralCandidate,
        commit: bool,
    ) -> WriteResult:
        """The proven two-step create: add the referral as Referred, not
        Screened (01), read it back, then flip it to Completed successfully
        (49). Creating directly as 49 does not post real credit."""
        if not candidate.referral_date:
            raise ValueError("create candidate is missing referral_date")

        existing = find_existing_ged_referral(
            client, candidate.offender_id, candidate.referral_date
        )
        if existing is not None:
            if is_completed_status(existing.status_label):
                return WriteResult(
                    candidate,
                    ResultStatus.SKIPPED,
                    f"already complete (seq {existing.seq})",
                )
            if not is_flippable_status(existing.status_label):
                return WriteResult(
                    candidate,
                    ResultStatus.ERROR,
                    f"existing referral seq {existing.seq} has unsupported status "
                    f"{existing.status_label}; needs manual review",
                )
            return self._complete_referral(client, candidate, existing, commit)

        referer = _prepare_to_add_url(client)
        client.get(referer)
        add_payload = build_add_payload(
            referral_date=candidate.referral_date,
            status_date=candidate.referral_date,
            comment=self._completion_comment(
                self._add_comment, candidate.referral_date
            ),
            uuid=client.uuid,
        )
        if not commit:
            return WriteResult(
                candidate,
                ResultStatus.DRY_RUN,
                "would create status 01, then complete status 49",
            )

        first_response = client.post_write(add_payload, referer)
        referral = find_existing_ged_referral(
            client, candidate.offender_id, candidate.referral_date
        )
        if referral is None:
            return WriteResult(
                candidate,
                ResultStatus.ERROR,
                "created referral but could not read it back: "
                f"{success_detail(first_response)}",
            )
        return self._complete_referral(client, candidate, referral, commit)

    def _run_update(
        self,
        client: EomisClient,
        candidate: ArProgramReferralCandidate,
        commit: bool,
    ) -> WriteResult:
        referral = find_existing_ged_referral(
            client, candidate.offender_id, candidate.referral_date
        )
        if referral is None:
            return WriteResult(candidate, ResultStatus.ERROR, "no GED referral found")
        if is_completed_status(referral.status_label):
            return WriteResult(
                candidate, ResultStatus.SKIPPED, f"already {referral.status_label}"
            )
        return self._complete_referral(client, candidate, referral, commit)

    def _run_release_and_create(
        self,
        client: EomisClient,
        candidate: ArProgramReferralCandidate,
        commit: bool,
    ) -> WriteResult:
        """Handles a completion from the current incarceration whose only
        existing referral was entered during a prior incarceration: close that
        referral out as Released from Custody (if it is still open), then
        create and complete a new referral dated to the certificate award
        date."""
        if not candidate.referral_date:
            raise ValueError("release-and-create candidate is missing referral_date")
        if not candidate.prior_referral_date:
            raise ValueError(
                "release-and-create candidate is missing prior_referral_date"
            )

        prior = find_existing_ged_referral(
            client, candidate.offender_id, candidate.prior_referral_date
        )
        if prior is None:
            return WriteResult(
                candidate,
                ResultStatus.ERROR,
                f"no prior GED referral found dated {candidate.prior_referral_date}",
            )
        if is_completed_status(prior.status_label):
            return WriteResult(
                candidate,
                ResultStatus.ERROR,
                f"prior referral is already {prior.status_label}; needs manual review",
            )

        release_needed = not is_released_status(prior.status_label)
        if not commit:
            plan = "would create status 01, then complete status 49"
            if release_needed:
                plan = (
                    f"would release seq {prior.seq} from {prior.status_label} "
                    f"to status {STATUS_RELEASED_FROM_CUSTODY}, then {plan}"
                )
            return WriteResult(candidate, ResultStatus.DRY_RUN, plan)

        if release_needed:
            release_payload = build_update_payload(
                referral=prior,
                status=STATUS_RELEASED_FROM_CUSTODY,
                status_date=today_eomis(),
                comment=self._completion_comment(
                    self._comment, candidate.referral_date
                ),
                uuid=client.uuid,
            )
            client.post_write(release_payload, prior.detail_url)
            released = find_existing_ged_referral(
                client, candidate.offender_id, candidate.prior_referral_date
            )
            if released is None or not is_released_status(released.status_label):
                return WriteResult(
                    candidate,
                    ResultStatus.ERROR,
                    f"failed to release prior referral seq {prior.seq}; status is "
                    f"{released.status_label if released else 'unknown'}",
                )
        return self._run_create(client, candidate, commit)

    def _complete_referral(
        self,
        client: EomisClient,
        candidate: ArProgramReferralCandidate,
        referral: ExistingReferral,
        commit: bool,
    ) -> WriteResult:
        """Flips an existing referral to Completed successfully (49) and
        verifies the new status by reading the listing back."""
        if not commit:
            return WriteResult(
                candidate,
                ResultStatus.DRY_RUN,
                f"would complete seq {referral.seq} from {referral.status_label}",
            )
        payload = build_update_payload(
            referral=referral,
            status=STATUS_COMPLETED,
            status_date=today_eomis(),
            comment=self._completion_comment(self._comment, referral.referral_date),
            uuid=client.uuid,
        )
        response = client.post_write(payload, referral.detail_url)
        detail = success_detail(response)
        updated = find_existing_ged_referral(
            client, candidate.offender_id, referral.referral_date
        )
        if updated is None or not is_completed_status(updated.status_label):
            return WriteResult(
                candidate,
                ResultStatus.ERROR,
                "completion not confirmed on read-back (status "
                f"{updated.status_label if updated else 'unknown'}; {detail})",
            )
        return WriteResult(
            candidate,
            ResultStatus.SUCCESS,
            f"read back as {updated.status_label} ({detail})",
        )

    def _completion_comment(self, comment: str, referral_date: str) -> str:
        if comment:
            return wrap_comment(comment)
        return wrap_comment(
            f"This message was automatically generated on {today_eomis()} to "
            f"reflect a program completion dated {referral_date}."
        )


def _run_read(
    client: EomisClient, candidate: ArProgramReferralCandidate
) -> WriteResult:
    referral = find_existing_ged_referral(
        client, candidate.offender_id, candidate.referral_date
    )
    if referral is None:
        return WriteResult(candidate, ResultStatus.ERROR, "no GED referral found")
    return WriteResult(
        candidate,
        ResultStatus.READ,
        (
            f"seq {referral.seq}; date {referral.referral_date}; "
            f"status {referral.status_label}"
        ),
    )


def _referral_browse_params(offender_id: str) -> dict[str, str]:
    return {
        "task": "InmateProgramReferralServlet",
        "option": "browse",
        "offenderInfo": "Y",
        "IO_NAME": "IJPS001A",
        "TC": "0",
        "fromMenu": "Y",
        "offenderId": offender_id,
    }


def _referral_listing_url(client: EomisClient, offender_id: str) -> str:
    params = {**_referral_browse_params(offender_id), "uuid": client.uuid}
    return f"{client.controller_url}?{urlencode(params)}"


def _prepare_to_add_url(client: EomisClient) -> str:
    return (
        f"{client.controller_url}?IO_NAME=IJPS001A"
        "&task=InmateProgramReferralServlet"
        f"&option=prepareToAdd&filter=&filter=&uuid={client.uuid}"
    )


def find_existing_ged_referral(
    client: EomisClient, offender_id: str, referral_date: str | None
) -> ExistingReferral | None:
    rows = [
        row
        for row in client.fetch_all_listing_rows(
            browse_params=_referral_browse_params(offender_id),
            listing_referer=_referral_listing_url(client, offender_id),
            required_headers=REFERRAL_LISTING_HEADERS,
        )
        if row.get("ProgramReferralType") == GED_PROGRAM_LABEL
    ]
    if referral_date:
        rows = [
            row for row in rows if row.get("ProgramApplicationDate") == referral_date
        ]
    if not rows:
        return None

    row = rows[0]
    detail_url = row["_detail_url"]
    values = input_values(client.get(detail_url))
    return ExistingReferral(
        seq=referral_seq(row, values),
        referral_date=row.get("ProgramApplicationDate", ""),
        status_label=row.get("ProgramApplicationStatus", ""),
        detail_url=detail_url,
        date_last_update=values.get("dateLastUpdate", ""),
        time_last_update=values.get("timeLastUpdate", ""),
        staff_last_update=values.get("staffLastUpdate", ""),
        priority=values.get("ProgramReferralPriority") or PRIORITY_UNASSIGNED,
    )


def referral_seq(row: dict[str, str], detail_values: dict[str, str]) -> str:
    if match := re.search(r"ProgramReferralSeqNbr=([^&]+)", row.get("_detail_url", "")):
        return match.group(1)
    if seq := detail_values.get("ProgramReferralSeqNbr"):
        return seq
    unique_id = row.get("_detailuniqueid", "")
    return unique_id[-3:] if unique_id else ""


def build_add_payload(
    *, referral_date: str, status_date: str, comment: str, uuid: str
) -> list[tuple[str, str]]:
    return [
        ("IO_NAME", "IJPS001B"),
        ("task", "InmateProgramReferralServlet"),
        ("option", "add"),
        ("_Position", ""),
        ("_forwardToServlet", "InmateProgramReferralRvwServlet"),
        ("_forwardToMethod", "detail"),
        ("_returnToServlet", "InmateProgramReferralServlet"),
        ("_returnToMethod", "returnFromChild"),
        ("mode", "add"),
        ("dateLastUpdate", ""),
        ("timeLastUpdate", ""),
        ("staffLastUpdate", ""),
        ("alreadyWarned", ""),
        ("bypass24HourRule", "Y"),
        ("dontTreatAsStdFrmInNewInterface", ""),
        ("stdFormType", ""),
        ("DatePrepared", ""),
        ("TimePrepared", ""),
        ("stdFormFramework", ""),
        ("stdFormVersion", ""),
        ("workPgmAssignmentCode", WORK_PROGRAM_DEFAULT),
        ("ProgramApplicationDate", referral_date),
        ("ProgramApplicationDateDATE", referral_date),
        ("ProgramReferralSeqNbr", "***"),
        ("ProgramReferralType", PROGRAM_TYPE_GED),
        ("ReferralSource", REFERRAL_SOURCE_SCHOOL),
        ("ProgramReferralStaff", STAFF_DEFAULT),
        ("ProgramReferralPriority", PRIORITY_HIGH),
        ("codeValueOnhelpFlag", HELP_PRIORITY),
        ("PgmMandatedByPPTB", "N"),
        ("VolPgmParticipation", "N"),
        ("SuggestedFacility", ""),
        ("ProgramApplicationStatus", STATUS_REFERRED_NOT_SCREENED),
        ("codeValueOnhelpFlag", HELP_STATUS),
        ("ProgramApplicationStatDt", status_date),
        ("ProgramApplicationStatDtDATE", status_date),
        ("PgmReferralComments", comment),
        ("uuid", uuid),
    ]


def build_update_payload(
    *,
    referral: ExistingReferral,
    status: str,
    status_date: str,
    comment: str,
    uuid: str,
) -> list[tuple[str, str]]:
    return [
        ("IO_NAME", "IJPS001B"),
        ("task", "InmateProgramReferralServlet"),
        ("option", "update"),
        ("_Position", ""),
        ("_forwardToServlet", "InmateProgramReferralRvwServlet"),
        ("_forwardToMethod", "detail"),
        ("_returnToServlet", "InmateProgramReferralServlet"),
        ("_returnToMethod", "returnFromChild"),
        ("mode", "update"),
        ("dateLastUpdate", referral.date_last_update),
        ("timeLastUpdate", referral.time_last_update),
        ("staffLastUpdate", referral.staff_last_update),
        ("alreadyWarned", ""),
        ("bypass24HourRule", "Y"),
        ("dontTreatAsStdFrmInNewInterface", ""),
        ("stdFormType", ""),
        ("DatePrepared", ""),
        ("TimePrepared", ""),
        ("stdFormFramework", ""),
        ("stdFormVersion", ""),
        ("workPgmAssignmentCode", WORK_PROGRAM_DEFAULT),
        ("ProgramApplicationDateDisplay", referral.referral_date),
        ("ProgramApplicationDate", referral.referral_date),
        ("ProgramReferralSeqNbr", referral.seq),
        ("ProgramReferralType", PROGRAM_TYPE_GED),
        ("ReferralSource", REFERRAL_SOURCE_SCHOOL),
        ("ProgramReferralStaff", STAFF_DEFAULT),
        ("ProgramReferralPriority", referral.priority),
        ("codeValueOnhelpFlag", HELP_PRIORITY),
        ("PgmMandatedByPPTB", "N"),
        ("VolPgmParticipation", "N"),
        ("SuggestedFacility", SUGGESTED_FACILITY_DEFAULT),
        ("ProgramApplicationStatus", status),
        ("codeValueOnhelpFlag", HELP_STATUS),
        ("ProgramApplicationStatDt", status_date),
        ("ProgramApplicationStatDtDATE", status_date),
        ("PgmReferralComments", comment),
        ("uuid", uuid),
    ]


def success_detail(response: requests.Response) -> str:
    # eOMIS can return HTTP 200 on application errors; these screen-specific
    # markers are the only POST-level success signal. Read-back is the backstop.
    markers = (
        "added successfully",
        "Record updated",
        "Release Dates have been recomputed",
    )
    for marker in markers:
        if marker in response.text:
            return marker
    return f"POST {response.status_code}; no known success marker"


def classify_view_row(row: dict[str, Any]) -> ArProgramReferralCandidate:
    """Applies the V1 write criteria to one reconciliation-view row. This is
    the business logic of the writeback; the rules are mirrored one-to-one by
    the tests in recidiviz/tests/tools/eomis/us_ar."""
    offender_id = str(row["OFFENDERID"] or "").strip()
    referral_status = clean_optional(row.get("referral_status"))
    completed_prior = clean_bool(row.get("completed_in_prior_incarceration_flag"))
    entered_prior = clean_bool(row.get("entered_in_prior_incarceration_flag"))

    if completed_prior:
        return ArProgramReferralCandidate(
            offender_id=offender_id,
            action=SKIP_ACTION,
            reason="prior completion",
            referral_date=None,
            referral_status=referral_status,
        )
    if not referral_status:
        return ArProgramReferralCandidate(
            offender_id=offender_id,
            action=CREATE_ACTION,
            reason="missing GED referral",
            referral_date=to_eomis_date(row.get("certificate_award_date")),
            referral_status=referral_status,
        )
    if entered_prior:
        if is_completed_status(referral_status):
            return ArProgramReferralCandidate(
                offender_id=offender_id,
                action=SKIP_ACTION,
                reason=(
                    "prior-incarceration referral already complete; "
                    "needs manual review"
                ),
                referral_date=clean_date(row.get("referral_application_date")),
                referral_status=referral_status,
            )
        certificate_date = to_eomis_date(row.get("certificate_award_date"))
        prior_referral_date = clean_date(row.get("referral_application_date"))
        if certificate_date == prior_referral_date:
            return ArProgramReferralCandidate(
                offender_id=offender_id,
                action=SKIP_ACTION,
                reason="prior referral shares certificate date; needs manual review",
                referral_date=prior_referral_date,
                referral_status=referral_status,
            )
        return ArProgramReferralCandidate(
            offender_id=offender_id,
            action=RELEASE_AND_CREATE_ACTION,
            reason="release prior-incarceration referral, create new",
            referral_date=certificate_date,
            referral_status=referral_status,
            prior_referral_date=prior_referral_date,
        )
    if is_completed_status(referral_status):
        return ArProgramReferralCandidate(
            offender_id=offender_id,
            action=SKIP_ACTION,
            reason="already complete",
            referral_date=clean_date(row.get("referral_application_date")),
            referral_status=referral_status,
        )
    if is_flippable_status(referral_status):
        return ArProgramReferralCandidate(
            offender_id=offender_id,
            action=UPDATE_ACTION,
            reason="mark existing referral complete",
            referral_date=clean_date(row.get("referral_application_date")),
            referral_status=referral_status,
        )
    return ArProgramReferralCandidate(
        offender_id=offender_id,
        action=SKIP_ACTION,
        reason="unsupported referral status",
        referral_date=clean_date(row.get("referral_application_date")),
        referral_status=referral_status,
    )


def is_completed_status(status: str) -> bool:
    return status.lower() in {
        "49",
        "completed successfully",
        "prior completion verified",
    }


def is_released_status(status: str) -> bool:
    return status.lower() in {
        STATUS_RELEASED_FROM_CUSTODY,
        "released from custody",
    }


def is_flippable_status(status: str) -> bool:
    return status.lower() in {
        "01",
        "referred, not screened",
        "active participant",
        "volunteered, not screened",
    }


def skip_offenders_already_completed_this_incarceration(
    rows: list[dict[str, Any]],
    candidates: list[ArProgramReferralCandidate],
) -> list[ArProgramReferralCandidate]:
    """Returns the candidates with writes demoted to skip for any offender who
    already has a completed GED referral this incarceration."""
    already_completed = {
        str(row["OFFENDERID"]).strip()
        for row in rows
        if not clean_bool(row.get("entered_in_prior_incarceration_flag"))
        and (status := clean_optional(row.get("referral_status")))
        and is_completed_status(status)
    }
    return [
        replace(
            candidate,
            action=SKIP_ACTION,
            reason="offender already completed a GED referral this incarceration",
        )
        if candidate.action != SKIP_ACTION
        and candidate.offender_id in already_completed
        else candidate
        for candidate in candidates
    ]


def load_bq_candidates(
    view: str, project_id: str, limit: int | None
) -> list[ArProgramReferralCandidate]:
    query = f"""
        SELECT
            OFFENDERID,
            certificate_award_date,
            referral_application_date,
            referral_status,
            completed_in_prior_incarceration_flag,
            entered_in_prior_incarceration_flag
        FROM `{view}`
        WHERE OFFENDERID IS NOT NULL
        ORDER BY certificate_award_date, OFFENDERID
    """
    if limit:
        query += f"\nLIMIT {limit}"
    client = bigquery.Client(project=project_id)
    rows = [dict(row.items()) for row in client.query(query)]
    candidates = [classify_view_row(row) for row in rows]
    return skip_offenders_already_completed_this_incarceration(rows, candidates)


def load_csv_candidates(
    path: str, limit: int | None
) -> list[ArProgramReferralCandidate]:
    with open(path, newline="", encoding="utf-8") as candidates_file:
        reader = csv.DictReader(candidates_file)
        if reader.fieldnames is None or "OFFENDERID" not in reader.fieldnames:
            raise ValueError(f"{path} must have an OFFENDERID column")
        rows = list(reader)
    if limit:
        rows = rows[:limit]
    candidates = [classify_csv_row(row) for row in rows]
    for candidate in candidates:
        if (
            candidate.action == RELEASE_AND_CREATE_ACTION
            and not candidate.prior_referral_date
        ):
            raise ValueError(
                f"{candidate.offender_id}: release-and-create requires a "
                "prior_referral_date column value"
            )
    return candidates


def classify_csv_row(row: dict[str, Any]) -> ArProgramReferralCandidate:
    if clean_optional(row.get("action")):
        action = str(row["action"]).strip().lower()
        if action not in VALID_CSV_ACTIONS:
            raise ValueError(
                f"[{row['OFFENDERID']}]: unknown action [{action}]; "
                f"expected one of {sorted(VALID_CSV_ACTIONS)}"
            )
        return ArProgramReferralCandidate(
            offender_id=str(row["OFFENDERID"]).strip(),
            action=action,
            reason="csv action",
            referral_date=clean_date(
                row.get("referral_date")
                or row.get("referral_application_date")
                or row.get("certificate_award_date")
            ),
            referral_status=clean_optional(row.get("referral_status")),
            prior_referral_date=clean_date(row.get("prior_referral_date")),
        )
    return classify_view_row(row)


def build_id_candidates(
    offender_ids: list[str], action: str, referral_date: str | None
) -> list[ArProgramReferralCandidate]:
    if action == "auto":
        raise ValueError("--source ids requires --action create or --action update")
    if action == CREATE_ACTION and not referral_date:
        raise ValueError("--action create requires --referral-date")
    return [
        ArProgramReferralCandidate(
            offender_id=offender_id,
            action=action,
            reason="manual id",
            referral_date=to_eomis_date(referral_date) if referral_date else None,
            referral_status=None,
        )
        for offender_id in offender_ids
    ]
