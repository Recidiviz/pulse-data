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
"""Earned-time credit calculation for Edovo course completions.

Downstream consumer of the ``edovo_course_completions`` table (not the request
path). Resolves each stored external id to its internal ``person_id`` and pools
content-hours into whole credits at each state's configured threshold (CO: 1 per
6 hours; see ``HOURS_PER_CREDIT_BY_STATE``). Pooling and dedup are by
internal person, so someone credited under several external ids is neither split
nor double-counted. Recomputes from full history each run (idempotent). The
eOMIS handoff lives behind ``pending_credit_sink`` (OBT-22951).
"""
import logging
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdovoCourseCompletion,
)
from recidiviz.persistence.database.session import Session
from recidiviz.utils.metadata import project_id

# Whole-credit threshold per supported state. 1 credit per 6 pooled content-hours
# is CO phase-2b policy; the calculator runs over whatever states are in the
# table, so a completion for a state absent from this map is a configuration
# error and fails loudly rather than being credited under CO's rate.
HOURS_PER_CREDIT_BY_STATE: dict[StateCode, Decimal] = {
    StateCode.US_CO: Decimal("6"),
}

# content_hours is a Float (NUMERIC doesn't survive the Cloud SQL -> BQ export),
# so each value is rounded to 2dp before pooling: this matches the DA validation
# sheet and stops float drift from flipping a 6-hour credit boundary.
CONTENT_HOURS_QUANTUM = Decimal("0.01")


def pooled_credits_for_hours(total_hours: Decimal, *, hours_per_credit: Decimal) -> int:
    """Returns whole credits for |total_hours|: one per |hours_per_credit|, remainder carries."""
    if total_hours < 0:
        raise ValueError(f"total_hours must be non-negative, got [{total_hours}]")
    return int(total_hours // hours_per_credit)


def _hours_per_credit_for_state(state_code: str) -> Decimal:
    """Returns the whole-credit hour threshold for |state_code|, raising if the
    state has no configured Edovo earned-time policy."""
    try:
        state = StateCode(state_code)
    except ValueError as e:
        raise ValueError(
            f"Edovo completion has unrecognized state_code [{state_code}]"
        ) from e
    if state not in HOURS_PER_CREDIT_BY_STATE:
        raise ValueError(
            f"No Edovo earned-time credit policy configured for state [{state_code}]"
        )
    return HOURS_PER_CREDIT_BY_STATE[state]


def _resolve_external_ids_to_person_ids(
    bq_client: BigQueryClientImpl,
    *,
    state_code: str,
    id_type: str,
    external_ids: set[str],
) -> dict[str, int]:
    """Returns {external_id: internal person_id} for |external_ids| in the given state/id_type.

    Reads ``normalized_state.state_person_external_id`` (the source the capture
    endpoint's existence check uses). Unmatched external ids are omitted.
    """
    query = f"""
        SELECT external_id, person_id
        FROM `{project_id()}.normalized_state.state_person_external_id`
        WHERE state_code = @state_code
          AND id_type = @id_type
          AND external_id IN UNNEST(@external_ids)
    """
    query_parameters: list[
        bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter
    ] = [
        bigquery.ScalarQueryParameter("state_code", "STRING", state_code),
        bigquery.ScalarQueryParameter("id_type", "STRING", id_type),
        bigquery.ArrayQueryParameter("external_ids", "STRING", sorted(external_ids)),
    ]
    job = bq_client.run_query_async(
        query_str=query,
        use_query_cache=False,
        # Wrapper types query_parameters as scalar-only; BQ accepts arrays at runtime.
        query_parameters=query_parameters,  # type: ignore[arg-type]
    )
    return {row["external_id"]: row["person_id"] for row in job}


@attr.define(frozen=True, kw_only=True)
class PersonCreditResult:
    """Pooled earned-time credit total for one person (by internal person_id) in one state."""

    person_id: int = attr.ib(validator=attr_validators.is_positive_int)
    state_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    total_pooled_hours: Decimal = attr.ib(
        validator=attr.validators.instance_of(Decimal)
    )
    credits_earned: int = attr.ib(validator=attr_validators.is_non_negative_int)


@attr.define(frozen=True, kw_only=True)
class UnresolvedCompletion:
    """An external id that matched no person — surfaced for the DA sheet, not credited."""

    state_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    id_type: str = attr.ib(validator=attr_validators.is_non_empty_str)
    person_external_id: str = attr.ib(validator=attr_validators.is_non_empty_str)


@attr.define(frozen=True, kw_only=True)
class CourseHoursConflict:
    """A resolved (person, course) that carried more than one distinct content-hours value.

    course_id is 1:1 with content per the Edovo spec, so differing hours mean that
    invariant broke upstream. The course is surfaced for review and excluded from
    crediting rather than silently resolved (e.g. by taking the max), which would
    bias toward more credit and hide the break."""

    state_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    person_id: int = attr.ib(validator=attr_validators.is_positive_int)
    course_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    content_hours_values: list[Decimal] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(Decimal),
        ]
    )
    """The distinct content-hours values seen for this course, ascending."""


@attr.define(frozen=True, kw_only=True)
class EdovoCreditCalculation:
    """Result of a run: creditable per-person totals, external ids that didn't
    resolve, and courses excluded for conflicting content-hours."""

    person_credits: list[PersonCreditResult] = attr.ib(
        validator=attr_validators.is_list_of(PersonCreditResult)
    )
    unresolved: list[UnresolvedCompletion] = attr.ib(
        validator=attr_validators.is_list_of(UnresolvedCompletion)
    )
    conflicts: list[CourseHoursConflict] = attr.ib(
        validator=attr_validators.is_list_of(CourseHoursConflict)
    )


def calculate_all_pending_credits(
    *, session: Session, bq_client: BigQueryClientImpl
) -> EdovoCreditCalculation:
    """Pools every resolvable person's stored completions into whole credits.

    Resolves external ids to internal person_ids, collapses each course to one
    content-hours value per person (a course counts once — policy: no double
    credit), rounds per row to 2dp, and pools at the per-state credit threshold.
    Unresolvable external ids are returned in ``unresolved``; courses whose rows
    disagree on content-hours are returned in ``conflicts`` — both are surfaced
    rather than credited.
    """
    # Rounding/summing is done in Python: a SQL SUM over the Float column would
    # reintroduce the drift the 2dp rounding removes (keep 2dp if ever moved to SQL).
    rows = session.query(
        EdovoCourseCompletion.state_code,
        EdovoCourseCompletion.id_type,
        EdovoCourseCompletion.person_external_id,
        EdovoCourseCompletion.course_id,
        EdovoCourseCompletion.content_hours,
    ).all()

    external_ids_by_group: dict[tuple[str, str], set[str]] = defaultdict(set)
    for state_code, id_type, external_id, _course_id, _content_hours in rows:
        external_ids_by_group[(state_code, id_type)].add(external_id)

    person_id_by_external_id: dict[tuple[str, str, str], int] = {}
    for (state_code, id_type), external_ids in external_ids_by_group.items():
        for external_id, person_id in _resolve_external_ids_to_person_ids(
            bq_client,
            state_code=state_code,
            id_type=id_type,
            external_ids=external_ids,
        ).items():
            person_id_by_external_id[(state_code, id_type, external_id)] = person_id

    # Collect the distinct per-row content-hours values seen for each resolved
    # (person, course), so a course counts once even across a person's multiple
    # external ids. A course is 1:1 with content per the spec, so a course whose
    # rows disagree is a broken invariant handled as a conflict below.
    hours_seen_by_course: dict[tuple[str, int, str], set[Decimal]] = defaultdict(set)
    unresolved: set[tuple[str, str, str]] = set()
    for state_code, id_type, external_id, course_id, content_hours in rows:
        resolved_person_id: int | None = person_id_by_external_id.get(
            (state_code, id_type, external_id)
        )
        if resolved_person_id is None:
            unresolved.add((state_code, id_type, external_id))
            continue
        quantized_hours = Decimal(str(content_hours)).quantize(
            CONTENT_HOURS_QUANTUM, rounding=ROUND_HALF_UP
        )
        hours_seen_by_course[(state_code, resolved_person_id, course_id)].add(
            quantized_hours
        )

    if unresolved:
        # Count only — the external ids are DOC-facing PII.
        logging.warning(
            "Found [%d] Edovo external id(s) that did not resolve to a known person.",
            len(unresolved),
        )

    conflicts: list[CourseHoursConflict] = []
    hours_by_person: dict[tuple[str, int], Decimal] = defaultdict(lambda: Decimal("0"))
    for (
        state_code,
        person_id,
        course_id,
    ), hours_values in hours_seen_by_course.items():
        if len(hours_values) > 1:
            conflicts.append(
                CourseHoursConflict(
                    state_code=state_code,
                    person_id=person_id,
                    course_id=course_id,
                    content_hours_values=sorted(hours_values),
                )
            )
            continue
        hours_by_person[(state_code, person_id)] += next(iter(hours_values))

    if conflicts:
        logging.warning(
            "Excluded [%d] Edovo course(s) with conflicting content_hours from crediting.",
            len(conflicts),
        )

    return EdovoCreditCalculation(
        person_credits=[
            PersonCreditResult(
                person_id=person_id,
                state_code=state_code,
                total_pooled_hours=total_hours,
                credits_earned=pooled_credits_for_hours(
                    total_hours,
                    hours_per_credit=_hours_per_credit_for_state(state_code),
                ),
            )
            for (state_code, person_id), total_hours in sorted(hours_by_person.items())
        ],
        unresolved=[
            UnresolvedCompletion(
                state_code=state_code,
                id_type=id_type,
                person_external_id=external_id,
            )
            for state_code, id_type, external_id in sorted(unresolved)
        ],
        conflicts=sorted(
            conflicts, key=lambda c: (c.state_code, c.person_id, c.course_id)
        ),
    )
