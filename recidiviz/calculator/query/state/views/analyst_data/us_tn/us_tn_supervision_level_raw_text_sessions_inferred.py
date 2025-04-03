# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Sessionized view of raw-text supervision levels in TN, accounting for backdating to
infer start and end dates that more accurately represent the client's actual supervision
experience.

In TN, changes to supervision levels are generally backdated or postdated to the 1st of
the month. To work around the backdating (which can make it appear in the data as if a
client's level was changed before they actually experienced that change in real life),
we incorporate data from `SupervisionPlan` to infer adjusted start and end dates for
supervision-level sessions.

We start with the `supervision_level_raw_text_sessions` view and adjust dates with input
from `SupervisionPlan`, rather than trying to recreate an alternative view based solely
on raw data here. We do this because the `sessions` dataset already reflects
normalization, deduping, and sessionization that we want to preserve. (For example, the
downstream view may already have accounted for overlapping periods or other issues, and
we don't want to re-do that logic here.) We end up starting with
`supervision_level_raw_text_sessions` as our source of truth here, then, and shift
session start/end dates within that view when we feel confident that backdating was
happening.

We take a conservative approach to adjusting dates here, inferring different start/end
dates only under the following conditions that we think indicate that a change in
supervision level was likely to have been backdated:
    - The supervision plan assigning a client to a given supervision level has a
    `PostedDate` that comes after the plan's start date but within the same month.
    - The `PostedDate` of the plan corresponding to a level is before the end date of
    the session for that level. (If it comes after, we think that some kind of belated
    data entry/clean-up was happening, rather than backdating. We only want to correct
    for backdating when we think that the `PostedDate` indicates the date of an actual
    transition.)

Below is an example of the simplest version of how this works, where we match the plan
(with its `PostedDate`) for a given supervision level to the existing session for that
level, then adjust the start and end dates when backdating occurred. The plans for
levels A, B, and D were backdated, so we adjust dates accordingly. (The plan for C,
which is posted on the same day as the session start, does not seem to be backdated.)
    level       start       end     PostedDate      adjusted start      adjusted end
    -----       -----       ---     ----------      --------------      ------------
    A           1/1         2/1     1/1             1/1                 2/10
    B           2/1         3/1     2/10            2/10                3/1
    C           3/1         4/1     3/1             3/1                 4/5
    D           4/1         null    4/5             4/5                 null

Here's another example that shows more nuance. In the example below, even though the
plan for level V looks backdated, we don't move up the start date because it's the first
session in a continuous period on supervision. The plan for W is backdated, so we adjust
accordingly. The plan for X looks like post hoc data entry (after the session has
already ended), so we don't adjust any dates to match that `PostedDate`. We can't match
a plan to level Y (perhaps because plans in TN usually start on the 1st of the month,
but something in our data pipeline already adjusted that date), so neither the end date
for X nor the start date for Y is adjusted. Finally, though the start date gets adjusted
for level Z, since Z is the last session in a continuous period of supervision, its
end date doesn't get adjusted.
    level       start       end     PostedDate      adjusted start      adjusted end
    -----       -----       ---     ----------      --------------      ------------
    V           1/5         2/1     1/15            1/5                 2/10
    W           2/1         3/1     2/10            2/10                3/1
    X           3/1         3/15    3/20            3/1                 3/15
    Y           3/15        4/1     null            3/15                4/15
    Z           4/1         4/25    4/15            4/15                4/25
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_VIEW_NAME = (
    "us_tn_supervision_level_raw_text_sessions_inferred"
)

# TODO(#39827): Consider moving this logic upstream at some point (if we think it would
# be correct/beneficial/possible to do so).
# TODO(#40144): If we decide to undo the backdating workarounds for TN, we may want to
# delete this view.
US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_QUERY_TEMPLATE = f"""
    WITH supervision_level_raw_text_sessions AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            supervision_level,
            supervision_level_raw_text,
            case_type,
            /* The following field helps us check if the session in question is the
            first in a continuous supervision session defined by `date_gap_id`). */
            (ROW_NUMBER() OVER continuous_session_window)=1 AS first_in_continuous_session,
            /* The following fields will be null if there is no subsequent session
            (i.e., if the session in question is a currently active one or is the last
            session in a continuous supervision session). */
            LEAD(supervision_level_raw_text) OVER continuous_session_window AS next_supervision_level_raw_text_within_continuous_session,
            LEAD(end_date_exclusive) OVER continuous_session_window AS next_end_date_exclusive_within_continuous_session,
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized`
        WHERE state_code='US_TN'
        WINDOW continuous_session_window AS (
            PARTITION BY state_code, person_id, date_gap_id
            ORDER BY start_date
        )
    ),
    prioritized_plans AS (
        SELECT
            pei.state_code,
            pei.person_id,
            DATE(sp.PlanStartDate) AS PlanStartDate,
            DATE(sp.PostedDate) AS PostedDate,
            sp.SupervisionLevel,
        FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.SupervisionPlan_latest` sp
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON sp.OffenderID=pei.external_id
            AND pei.id_type='US_TN_DOC'
        /* In case there are multiple plans entered for a client that have the same
        start date + supervision level, we deduplicate here by keeping the plan that was
        posted first. */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pei.state_code, pei.person_id, DATE(sp.PlanStartDate), sp.SupervisionLevel
            ORDER BY DATE(sp.PostedDate)
        ) = 1
    ),
    inferred_sessions AS (
        SELECT
            slrts.state_code,
            slrts.person_id,
            /* Adjust start dates by inferring when backdating happened. */
            IF(
                (
                    /* If this is the first period in a continuous supervision session,
                    we don't want to allow the start date to be pushed forward, since we
                    still want to cover all periods in supervision. */
                    (NOT first_in_continuous_session)
                    /* If the plan's `PostedDate` is after the `start_date` (but in the
                    same calendar month), then we think the change to the client's
                    supervision level was backdated. In these cases, we think the client
                    was still on their previous supervision level up until the
                    `PostedDate`, and so we infer that the client didn't actually start
                    on the new level until the `PostedDate`. */
                    AND (current_plan.PostedDate>slrts.start_date)
                    AND (DATE_TRUNC(current_plan.PostedDate, MONTH)=DATE_TRUNC(slrts.start_date, MONTH))
                    /* There are cases where `PostedDate` comes after the end of a given
                    supervision-level session. Replacing the `start_date` with the
                    `PostedDate` in these cases could create overlapping spans and/or
                    situations where a span's start date comes after its end date. We
                    think these situations are ones where `PostedDate` reflects some
                    post hoc data entry/clean-up, since the date comes after a session
                    has already ended, rather than evidence of `start_date` being
                    backdated, so we don't adjust `start_date` in such cases.
                    We use the `nonnull_end_date_clause` here because we don't want to
                    subtract a day from the exclusive end date when we do this logic. */
                    AND (current_plan.PostedDate<{nonnull_end_date_clause('slrts.end_date_exclusive')})
                ),
                current_plan.PostedDate,
                slrts.start_date
            ) AS start_date,
            /* Adjust end dates via similar inference. Note that if the end date of a
            session is adjusted here, the start date of that next session should be
            similarly adjusted because the logic here parallels the logic above. This
            should ensure we don't end up with gaps or with overlapping sessions. */
            IF (
                /* If there's no next plan because the session in question is currently
                open or the last in a continuous supervision session, the conditions
                below will end up being fully null since the `next_plan` fields will be
                null (and so we don't need to worry about non-nulling the
                `end_date_exclusive` fields, as it won't make a difference). */
                (
                    /* If the next plan's `PostedDate` is after the `end_date_exclusive`
                    of the span associated with the current plan (but in the same
                    calendar month), then we think the next change to the client's
                    supervision level was backdated. In these cases, we think the client
                    was still on their current supervision level up until the
                    `PostedDate` of the next plan, and so we infer that the client
                    didn't actually start on the new level until the next plan's
                    `PostedDate`. */
                    (next_plan.PostedDate>slrts.end_date_exclusive)
                    AND (DATE_TRUNC(next_plan.PostedDate, MONTH)=DATE_TRUNC(slrts.end_date_exclusive, MONTH))
                    /* As above, we restrict inference to cases where the `PostedDate`
                    of the next plan comes before the end of the next supervision-level
                    session to try to adjust for backdating but not post hoc data
                    entry/clean-up.
                    We use the `nonnull_end_date_clause` here because we don't want to
                    subtract a day from the exclusive end date when we do this logic. */
                    AND (next_plan.PostedDate<{nonnull_end_date_clause('slrts.next_end_date_exclusive_within_continuous_session')})
                ),
                next_plan.PostedDate,
                slrts.end_date_exclusive
            ) AS end_date_exclusive,
            slrts.supervision_level,
            slrts.supervision_level_raw_text,
            case_type,
        FROM supervision_level_raw_text_sessions slrts
        /* Join each session to the plan corresponding to that session (to be used to
        correct start dates). */
        LEFT JOIN prioritized_plans current_plan
            ON slrts.state_code=current_plan.state_code
            AND slrts.person_id=current_plan.person_id
            /* Join to plan(s) with same plan start date as the date of the observed
            supervision-level start. */
            AND slrts.start_date=current_plan.PlanStartDate
            /* Join to plan(s) that have the same supervision level as the level onto
            which the client is transitioning. (We do this to address instances in which
            we see multiple plans with the same `PlanStartDate` but different
            supervision levels.) */
            AND slrts.supervision_level_raw_text=current_plan.SupervisionLevel
        /* Join each session to the plan corresponding to the following session (to be
        used to correct end dates). */
        LEFT JOIN prioritized_plans next_plan
            ON slrts.state_code=next_plan.state_code
            AND slrts.person_id=next_plan.person_id
            /* Join to plan(s) with same plan end date as the date of the subsequent
            observed supervision-level session start. 
            Because of the below condition, note that if the session in question is
            currently open and has a null `end_date_exclusive`, no plan data will be
            joined in here. */
            AND slrts.end_date_exclusive=next_plan.PlanStartDate
            /* Join to plan(s) that have the same supervision level as the level of the
            client's subsequent session start. */
            AND slrts.next_supervision_level_raw_text_within_continuous_session=next_plan.SupervisionLevel
    )
    SELECT
        *,
        /* These fields are present in the original sessions view, so we re-create them
        here too. */
        DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) AS end_date,
        LAG(supervision_level) OVER w AS previous_supervision_level,
        LAG(supervision_level_raw_text) OVER w AS previous_supervision_level_raw_text,
    FROM (
        /* Aggregate adjacent spans so that we can end up with the same session and
        date-gap ID fields as the original sessions view. We don't expect any actual
        aggregation to happen here, since the original view was already aggregated, but
        having these ID fields can be helpful for debugging and validating this view.
        We'd expect that this view shifts dates but not overall numbers of session
        or date-gap IDs. */
        {aggregate_adjacent_spans(
            "inferred_sessions",
            end_date_field_name="end_date_exclusive",
            session_id_output_name="supervision_level_session_id",
            attribute=['supervision_level', 'supervision_level_raw_text', 'case_type']
        )}
    )
    WINDOW w AS (
        PARTITION BY state_code, person_id, date_gap_id
        ORDER BY start_date
    )
"""

US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_VIEW_NAME,
    description=__doc__,
    view_query_template=US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN,
        instance=DirectIngestInstance.PRIMARY,
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_VIEW_BUILDER.build_and_print()
