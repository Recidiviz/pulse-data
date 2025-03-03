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
"""Identify when a TN client's supervision level has been downgraded, excluding
downgrades to limited supervision (Compliant Reporting).

In TN, changes to supervision levels are generally backdated or postdated to the 1st of
the month. To work around the backdating (which can make it appear in the data as if a
client's level was changed before they actually experienced that change in real life),
we incorporate data from `SupervisionPlan` to infer the date on which an officer
actually initiated the downgrade. We don't try to work around postdating because we are
interested in tracking when the client actually experienced the transition, and in cases
of postdating, the client still wouldn't experience the transition until the later date.
"""

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
    WITH downgrades AS (
        SELECT
            state_code,
            person_id,
            sls.start_date AS observed_transition_date,
            slrts.supervision_level_raw_text AS new_supervision_level_raw_text,
        /* Note that `supervision_level_sessions` does not contain spans that start in
        the future, so we won't pick up any transitions here that have been initiated by
        officers but are postdated into the future. This should be fine, since these
        clients with future transitions presumably haven't experienced the actual
        transitions yet. */
        FROM `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` sls
        /* Join in the raw-text supervision-level sessions so that we can identify the
        raw-text level to which someone was downgraded. This will be used to help us
        correctly match up supervision plans to transitions later in the query. */
        LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_raw_text_sessions_materialized` slrts
            USING (state_code, person_id, start_date, supervision_level)
        WHERE state_code='US_TN'
            AND sls.supervision_downgrade=1
            AND sls.supervision_level!='LIMITED'
    )
    SELECT
        state_code,
        person_id,
        /* If the plan's `PostedDate` is after the `observed_transition_date`, then the
        downgrade was backdated. In these cases, we want to use the `PostedDate` as the
        date of the completion event, since the client presumably did not actually
        experience the downgrade until then. Alternatively, if the `PostedDate` comes
        before the `observed_transition_date`, we just stick to using the
        `observed_transition_date` as the date of the completion event.
        We also require that the `PostedDate` and the `observed_transition_date` be in
        the same calendar month in order for us to replace the observed date. We do this
        because our aim here is to correct only for the commonplace backdating practice
        in TN, not for broader data errors or administrative delays. */
        IF(
            (
                (DATE(sp.PostedDate)>observed_transition_date)
                AND (DATE_TRUNC(DATE(sp.PostedDate), MONTH)=DATE_TRUNC(observed_transition_date, MONTH))
            ),
            DATE(sp.PostedDate),
            observed_transition_date
        ) AS completion_event_date,
    FROM downgrades
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        USING (state_code, person_id)
    LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.SupervisionPlan_latest` sp
        ON pei.external_id=sp.OffenderID
        /* Join to plan(s) with same plan start date as the date of the observed
        transition. */
        AND observed_transition_date=DATE(sp.PlanStartDate)
        /* Join to plan(s) that have the same supervision level as the level to which
        the client is transitioning. (We do this to address uncommon instances in which
        we see multiple plans with the same `PlanStartDate` but different supervision
        levels.) */
        AND new_supervision_level_raw_text=sp.SupervisionLevel
    /* In case there are multiple plans entered for a client that have the same start
    date + supervision level, we deduplicate here by keeping the plan that was posted
    first. (It is very uncommon to have multiple plans with the same start date +
    supervision level that are matched to an observed transition; this deduplication
    just handles those infrequent edge cases.) */
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id, observed_transition_date
        ORDER BY DATE(sp.PostedDate)
    ) = 1
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        completion_event_type=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN,
            instance=DirectIngestInstance.PRIMARY,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
