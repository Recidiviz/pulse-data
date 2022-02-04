# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Combined view of supervision starts and incarceration admissions used to determine
the start reason of a session"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSION_START_REASONS_VIEW_NAME = "compartment_session_start_reasons"

COMPARTMENT_SESSION_START_REASONS_VIEW_DESCRIPTION = """Combined view of supervision
 starts and incarceration admissions, used to determine the start reason of a session"""

COMPARTMENT_SESSION_START_REASONS_QUERY_TEMPLATE = """
    /*{description}*/
    /*
    This query combines together 2 dataflow metrics (INCARCERATION_ADMISSION, SUPERVISION_START)
    and then does some de-duplicating across person / days. Each of these metrics is de-duplicated individually.
    Look-up tables are used to prioritize start reasons when there is more than one 
    start for a given person on a given day. This is less of an issue with the SUPERVISION_REVOCATION and 
    the de-duplicating logic is just handled within the query. The 
    INCARCERATION_COMMITMENT_FROM_SUPERVISION metric is also brought in to gather 
    some additional details on incarceration admissions that qualify as commitments 
    from supervision.

    A person should in theory not be able to have a supervision start and an 
    incarceration admission on the same day. However, this does happen and is NOT deduped in 
    this view, but is instead handled by the join with sessions (join is done based on person_id, start_date, and 
    compartment_level_1).
    */
   WITH start_metric_cte AS (
    SELECT
        person_id,
        admission_date as start_date,
        state_code,
        COALESCE(admission_reason, 'INTERNAL_UNKNOWN') AS start_reason,
        adm.admission_reason_raw_text AS start_reason_raw_text,
        most_severe_violation_type AS start_sub_reason,
        'INCARCERATION' as compartment_level_1,
        adm.metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, admission_date
                             ORDER BY COALESCE(priority, 999) ASC,
                                     --This is very rare (2 cases) where a person has more that one revocation (with different reasons) on the same day. 
                                     --In both cases one of the records has a null reason, so here I dedup prioritizing the non-null one.
                                      IF(most_severe_violation_type IS NULL, 1, 0)) AS reason_priority,
    FROM
     `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_admission_metrics_included_in_state_population_materialized` adm
    LEFT JOIN 
     `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized` cfs
    USING (state_code, person_id, admission_date, admission_reason)
    LEFT JOIN `{project_id}.{sessions_dataset}.admission_start_reason_dedup_priority` d
        ON d.metric_source = adm.metric_type
        AND d.start_reason = adm.admission_reason
    UNION ALL
    SELECT 
        person_id,
        start_date,
        state_code,
        COALESCE(admission_reason, 'INTERNAL_UNKNOWN') AS start_reason,
        CAST(NULL AS STRING) AS start_reason_raw_text,
        CAST(NULL AS STRING) AS start_sub_reason,
        'SUPERVISION' as compartment_level_1,
        metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, start_date ORDER BY COALESCE(priority, 999) ASC) AS reason_priority,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_start_metrics_materialized` m
    --The main logic here is to de-prioritize transfers when they are concurrent with another reason
    LEFT JOIN `{project_id}.{sessions_dataset}.admission_start_reason_dedup_priority` d
        ON d.metric_source = m.metric_type
        AND d.start_reason = m.admission_reason
    )
    ,
    prep_cte AS
    (
    SELECT DISTINCT
        starts.* EXCEPT(reason_priority),
        -- TODO(#8131): Pull these boolean flags directly from the dataflow metrics
        inc_pop.person_id IS NOT NULL AS in_incarceration_population_on_date,
        COALESCE(sup_pop.person_id,sup_oos_pop.person_id) IS NOT NULL AS in_supervision_population_on_date, 
        COALESCE(releases.person_id, terminations.person_id) IS NOT NULL AS same_day_start_end,
    FROM start_metric_cte starts
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized` inc_pop
        ON starts.person_id = inc_pop.person_id
        AND starts.start_date = inc_pop.date_of_stay 
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized` sup_pop
        ON starts.person_id = sup_pop.person_id
        AND starts.start_date = sup_pop.date_of_supervision 
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_out_of_state_population_metrics_materialized` sup_oos_pop
        ON starts.person_id = sup_oos_pop.person_id
        AND starts.start_date = sup_oos_pop.date_of_supervision 
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics_included_in_state_population_materialized` releases
        ON starts.person_id = releases.person_id
        AND starts.start_date = releases.release_date
        AND starts.compartment_level_1 = 'INCARCERATION'
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_termination_metrics_materialized` terminations
        ON starts.person_id = terminations.person_id
        AND starts.start_date = terminations.termination_date
        AND starts.compartment_level_1 = 'SUPERVISION'
    WHERE reason_priority = 1
    )
    SELECT
        *,
        /*
        Exclude dataflow events if any of the following conditions are met:
        1.  Event is a same day start/end where the person also does not appear in the population metric on that day.
            This captures single day periods which don't appear in sessions.
        2.  Event is a supervision dataflow start/termination that occurs while a person is incarcerated. This addresses
            events that don't line up because of supervision periods that overlap incarceration periods.
        3.  Dataflow event is a transfer or of unknown type
        */
        IF(
            (compartment_level_1 = 'INCARCERATION'
                AND same_day_start_end
                AND NOT in_incarceration_population_on_date
            )
            OR
            (compartment_level_1 = 'SUPERVISION'
                AND same_day_start_end
                AND NOT in_supervision_population_on_date
            )
            OR
            (compartment_level_1 = 'SUPERVISION'
                AND in_incarceration_population_on_date
            )
            OR start_reason IN ('TRANSFER','TRANSFER_WITHIN_STATE','INTERNAL_UNKNOWN','EXTERNAL_UNKNOWN'),
        FALSE, TRUE) AS valid_dataflow_event
    FROM prep_cte
    """

COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_SESSION_START_REASONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSION_START_REASONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSION_START_REASONS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER.build_and_print()
