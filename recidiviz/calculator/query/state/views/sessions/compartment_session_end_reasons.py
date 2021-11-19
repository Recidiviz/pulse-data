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
"""Combined view of supervision terminations and incarceration releases used to determine the end reason of a session"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSION_END_REASONS_VIEW_NAME = "compartment_session_end_reasons"

COMPARTMENT_SESSION_END_REASONS_VIEW_DESCRIPTION = """Combined view of supervision terminations and incarceration releases used to determine the end reason of a
    session"""

COMPARTMENT_SESSION_END_REASONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH release_metric_cte AS
    /*
    This query combines together 2 dataflow metrics (INCARCERATION_RELEASE and SUPERVISION_TERMINATION). Metrics are 
    de-duplicated individually across person / days based on look-up tables that prioritize release reasons. 
    Deduplication is not done across metrics and that is instead handled by the join with sessions.
    */
    (
    SELECT
        person_id,
        state_code,
        release_date AS release_termination_date,
        DATE_SUB(release_date, INTERVAL 1 DAY) AS end_date,
        COALESCE(release_reason, 'INTERNAL_UNKNOWN') AS end_reason,
        'INCARCERATION' AS compartment_level_1,
        metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, release_date ORDER BY COALESCE(priority, 999)) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics_included_in_state_population_materialized` AS m
    LEFT JOIN `{project_id}.{sessions_dataset}.release_termination_reason_dedup_priority` AS d
        ON d.end_reason = m.release_reason
        AND d.metric_source = m.metric_type
    UNION ALL  
    SELECT 
        person_id,
        state_code,
        termination_date AS release_termination_date,
        DATE_SUB(termination_date, INTERVAL 1 DAY) AS end_date,
        COALESCE(termination_reason, 'INTERNAL_UNKNOWN') AS end_reason,
        'SUPERVISION' AS compartment_level_1,
        metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, termination_date ORDER BY COALESCE(priority, 999)) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_termination_metrics_materialized` m
    LEFT JOIN `{project_id}.{sessions_dataset}.release_termination_reason_dedup_priority` AS d
        ON  d.end_reason = m.termination_reason
        AND d.metric_source = m.metric_type
    )
    ,
    prep_cte AS
    (
    SELECT DISTINCT
        ends.* EXCEPT (rn),
        -- TODO(#8131): Pull these boolean flags directly from the dataflow metrics
        inc_pop.person_id IS NOT NULL AS in_incarceration_population_on_date,
        COALESCE(sup_pop.person_id,sup_oos_pop.person_id) IS NOT NULL AS in_supervision_population_on_date,
        COALESCE(admissions.person_id, sup_starts.person_id) IS NOT NULL AS same_day_start_end
    FROM release_metric_cte ends
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized` inc_pop
        ON ends.person_id = inc_pop.person_id
        AND ends.end_date = inc_pop.date_of_stay
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized` sup_pop
        ON ends.person_id = sup_pop.person_id
        AND ends.end_date = sup_pop.date_of_supervision
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_out_of_state_population_metrics_materialized` sup_oos_pop
        ON ends.person_id = sup_oos_pop.person_id
        AND ends.end_date = sup_oos_pop.date_of_supervision
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_admission_metrics_included_in_state_population_materialized` admissions
        ON ends.person_id = admissions.person_id
        AND ends.release_termination_date = admissions.admission_date
        AND ends.compartment_level_1 = 'INCARCERATION'
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_start_metrics_materialized` sup_starts
        ON ends.person_id = sup_starts.person_id
        AND ends.release_termination_date = sup_starts.start_date
        AND ends.compartment_level_1 = 'SUPERVISION'
    WHERE rn = 1
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
            OR end_reason IN ('TRANSFER','TRANSFER_WITHIN_STATE','INTERNAL_UNKNOWN','EXTERNAL_UNKNOWN'),
        FALSE, TRUE) AS valid_dataflow_event
    FROM prep_cte
    """

COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_SESSION_END_REASONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSION_END_REASONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSION_END_REASONS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER.build_and_print()
