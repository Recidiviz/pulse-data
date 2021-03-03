# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Combined view of supervision starts, incarceration admissions, and supervision revocations used to determine the
start reason of a session"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSION_START_REASONS_VIEW_NAME = "compartment_session_start_reasons"

COMPARTMENT_SESSION_START_REASONS_VIEW_DESCRIPTION = """Combined view of supervision starts, incarceration admissions, and supervision revocations used to determine the
    start reason of a session"""

COMPARTMENT_SESSION_START_REASONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH start_metric_cte AS
    /*
    This query combines together 3 dataflow metrics (INCARCERATION_ADMISSION, SUPERVISION_REVOCATION, SUPERVISION_START)
    and then does some de-duplicating across person / days. Each of these metrics is first de-duplicated individually.
    For INCARCERATION_ADMISSION and SUPERVISION_START, look-up tables are used to prioritize start reasons when there is
    more than one start for a given person on a given day. This is less of an issue with the SUPERVISION_REVOCATION and 
    the de-duplicating logic is just handled within the query.
    
    Then there is a second de-duplication that is done on the union of these three CTEs. However, it is intentionally
    not enforced that rows are unique on person_id and start_date, but instead unique on person_id, 
    start_date, AND compartment_level_1, which is the compartment that the start pertains to (INCARCERATION or 
    SUPERVISION). This essentially just dedups within incarceration start reasons prioritizing records from the
    SUPERVISION_REVOCATION metric over the INCARCERATION_ADMISSION metric. A person should in theory not be able to have
    a supervision start and an incarceration admission on the same day. However, this does happen and is NOT deduped in 
    this view, but is instead handled by the join with sessions (join is done based on person_id, start_date, and 
    compartment_level_1).
    */
    (
    SELECT 
        person_id,
        admission_date AS start_date,
        state_code,
        COALESCE(admission_reason, 'INTERNAL_UNKNOWN') AS start_reason,
        COALESCE(admission_reason, 'INTERNAL_UNKNOWN') AS start_sub_reason,
        'INCARCERATION' as compartment_level_1,
        metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, admission_date ORDER BY COALESCE(priority, 999) ASC) AS reason_priority,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_admission_metrics_materialized` m
    LEFT JOIN `{project_id}.{analyst_dataset}.admission_start_reason_dedup_priority` d
        ON d.metric_source = m.metric_type
        AND d.start_reason = m.admission_reason
    UNION ALL 
    SELECT 
        person_id,
        revocation_admission_date as start_date,
        state_code,
        'REVOCATION' AS start_reason,
        COALESCE(source_violation_type, 'INTERNAL_UNKNOWN') AS start_sub_reason,
        'INCARCERATION' AS compartment_level_1,
        metric_type AS metric_source,
        --This is very rare (2 cases) where a person has more that one revocation (with different reasons) on the same day. 
        --In both cases one of the records has a null reason, so here I dedup prioritizing the non-null one.
        ROW_NUMBER() OVER(PARTITION BY person_id, revocation_admission_date ORDER BY IF(source_violation_type IS NULL, 1, 0)) AS reason_priority,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_revocation_metrics_materialized`
    UNION ALL
    SELECT 
        person_id,
        start_date,
        state_code,
        COALESCE(admission_reason, 'INTERNAL_UNKNOWN') AS start_reason,
        COALESCE(admission_reason, 'INTERNAL_UNKNOWN') AS start_sub_reason,
        'SUPERVISION' as compartment_level_1,
        metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, start_date ORDER BY COALESCE(priority, 999) ASC) AS reason_priority,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_start_metrics_materialized` m
    --The main logic here is to de-prioritize transfers when they are concurrent with another reason
    LEFT JOIN `{project_id}.{analyst_dataset}.admission_start_reason_dedup_priority` d
        ON d.metric_source = m.metric_type
        AND d.start_reason = m.admission_reason
    )
    SELECT 
        * EXCEPT(reason_priority, metric_source_priority)
    FROM 
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY person_id, start_date, compartment_level_1 
                ORDER BY IF(metric_source = 'SUPERVISION_REVOCATION', 0, 1)) AS metric_source_priority
        FROM start_metric_cte
        WHERE reason_priority = 1
        )
    WHERE metric_source_priority = 1
    ORDER BY 1,2
    """

COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SESSION_START_REASONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSION_START_REASONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSION_START_REASONS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER.build_and_print()
