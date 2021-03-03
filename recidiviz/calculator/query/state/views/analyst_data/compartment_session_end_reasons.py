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
"""Combined view of supervision terminations and incarceration releases used to determine the end reason of a session"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
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
        release_date AS end_date,
        COALESCE(release_reason, 'INTERNAL_UNKNOWN') AS end_reason,
        'INCARCERATION' AS compartment_level_1,
        metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, release_date ORDER BY COALESCE(priority, 999)) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics_materialized` AS m
    LEFT JOIN `{project_id}.{analyst_dataset}.release_termination_reason_dedup_priority` AS d
        ON d.end_reason = m.release_reason
        AND d.metric_source = m.metric_type
    WHERE end_reason IS NOT NULL
    UNION ALL  
    SELECT 
        person_id,
        state_code,
        termination_date AS end_date,
        COALESCE(termination_reason, 'INTERNAL_UNKNOWN') AS end_reason,
        'SUPERVISION' AS compartment_level_1,
        metric_type AS metric_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, termination_date ORDER BY COALESCE(priority, 999)) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_termination_metrics_materialized` m
    LEFT JOIN `{project_id}.{analyst_dataset}.release_termination_reason_dedup_priority` AS d
        ON  d.end_reason = m.termination_reason
        AND d.metric_source = m.metric_type
    WHERE end_reason IS NOT NULL
    )
    SELECT 
        * EXCEPT (rn)
    FROM release_metric_cte
    WHERE rn = 1
    ORDER BY 1,2
    """

COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SESSION_END_REASONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSION_END_REASONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSION_END_REASONS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER.build_and_print()
