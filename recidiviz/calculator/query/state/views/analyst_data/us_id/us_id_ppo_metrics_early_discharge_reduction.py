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
"""Metric capturing proportional reduction in supervision sentence length via early discharge"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    STATE_BASE_DATASET,
    ANALYST_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_NAME = (
    "us_id_ppo_metrics_early_discharge_reduction"
)

US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_DESCRIPTION = """View capturing proportion of original sentence remaining at time of successful discharge (early discharge grant),
    for successful discharges in the past two years."""

US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_QUERY_TEMPLATE = """
    /*{description}*/

    WITH sentence_remaining AS (
      SELECT 
        state_code, 
        termination_month,
        supervision_type, 
        person_id,
        PERCENTILE_CONT(prop_sentence_left, .5) 
            OVER (PARTITION BY state_code, supervision_type, termination_month) 
            AS median_prop_sentence_left,
        COUNT(*) OVER (PARTITION BY state_code, supervision_type, termination_month) AS n,
        ROW_NUMBER() OVER (PARTITION BY state_code, supervision_type, termination_month) AS rn
      FROM (
        SELECT 
            ed.state_code, 
            ed.person_id, 
            t.supervision_type, 
            t.end_date as session_end_date,
            ed.end_date as early_discharge_date,
            DATE_TRUNC(t.end_date, MONTH) as termination_month,
            DATE_DIFF(c.projected_completion_date_max, t.end_date, DAY)/DATE_DIFF(c.projected_completion_date_max, t.start_date, DAY) as prop_sentence_left,
            ROW_NUMBER() OVER (
                PARTITION BY ed.state_code, ed.supervision_type, ed.person_id 
                ORDER BY ABS(DATE_DIFF(t.end_date, ed.end_date, DAY))
                ) AS closest_end_date_rank
        FROM `{project_id}.{analyst_dataset}.us_id_ppo_metrics_early_discharges` ed
        LEFT JOIN `{project_id}.{analyst_dataset}.us_id_ppo_metrics_successful_supervision_terminations` t
            USING (person_id, state_code, supervision_type)
        LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sentences_materialized` c
            USING (state_code, session_id, person_id)
        WHERE ed.state_code = 'US_ID'
      )
      WHERE closest_end_date_rank = 1
      ORDER BY ABS(DATE_DIFF(early_discharge_date, session_end_date, DAY))
    )

    SELECT 
        state_code, 
        /* Convert month label from first of month to last of month. If month is not yet complete, use current date \
        instead of the last of the month to indicate last date of available data */
        LEAST(CURRENT_DATE(), LAST_DAY(termination_month, MONTH)) AS termination_month,
        supervision_type, 
        median_prop_sentence_left, 
        n,
    FROM sentence_remaining
    WHERE termination_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
        AND rn = 1
    ORDER BY 1, 2, 3
    """

US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_NAME,
    view_query_template=US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_QUERY_TEMPLATE,
    description=US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_BUILDER.build_and_print()
