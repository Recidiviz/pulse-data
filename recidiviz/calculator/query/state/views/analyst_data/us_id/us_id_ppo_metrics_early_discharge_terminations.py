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
"""Metric capturing proportion of supervision periods successfully terminated
in a month via a granted early discharge"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_NAME = (
    "us_id_ppo_metrics_early_discharge_terminations"
)

US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_DESCRIPTION = """View capturing proportion of successful supervision terminations via granted early discharge in the past 2 years.
    Successful termination defined as transition from SUPERVISION to RELEASE compartment, and early discharge grants 
    identified from either supervision period termination reasons (probation) or incarceration sentence status (parole)"""

US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_QUERY_TEMPLATE = """
    /*{description}*/

    WITH successful_terminations_per_month AS 
    /* Proportion of successful terminations by month and supervision type with an associated early discharge grant, 
    where successful terminations are associated with the discharge grant closest to termination date within a 
    2-year window */
    (
      SELECT 
        state_code, 
        supervision_type, 
        termination_month,
        termination_via_early_discharge
      FROM (
        SELECT
            t.state_code, 
            t.supervision_type, 
            DATE_TRUNC(t.end_date, MONTH) AS termination_month,
            IF(ed.end_date IS NULL, 0, 1) AS termination_via_early_discharge,
            
            /* For each supervision termination, select discharge with grant date closest to termination date */
            ROW_NUMBER() OVER (
                PARTITION BY t.state_code, t.person_id, t.supervision_type, t.end_date
                ORDER BY ABS(DATE_DIFF(t.end_date, COALESCE(ed.end_date, '9999-12-31'), DAY))
                ) AS closest_end_date_rank
        FROM `{project_id}.{analyst_dataset}.us_id_ppo_metrics_successful_supervision_terminations` t
        LEFT JOIN `{project_id}.{analyst_dataset}.us_id_ppo_metrics_early_discharges` ed
        ON t.person_id = ed.person_id
            AND t.state_code = ed.state_code
            AND t.supervision_type = ed.supervision_type
            /* Discharge grant date must occur within 2 years of session termination date */
            -- TODO(#4873): Investigate how different threshold impacts join between termination and discharge dates
            AND ABS(DATE_DIFF(t.end_date, ed.end_date, MONTH)) <= 24
        WHERE t.state_code = 'US_ID'
        ORDER BY t.person_id, ed.person_id
      )
      WHERE closest_end_date_rank = 1
    )
    
    SELECT 
        state_code,
        /* Convert month label from first of month to last of month. If month is not yet complete, use current date \
        instead of the last of the month to indicate last date of available data */
        LEAST(CURRENT_DATE(), LAST_DAY(termination_month, MONTH)) AS termination_month,
        supervision_type, 
        SUM(termination_via_early_discharge) as early_discharges,
        COUNT(*) as all_terminations,
        SUM(termination_via_early_discharge)/COUNT(*) as prop_terminations_via_early_discharge
    FROM successful_terminations_per_month
    WHERE termination_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """

US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_NAME,
    view_query_template=US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_QUERY_TEMPLATE,
    description=US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_BUILDER.build_and_print()
