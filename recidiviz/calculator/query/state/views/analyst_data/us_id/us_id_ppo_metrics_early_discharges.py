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
"""Metric capturing early discharge grants and their request records"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_NAME = "us_id_ppo_metrics_early_discharges"
US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_DESCRIPTION = """View capturing early discharge grants and their associated early discharge request records"""

US_ID_PPO_METRICS_EARLY_DISCHARGES_QUERY_TEMPLATE = """
    /*{description}*/
    
    WITH valid_early_discharge_requests AS 
    (
      SELECT 
        state_code, 
        person_id, 
        request_date
      FROM `{project_id}.{base_dataset}.state_early_discharge`
      WHERE decision_status != 'INVALID'
    ),
    all_discharges AS 
    (
      /* Probation discharges from supervision periods ending with termination reason 'DISCHARGE' */
      SELECT DISTINCT
        state_code, 
        person_id, 
        termination_date as end_date, 
        supervision_period_supervision_type AS supervision_type  
      FROM `{project_id}.{base_dataset}.state_supervision_period`
      WHERE termination_reason = 'DISCHARGE'
      
      UNION DISTINCT 
      
      /* Parole discharges from completed incarceration sentences with status code 'F' */
      SELECT DISTINCT
        state_code, 
        person_id, 
        completion_date as end_date, 
        'PAROLE' AS supervision_type,
      FROM `{project_id}.{base_dataset}.state_incarceration_sentence`
      WHERE status = 'COMPLETED' AND status_raw_text = 'F'
    )
    /* Subset of all_discharges associated with a valid early discharge request within a 2-year window */
    SELECT state_code, person_id, request_date, end_date, supervision_type
      FROM (
        SELECT 
            state_code, 
            person_id, 
            request_date,
            end_date,
            supervision_type, 
            /* For each discharge request, select the discharge with a grant date closest to request date */
            ROW_NUMBER() OVER (
              PARTITION BY state_code, person_id, request_date 
              ORDER BY ABS(DATE_DIFF(request_date, end_date, DAY))
            ) AS rn
        FROM valid_early_discharge_requests
        JOIN all_discharges
        USING (state_code, person_id)
        )
      WHERE rn = 1
        -- TODO(#4873): Investigate how different threshold impacts join between discharge requests and grants
        AND ABS(DATE_DIFF(request_date, end_date, MONTH)) <= 24
        
"""

US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_NAME,
    view_query_template=US_ID_PPO_METRICS_EARLY_DISCHARGES_QUERY_TEMPLATE,
    description=US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_BUILDER.build_and_print()
