# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Computes Q2 (Escape History) of Idaho's Reclassification of Security Level form at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_SLS_Q2_VIEW_NAME = "us_ix_sls_q2"

US_IX_SLS_Q2_VIEW_DESCRIPTION = """Computes Q2 (Escape History) of Idaho's Reclassification of Security Level form at any point
in time. See details of the Reclassification Form here:
https://drive.google.com/file/d/1lmzOgjjlcMuBlVvYEqfWOlbQ5HGjZpJ_/view?usp=sharing"""

US_IX_SLS_Q2_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
    SELECT 
      state_code,
      person_id,
      SAFE_CAST(LEFT(EscapeDate, 10) AS DATE) AS start_datetime,
      DATE(NULL) AS end_datetime,
      --if the EscapeTypeId is the least severe, only check within the last 5 years 
      --otherwise check in the last 10 years
      IF(EscapeTypeId = '143', 
        DATE_ADD(SAFE_CAST(LEFT(EscapeDate, 10) AS DATE), INTERVAL 5 YEAR),
        DATE_ADD(SAFE_CAST(LEFT(EscapeDate, 10) AS DATE), INTERVAL 10 YEAR)) AS critical_date,
      EscapeTypeId
    FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_Escape_latest` scl
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON scl.OffenderId = pei.external_id
         AND pei.state_code = 'US_IX'
         AND pei.id_type = 'US_IX_DOC'
    WHERE EscapeTypeId IN ('141', '142', '143')
),
{critical_date_has_passed_spans_cte(attributes=['EscapeTypeId'])},
score_spans AS(
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        CASE 
          WHEN EscapeTypeId = '141' AND NOT critical_date_has_passed THEN 10
          WHEN EscapeTypeId = '142' AND NOT critical_date_has_passed THEN 7
          WHEN EscapeTypeId = '143' AND NOT critical_date_has_passed THEN 4
          ELSE 0
          END AS q2_score
        FROM critical_date_has_passed_spans
),
{create_sub_sessions_with_attributes('score_spans')},
deduped_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        MAX(q2_score) AS q2_score
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
)   
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    q2_score
FROM ({aggregate_adjacent_spans(table_name='deduped_sessions', attribute=['q2_score'])})
    
"""

US_IX_SLS_Q2_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_SLS_Q2_VIEW_NAME,
    description=US_IX_SLS_Q2_VIEW_DESCRIPTION,
    view_query_template=US_IX_SLS_Q2_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_SLS_Q2_VIEW_BUILDER.build_and_print()
