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
"""Computes Q5 (DORs) of Idaho's Reclassification of Security Level form at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_SLS_Q5_VIEW_NAME = "us_ix_sls_q5"

US_IX_SLS_Q5_VIEW_DESCRIPTION = """Computes Q5 (DORs) of Idaho's Reclassification of Security Level form at any point
in time. See details of the Reclassification Form here:
https://drive.google.com/file/d/1-Y3-RAqPEUrAKoeSdkNTDmB-V1YAEp1l/view"""

US_IX_SLS_Q5_QUERY_TEMPLATE = f"""
WITH dors AS (
    SELECT 
      state_code,
      person_id,
      incident_date,
      SUBSTR(JSON_EXTRACT_SCALAR(incident_metadata, '$.OffenseCode'), 1, 1) AS dor_class,
      CASE 
        WHEN JSON_EXTRACT_SCALAR(incident_metadata, '$.OffenseCode') IN 
                                              ('A02',
                                              'A09',
                                              'A11',
                                              'A12',
                                              'A19',
                                              'A21',
                                              'A23',
                                              'A24') THEN 1
      WHEN JSON_EXTRACT_SCALAR(incident_metadata, '$.OffenseCode') IN ('A03','A10','A22') THEN 2
      ELSE NULL 
      END AS dor_enhancement
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident`
    WHERE state_code = 'US_IX'
    --only query A,B,C DORs. There are INF DORs in this table that are infractions and should not be considered here
    AND SUBSTR(JSON_EXTRACT_SCALAR(incident_metadata, '$.OffenseCode'), 1, 1) IN ('A', 'B', 'C')
    ),
critical_date_spans AS (
  SELECT
    state_code,
    person_id,
    incident_date AS start_datetime,
    DATE(NULL) AS end_datetime,
    dor_class,
    dor_enhancement,
    CASE
      WHEN dor_class = 'A' AND dor_enhancement = 1 THEN DATE_ADD(incident_date, INTERVAL 5 YEAR)
      WHEN dor_class = 'A' AND dor_enhancement = 2 THEN DATE_ADD(incident_date, INTERVAL 3 YEAR)
      WHEN dor_class = 'A' AND dor_enhancement IS NULL THEN DATE_ADD(incident_date, INTERVAL 1 YEAR)
      WHEN dor_class = 'B' THEN DATE_ADD(incident_date, INTERVAL 1 YEAR)
      WHEN dor_class = 'C' THEN DATE_ADD(incident_date, INTERVAL 1 YEAR)
    END AS critical_date
  FROM dors
),
{critical_date_has_passed_spans_cte(attributes=['dor_class', 'dor_enhancement'])},
score_spans AS(
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        CASE 
          WHEN dor_class = 'A' AND dor_enhancement = 1 AND NOT critical_date_has_passed THEN 25
          WHEN dor_class = 'A' AND dor_enhancement = 2 AND NOT critical_date_has_passed THEN 23
          WHEN dor_class = 'A' AND dor_enhancement IS NULL AND NOT critical_date_has_passed THEN 20
          WHEN dor_class = 'B' AND NOT critical_date_has_passed THEN 7
          WHEN dor_class = 'C' AND NOT critical_date_has_passed THEN 0
          ELSE -1 
          END AS q5_score
        FROM critical_date_has_passed_spans
),
{create_sub_sessions_with_attributes('score_spans')},
deduped_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        MAX(q5_score) AS q5_score
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
)
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    q5_score
FROM ({aggregate_adjacent_spans(table_name='deduped_sessions', attribute=['q5_score'])})
"""

US_IX_SLS_Q5_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_SLS_Q5_VIEW_NAME,
    description=US_IX_SLS_Q5_VIEW_DESCRIPTION,
    view_query_template=US_IX_SLS_Q5_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_SLS_Q5_VIEW_BUILDER.build_and_print()
