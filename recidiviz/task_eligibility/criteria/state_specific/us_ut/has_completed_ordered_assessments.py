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
# ============================================================================
"""Spans of time when someone in UT has completed ordered assessments and 
any recommended treatment or programming by a licensed provider.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_UT_HAS_COMPLETED_ORDERED_ASSESSMENTS"

_QUERY_TEMPLATE = """
WITH dedup_assess_by_date AS (
  SELECT 
    peid.state_code,
    peid.person_id,
    SAFE_CAST(LEFT(sc.cntc_dt, 10) AS DATE) AS contact_date,
    CASE 
        WHEN sc.assess_stat_cd = 'C' THEN 'COMPLETE'
        WHEN sc.assess_stat_cd = 'X' THEN 'NOT ORDERED'
        WHEN sc.assess_stat_cd = 'I' THEN 'NOT COMPLETED/ENROLLED/ATTENDING'
        ELSE NULL 
    END AS ordered_assessment_status,
  FROM `{project_id}.us_ut_raw_data_up_to_date_views.sprvsn_cntc_latest` sc
  INNER JOIN `{project_id}.normalized_state.state_person_external_id` peid
    ON sc.ofndr_num = peid.external_id
      AND peid.state_code = 'US_UT'
      AND peid.id_type = 'US_UT_DOC'
  WHERE sc.assess_stat_cd != '(null)'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.state_code, peid.person_id, contact_date ORDER BY 
      CASE 
        WHEN sc.assess_stat_cd = 'C' THEN 0
        WHEN sc.assess_stat_cd = 'X' THEN 1
        WHEN sc.assess_stat_cd = 'I' THEN 2
        ELSE NULL 
    END) = 1 
)

SELECT 
    state_code,
    person_id,
    contact_date AS start_date,
    LEAD(contact_date) OVER(PARTITION BY state_code, person_id ORDER BY contact_date) AS end_date,
    ordered_assessment_status IN ('NOT ORDERED', 'COMPLETE') AS meets_criteria,
    TO_JSON(STRUCT(
        ordered_assessment_status AS ordered_assessment_status
        )) AS reason,
    ordered_assessment_status,
FROM dedup_assess_by_date
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        reasons_fields=[
            ReasonsField(
                name="ordered_assessment_status",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Ordered assessment status",
            ),
        ],
        state_code=StateCode.US_UT,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
