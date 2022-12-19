# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

"""Defines a criteria view that shows spans of time for
which residents are within 90 days of having received a A or B violaiton.
"""
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which residents are within 90 days of having received a A or B violaiton.
"""

_QUERY_TEMPLATE = f"""
WITH disciplinary_cases_cte AS (
  SELECT 
      "US_ME" AS state_code,
      ei.person_id,
      IF(vd.Cis_1813_Disposition_Outcome_Type_Cd IS NULL,
         CONCAT('Pending since ', SAFE_CAST(LEFT(dc.CREATED_ON_DATE, 10) AS STRING)),
         vdt.E_Violation_Disposition_Type_Desc) 
            AS disp_type,
      vdc.E_Violation_Disposition_Class_Desc AS disp_class,
      vd.Cis_1813_Disposition_Outcome_Type_Cd AS disp_outcome,
      SAFE_CAST(LEFT(dc.HEARING_ACTUALLY_HELD_DATE, 10) AS DATE) AS start_date,
      SAFE_CAST(LEFT(dc.CREATED_ON_DATE, 10) AS DATE) AS pending_violation_start_date,
      dc.CIS_462_CLIENTS_INVOLVED_ID,
      ci.Cis_100_Client_Id,
  FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_181_VIOLATION_DISPOSITION_latest`  vd
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_180_DISCIPLINARY_CASE_latest` dc
    ON vd.Cis_180_Disciplinary_Case_Id = dc.DISCIPLINARY_CASE_ID
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_1811_VIOLATION_DISPOSITION_TYPE_latest` vdt
    ON vd.Cis_1811_Violation_Type_Cd = vdt.Violation_Disposition_Type_Cd
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_1810_VIOLATION_DISPOSITION_CLASS_latest` vdc
    ON vd.Cis_1810_Violation_Class_Cd = vdc.Violation_Disposition_Class_Cd
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_462_CLIENTS_INVOLVED_latest` ci
    ON ci.Clients_Involved_Id = dc.CIS_462_CLIENTS_INVOLVED_ID
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` ei
      ON ci.Cis_100_Client_Id = external_id
      AND id_type = 'US_ME_DOC'
  WHERE
      vdc.E_Violation_Disposition_Class_Desc in ('A', 'B')
      # Drop if logical delete = yes
      AND COALESCE(dc.LOGICAL_DELETE_IND, 'N') != 'Y'
      AND COALESCE(vd.Logical_Delete_Ind , 'N') != 'Y'
      AND COALESCE(dc.DISCIPLINARY_ACTION_FORMAL_IND, 'Y') != 'N'
      
),
cases_wstart_end_cte AS (
  -- Resolved disciplines
  SELECT
        * EXCEPT (start_date, pending_violation_start_date),
        start_date,
        DATE_ADD(start_date, INTERVAL 90 DAY) AS end_date,
        -- Keep another date so it doesn't get lost in a sub-session later
        DATE_ADD(start_date, INTERVAL 90 DAY) AS eligible_date,
  FROM disciplinary_cases_cte
  WHERE disp_outcome IS NOT NULL

  UNION ALL

  -- Pending disciplines
  SELECT
  -- Create an open span on the pending start date if the violation disposition outcome is not set
        * EXCEPT (start_date, pending_violation_start_date),
        pending_violation_start_date AS start_date,
        NULL AS end_date,
        -- Keep another date so it doesn't get lost in a sub-session later
        NULL AS eligible_date,
  FROM disciplinary_cases_cte
  WHERE disp_outcome IS NULL
),
{create_sub_sessions_with_attributes(table_name='cases_wstart_end_cte')},
no_dup_subsessions_cte AS (
  -- Drop duplicate sub-sessions by keeping the class with the highest priority
  -- but keep the highest end_date as an eligible_date
  SELECT * EXCEPT(disp_type, eligible_date),
      -- If more than 1 violation at the time, state that there are 'More than 1'
      IF(SUM(1) OVER(PARTITION BY person_id, state_code, start_date, end_date) > 1,
        'More than 1',
        disp_type) AS disp_type_wmorethan1,
      -- When 2 duplicates subsessions are present, we keep the highest eligible date
      MAX({nonnull_end_date_clause('eligible_date')}) OVER(PARTITION BY person_id, state_code, start_date, end_date)
        AS eligible_date,
  FROM sub_sessions_with_attributes
  -- Drop cases where resident was 'Found Not Guilty' or 'Dismissed (Technical)'
  WHERE disp_outcome NOT IN ('5', '10', '3', '8') 
        OR disp_outcome IS NULL
  -- Keep the violaiton with the highest priority
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date
                            ORDER BY disp_class) = 1
)

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT(disp_class AS highest_class_viol,
                   disp_type_wmorethan1 AS viol_type,
                   {revert_nonnull_end_date_clause('eligible_date')} AS eligible_date)) AS reason
FROM no_dup_subsessions_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
