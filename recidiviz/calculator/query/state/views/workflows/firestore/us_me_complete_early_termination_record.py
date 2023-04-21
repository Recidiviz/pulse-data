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
"""Queries information needed to display Early Termination eligibility in ME
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.almost_eligible_query_fragments import (
    json_to_array_cte,
    x_time_away_from_eligibility,
)
from recidiviz.task_eligibility.utils.raw_table_import import (
    cis_408_violations_notes_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_STANDARD_PAROLE_CONDITIONS = ", ".join(
    [
        "1",
        "2",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "15",
        "16",
        "17",
        "18",
        "19",
        "21",
        "23",
        "24",
        "25",
        "26",
        "27",
        "36",
        "44",
        "47",
        "58",
        "60",
        "62",
        "63",
        "64",
        "76",
        "107",
        "136",
        "156",
        "157",
        "169",
        "5",
        "6",
        "14",
        "20",
        "30",
        "32",
        "35",
        "39",
        "52",
        "56",
        "170",
        "34",
        "59",
    ]
)


US_ME_COMPLETE_EARLY_TERMINATION_FORM_RECORD_VIEW_NAME = (
    "us_me_complete_early_termination_record"
)

US_ME_COMPLETE_EARLY_TERMINATION_RECORD_DESCRIPTION = """
    Queries information needed to display Early Termination eligibility in ME
    """

US_ME_COMPLETE_EARLY_TERMINATION_RECORD_QUERY_TEMPLATE = f"""

WITH current_supervision_pop_cte AS (
    -- Keep individuals on probation today
    SELECT pei.external_id,
        tes.state_code,
        tes.reasons,
        tes.ineligible_criteria,
        tes.is_eligible,
    FROM `{{project_id}}.{{task_eligibility_dataset}}.early_termination_from_probation_materialized` tes
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        USING(person_id)
    WHERE 
      CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND 
                                         {nonnull_end_date_exclusive_clause('tes.end_date')}
      AND tes.state_code = 'US_ME'
),

case_notes_cte AS (
-- Get together all case_notes

    -- Supervision conditions
    SELECT
        crt.Cis_400_Cis_100_Client_Id AS external_id,
        "Special Supervision Conditions" AS criteria,
        CONCAT(ct.E_Condition_Type_Desc, IF(c.Completed_Ind = 'Y', ' - Completed', '')) AS note_title,
        c.General_Comments_Tx AS note_body,
        SAFE_CAST(LEFT(c.Due_Date, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_403_CONDITION_latest` c
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_4030_CONDITION_TYPE_latest` ct
        ON c.Cis_4030_Condition_Type_Cd = ct.Condition_Type_Cd 
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_409_CRT_ORDER_CNDTION_HDR_latest` crt_cn
        USING(Cis_408_Condition_Hdr_Id)
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_401_CRT_ORDER_HDR_latest` crt
        ON crt_cn.Cis_401_Court_Order_Id = crt.Court_Order_Id
    WHERE Logical_Delete_Ind != 'Y'
        # Exclude completed sentences
        AND crt.Cis_4010_Crt_Order_Status_Cd != '3' 
        # We are not surfacing standard supervision conditions, since they apply to all
        #   probationees.
        AND CAST(ct.Condition_Type_Cd AS INT64) NOT IN ({_STANDARD_PAROLE_CONDITIONS})

    UNION ALL

    -- Technical violations
    {cis_408_violations_notes_cte(violation_type='TECHNICAL', 
                                  violation_type_for_criteria='Technical Violations (in the past 6 months)',
                                  time_interval = 6,
                                  date_part = 'MONTH')}

    UNION ALL

    -- Misdemeanors
    {cis_408_violations_notes_cte(violation_type='MISDEMEANOR', 
                                  violation_type_for_criteria='Misdemeanors (in the past 6 months)',
                                  time_interval = 6,
                                  date_part = 'MONTH')}

    UNION ALL

    -- Felonies
    {cis_408_violations_notes_cte(violation_type='FELONY', 
                                  violation_type_for_criteria='Felonies (in the past 6 months)',
                                  time_interval = 6,
                                  date_part = 'MONTH')}

                                  
    UNION ALL

    -- Pending violations
    SELECT 
        v.Cis_100_Client_Id AS external_id,
        'Pending Violations (in the past 3 years)' AS criteria,
        CONCAT(sc.Sent_Calc_Sys_Desc, ' - ', sc2.Sent_Calc_Sys_Desc) AS note_title,
        CONCAT(v.Violation_Descr_Tx, 
                IF(v.Finding_Notes_Tx IS NOT NULL,
                    ' - ',
                    ''), 
                COALESCE(v.Finding_Notes_Tx, '')) AS note_body,
        SAFE_CAST(LEFT(v.Toll_Start_Date, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_480_VIOLATION_latest` v
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_4009_SENT_CALC_SYS_latest` sc
    ON v.Cis_4009_Violation_Type_Cd = sc.Sent_Calc_Sys_Cd
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_4009_SENT_CALC_SYS_latest` sc2
    ON v.Cis_4009_Toll_Violation_Cd = sc2.Sent_Calc_Sys_Cd
    WHERE v.Cis_4800_Violation_Finding_Cd IS NULL
        AND DATE_SUB(CURRENT_DATE('US/Eastern') , INTERVAL 3 YEAR) <= SAFE_CAST(LEFT(v.Toll_Start_Date, 10) AS DATE)
), 

{json_to_array_cte('current_supervision_pop_cte')}, 

eligible_and_almost_eligible AS (

    -- ELIGIBLE
    SELECT * EXCEPT(is_eligible)
    FROM current_supervision_pop_cte
    WHERE is_eligible

    UNION ALL 

    -- ALMOST ELIGIBLE (<6mo remaining before half time)
    {x_time_away_from_eligibility(time_interval= 6, date_part= 'MONTH',
        criteria_name= 'SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE')}
),

array_case_notes_cte AS (
  SELECT 
      external_id,
      -- Group all notes into an array within a JSON
      TO_JSON(ARRAY_AGG( STRUCT(note_title, note_body, event_date, criteria))) AS case_notes,
  FROM eligible_and_almost_eligible
  -- left join after removing repeated notes
  LEFT JOIN case_notes_cte
    USING(external_id)
  WHERE criteria IS NOT NULL
  GROUP BY 1
)

SELECT 
    external_id,
    state_code,
    reasons,
    ineligible_criteria,
    case_notes,
FROM eligible_and_almost_eligible
LEFT JOIN array_case_notes_cte
  USING(external_id)
"""

US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_COMPLETE_EARLY_TERMINATION_FORM_RECORD_VIEW_NAME,
    view_query_template=US_ME_COMPLETE_EARLY_TERMINATION_RECORD_QUERY_TEMPLATE,
    description=US_ME_COMPLETE_EARLY_TERMINATION_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ME
    ),
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER.build_and_print()
