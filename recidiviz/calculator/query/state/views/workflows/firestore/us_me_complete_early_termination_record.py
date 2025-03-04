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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    current_snooze,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    cis_204_notes_cte,
    cis_408_violations_notes_cte,
    cis_425_program_enrollment_notes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_PROGRAM_ENROLLMENT_WHERE_CLAUSE = """
        AND st.E_STAT_TYPE_DESC = 'Completed Successfully'
        AND ss.start_date <= SAFE_CAST(LEFT(mp.MODIFIED_ON_DATE, 10) AS DATE)
"""
_PROGRAM_ENROLLMENT_ADDITIONAL_JOIN = f"""
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        ON peid.external_id = mp.CIS_100_CLIENT_ID
            AND peid.state_code = 'US_ME'
            AND peid.id_type = 'US_ME_DOC'
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.system_sessions_materialized` ss
        ON peid.person_id = ss.person_id
            AND CURRENT_DATE('US/Eastern') BETWEEN ss.start_date AND {nonnull_end_date_clause('ss.end_date_exclusive')}
"""

_ET_NOTE_TX_REGEX = "|".join(["EARLY TERMINATION", "EARLY TERM", "EARLY RELEASE"])

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
    -- Keep only the most current span per probation client
{join_current_task_eligibility_spans_with_external_id(
    state_code= "'US_ME'",
    tes_task_query_view = 'early_termination_from_probation_request_materialized',
    id_type = "'US_ME_DOC'",
    eligible_and_almost_eligible_only=True,
)}
),

case_notes_cte AS (
-- Get together all case_notes

    -- Supervision conditions
    SELECT
        DISTINCT
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

    -- ET-related notes
    {cis_204_notes_cte("Notes: Early Termination")}
    WHERE 
        REGEXP_CONTAINS(UPPER(n.Short_Note_Tx), r'{_ET_NOTE_TX_REGEX}') 
        OR REGEXP_CONTAINS(UPPER(n.Note_Tx), r'{_ET_NOTE_TX_REGEX}')
    GROUP BY 1,2,3,4,5   

    UNION ALL

    -- Technical violations
    {cis_408_violations_notes_cte(violation_type='TECHNICAL', 
                                  violation_type_for_criteria='Technical Violations (in the past 6 months)',
                                  note_title = " 'Violation Found' ",
                                  time_interval = 6,
                                  date_part = 'MONTH')}

    UNION ALL

    -- Graduated sanctions
    {cis_408_violations_notes_cte(violation_type='ANY', 
                                  violation_type_for_criteria='Graduated Sanctions (in the past year)',
                                  note_title = "Sent_Calc_Sys_Desc",
                                  time_interval = 1,
                                  date_part = 'YEAR')}

    UNION ALL 
    
    -- Misdemeanors
    {cis_408_violations_notes_cte(violation_type='MISDEMEANOR', 
                                  violation_type_for_criteria='Misdemeanors (6 months within Toll End Date)',
                                  note_title = " 'Violation Found' ",
                                  time_interval = 6,
                                  date_part = 'MONTH',
                                  violation_date = "v.Toll_End_Date")}

    UNION ALL

    -- Felonies
    {cis_408_violations_notes_cte(violation_type='FELONY', 
                                  violation_type_for_criteria='Felonies (6 months within Toll End Date)',
                                  note_title = " 'Violation Found' ",
                                  time_interval = 6,
                                  date_part = 'MONTH',
                                  violation_date = "v.Toll_End_Date")}
                         
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

    UNION ALL

    #TODO(#20608): get descriptions from these programs and add them to the note so POs have
        # more context

    -- Program enrollment notes: starting from the incarceration period that preceded
        -- the current supervision period
    {cis_425_program_enrollment_notes(
        where_clause= _PROGRAM_ENROLLMENT_WHERE_CLAUSE, 
        additional_joins=_PROGRAM_ENROLLMENT_ADDITIONAL_JOIN,
        criteria="'Completed Programs'",
        note_title = "pr.NAME_TX",
        note_body = 'pr.DESCRIPTION_TX',)}
),

array_case_notes_cte AS (
  {array_agg_case_notes_by_external_id(from_cte="current_supervision_pop_cte")}
),

-- Get most recent early termination snoozes per person

snooze_cte AS (
{current_snooze(
    state_code= "US_ME",
    opportunity_type= "usMeEarlyTermination", 
)}
),

add_snooze_info AS (
    SELECT *
    FROM array_case_notes_cte
    LEFT JOIN snooze_cte USING(external_id)
)

{opportunity_query_final_select_with_case_notes(
    from_cte="current_supervision_pop_cte", 
    left_join_cte="add_snooze_info", 
    additional_columns="metadata_denial"
)}
"""

US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_COMPLETE_EARLY_TERMINATION_FORM_RECORD_VIEW_NAME,
    view_query_template=US_ME_COMPLETE_EARLY_TERMINATION_RECORD_QUERY_TEMPLATE,
    description=US_ME_COMPLETE_EARLY_TERMINATION_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    sessions_dataset=SESSIONS_DATASET,
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
