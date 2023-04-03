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
"""Queries information needed to fill out transfer to SCCP form in ME
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
    one_criteria_away_from_eligibility,
    x_time_away_from_eligibility,
)
from recidiviz.task_eligibility.utils.raw_table_import import cis_204_notes_cte
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_NAME = "us_me_complete_transfer_to_sccp_form_record"

US_ME_TRANSFER_TO_SCCP_RECORD_DESCRIPTION = """
    Query for relevant information to fill out the transfer to SCCP forms in ME
    """

_SCCP_NOTE_TX_REGEX = "|".join(
    ["SCCP", "COMMUNITY CONFINEMENT", "HOME CONFINEMENT", "SCC"]
)

_PROGRAM_ENROLLMENT_NOTE_TX_REGEX = "|".join(
    [
        "COMPLET[A-Z]*",
        "CERTIFICAT[A-Z]",
        "PARTICIPATED",
        "ATTENDED",
        "EDUC[A-Z]*",
        "CAT ",
        "CRIMINAL ADDICTIVE THINKING",
        "ANGER MANAGEMENT",
        "NEW FREEDOM",
        "T4C",
        "THINKING FOR A CHANGE",
        "SAFE",
        "STOPPING ABUSE FOR EVERYONE",
        "CBI-IPV",
        "SUD ",
        "HISET",
        "PROBLEM SEXUAL BEHAVIOR",
    ]
)


US_ME_TRANSFER_TO_SCCP_RECORD_QUERY_TEMPLATE = f"""

WITH current_incarceration_pop_cte AS (
    -- Keep incarcerated individuals today
    SELECT pei.external_id,
        tes.state_code,
        tes.reasons,
        tes.ineligible_criteria,
        tes.is_eligible,
    FROM `{{project_id}}.{{task_eligibility_dataset}}.transfer_to_sccp_materialized` tes
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        USING(person_id)
    WHERE 
      CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND 
                                         {nonnull_end_date_exclusive_clause('tes.end_date')}
      AND tes.state_code = 'US_ME'
),

case_notes_cte AS (
-- Get together all case_notes

    -- Program enrollment data as notes
    SELECT 
        mp.CIS_100_CLIENT_ID AS external_id,
        "Program Enrollment" AS criteria,
        CONCAT(st.E_STAT_TYPE_DESC ,' - ', pr.NAME_TX) AS note_title,
        ps.Comments_Tx AS note_body,
        -- TODO(#17587) remove LEFT once the YAML file is updated
        SAFE_CAST(LEFT(mp.MODIFIED_ON_DATE, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_425_MAIN_PROG_latest` mp
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_420_PROGRAMS_latest` pr
        ON mp.CIS_420_PROGRAM_ID = pr.PROGRAM_ID
    -- Comments_Tx/Note_body could be NULL, which happens when the record does not contain free text 
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_426_PROG_STATUS_latest`  ps
        ON mp.ENROLL_ID = ps.Cis_425_Enroll_Id
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_9900_STATUS_TYPE_latest` st
        ON ps.Cis_9900_Stat_Type_Cd = st.STAT_TYPE_CD
    WHERE pr.NAME_TX IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY mp.ENROLL_ID ORDER BY Effct_Datetime DESC) = 1

    UNION ALL
  
    -- SCCP-related notes
    {cis_204_notes_cte("Notes: SCCP")}
    # While fuzzy matching is set up
    WHERE 
        REGEXP_CONTAINS(UPPER(n.Short_Note_Tx), r'{_SCCP_NOTE_TX_REGEX}') 
        OR REGEXP_CONTAINS(UPPER(n.Note_Tx), r'{_SCCP_NOTE_TX_REGEX}')
    GROUP BY 1,2,3,4,5   

    UNION ALL

    -- Program-related notes
    {cis_204_notes_cte("Notes: Program Enrollment")}
    WHERE ncd.Note_Type_Cd = '2'
        AND cncd.Contact_Mode_Cd = '20'
        AND (n.Short_Note_Tx IS NOT NULL OR n.Note_Tx IS NOT NULL)
        AND (REGEXP_CONTAINS(UPPER(n.Short_Note_Tx), r'{_PROGRAM_ENROLLMENT_NOTE_TX_REGEX}')
        OR REGEXP_CONTAINS(UPPER(n.Note_Tx), r'{_PROGRAM_ENROLLMENT_NOTE_TX_REGEX}'))
    GROUP BY 1,2,3,4,5    

    UNION ALL 

    -- SCCP Application Investigations
    SELECT 
        Cis_100_Client_Id AS external_id,
        "SCCP Application Investigation" AS criteria,
        IF(Complete_Date IS NULL,
            'Resolution not available yet',
            CONCAT("Resolution date: ", 
                    DATE(SAFE_CAST(Complete_Date AS DATETIME))))
        AS note_title,
        Notes_Tx AS note_body,
        -- TODO(#17587) remove LEFT once the YAML file is updated
        DATE(SAFE_CAST(Request_Date AS DATETIME)) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_130_INVESTIGATION_latest` i
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_1301_INVESTIGATION_latest` ic
        ON i.Cis_1301_Type_Cd = ic.Investigation_Cd
    -- Keep only SCCP related investigations
    WHERE Investigation_Cd = '11' 
        AND Cis_100_Client_Id IS NOT NULL 

    UNION ALL 

    -- Case Plan Goals
    SELECT  
        Cis_200_Cis_100_Client_Id AS external_id,
        "Case Plan Goals" AS criteria,
        CONCAT(Domain_Goal_Desc,' - ', Goal_Status_Desc) AS note_title,
        IF(E_Goal_Type_Desc = 'Other',
            CONCAT(E_Goal_Type_Desc, " - " ,Other),
            E_Goal_Type_Desc)
        AS note_body,
        DATE(SAFE.PARSE_DATETIME("%m/%d/%Y %I:%M:%S %p", Open_Date)) AS event_date,
    FROM (SELECT 
                *
            FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_201_GOALS_latest` gl
            INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2012_GOAL_STATUS_TYPE_latest` gs
                ON gl.Cis_2012_Goal_Status_Cd = gs.Goal_Status_Cd
            INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2010_GOAL_TYPE_latest` gt
                ON gl.Cis_2010_Goal_Type_Cd = gt.Goal_Type_Cd
            INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2011_DOMAIN_GOAL_TYPE_latest` dg
                ON gl.Cis_2011_Dmn_Goal_Cd = dg.Domain_Goal_Cd
            WHERE gs.Goal_Status_Cd IN ('1','2')) 
), 

{json_to_array_cte('current_incarceration_pop_cte')}, 

eligible_and_almost_eligible AS (

    -- ELIGIBLE
    SELECT * EXCEPT(is_eligible)
    FROM current_incarceration_pop_cte
    WHERE is_eligible

    UNION ALL 

    -- ALMOST ELIGIBLE (<6mo remaining before 30/24mo)
    {x_time_away_from_eligibility(time_interval= 6, date_part= 'MONTH',
        criteria_name= 'US_ME_X_MONTHS_REMAINING_ON_SENTENCE')}

    UNION ALL

    -- ALMOST ELIGIBLE (<6mo remaining before 1/2 or 2/3 of sentence served)
    {x_time_away_from_eligibility(time_interval= 6, date_part= 'MONTH',
        criteria_name= 'US_ME_SERVED_X_PORTION_OF_SENTENCE')}

    UNION ALL

    -- ALMOST ELIGIBLE (one discipline away)
    {one_criteria_away_from_eligibility('US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS')}
),

array_case_notes_cte AS (
  SELECT 
      external_id,
      -- Group all notes into an array within a JSON
      TO_JSON(ARRAY_AGG( STRUCT(note_title, note_body, event_date, criteria))) AS case_notes,
  FROM eligible_and_almost_eligible
  -- left join after removing repeated notes
  LEFT JOIN (SELECT 
                  *
             FROM case_notes_cte
             QUALIFY ROW_NUMBER() OVER(PARTITION BY external_id, note_title, note_body 
                                     ORDER BY IF(criteria = "SCCP notes", 0, 1), 
                                            criteria DESC) = 1) 
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

US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_NAME,
    view_query_template=US_ME_TRANSFER_TO_SCCP_RECORD_QUERY_TEMPLATE,
    description=US_ME_TRANSFER_TO_SCCP_RECORD_DESCRIPTION,
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
        US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_BUILDER.build_and_print()
