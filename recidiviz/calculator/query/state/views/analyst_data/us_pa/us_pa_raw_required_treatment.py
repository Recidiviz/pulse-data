# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""List of people in PA actively on supervision along with their required treatment and treatment referral/completion status"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_RAW_REQUIRED_TREATMENT_VIEW_NAME = "us_pa_raw_required_treatment"

US_PA_RAW_DATASET = "us_pa_raw_data_up_to_date_views"

US_PA_RAW_REQUIRED_TREATMENT_VIEW_DESCRIPTION = """List of people in PA actively on supervision along with their required treatment and treatment referral/completion status"""

US_PA_RAW_REQUIRED_TREATMENT_QUERY_TEMPLATE = """
    -- This CTE joins 3 tables: dbo_BoardAction, dbo_ConditionCode, and dbo_ConditionCodeDescription
    -- dbo_BoardAction gives us the date of a board action where supervision special conditions are determined
    -- dbo_ConditionCode and dbo_ConditionCodeDescription give us what those conditions are
    
    WITH board_action_conditions AS (
      #TODO(#10038): Ingest raw tables and switch to referencing corresponding state entities
      SELECT
        *,
        1 AS treatment_required
      FROM `{project_id}.{raw_dataset}.dbo_BoardAction_latest` bdaction
      -- There can be many board actions that aren't related to special conditions - we only keep the ones that are
      JOIN `{project_id}.{raw_dataset}.dbo_ConditionCode_latest` condition
        USING(ParoleNumber, ParoleCountID, BdActionID)
      -- dbo_ConditionCodeDescription breaks out sentences across a given person, board action, and condition code (identified by
      -- CndDescriptionID). We first concatenate the strings within a given ConditionCodeID so that we can do pattern matching 
      -- on the full string
      JOIN (
        SELECT
          ParoleNumber,
          ParoleCountID,
          BdActionID,
          ConditionCodeID,
          STRING_AGG(ConditionDescription ORDER BY CndDescriptionID) AS concatenated_string
        FROM `{project_id}.{raw_dataset}.dbo_ConditionCodeDescription_latest`
        GROUP BY 1, 2, 3, 4 
        )
        USING(ParoleNumber, ParoleCountID, BdActionID, ConditionCodeID)
      -- According to the PBPP Data Dictionary, the condition codes DAM and MDAM should correspond to treatment being a special condition,
      -- but we do an OR with pattern matching here to capture all cases
      #TODO(#10019): Update Gitbook documentation with information from data dictionary
      WHERE concatenated_string LIKE '%TREATMENT IS A SPECIAL CONDITION%'
          OR condition.CndConditionCode IN ('DAM','MDAM')
    ),
    treatment AS (
      SELECT
        ParoleNumber,
        TreatmentID,
        TrtClassCode,
        DATE(CAST(TrtStartDateYear AS INT),CAST(TrtStartDateMonth AS INT),CAST(TrtStartDateDay AS INT)) AS treatment_start_date,
        DATE(CAST(TrtEndDateYear AS INT),CAST(TrtEndDateMonth AS INT),CAST(TrtEndDateDay AS INT)) AS treatment_end_date,
      FROM `{project_id}.{raw_dataset}.dbo_Treatment_latest`
      -- According to the PBPP Data Dictionary, these two program codes correspond to treatment referrals, while other program codes
      -- don't appear to be related to treatment
      WHERE TrtProgramCode IN ('REF','REFO') 
    ),
    df AS (
      SELECT
        df.*,
        sessions_supervision.start_date AS latest_supervision_start_date,
        sessions_incarceration.start_date AS latest_incarceration_start_date,
      FROM `{project_id}.{dataflow_dataset}.most_recent_single_day_supervision_population_metrics_materialized` df
      -- The next two subqueries use compartment_level_0 sessions to get the latest supervision and latest incarceration start dates
      LEFT JOIN (
        SELECT *
        FROM `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized`
        WHERE compartment_level_0 = 'SUPERVISION' 
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY session_id_end DESC) = 1 
        ) sessions_supervision
        ON df.person_id = sessions_supervision.person_id
        AND df.date_of_supervision >= sessions_supervision.start_date
      LEFT JOIN (
        SELECT *
        FROM `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized`
        WHERE compartment_level_0 = 'INCARCERATION' 
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY session_id_end DESC) = 1 
        ) sessions_incarceration
        ON df.person_id = sessions_incarceration.person_id
        AND df.date_of_supervision >= sessions_incarceration.start_date
      WHERE df.state_code = 'US_PA' 
    ),
    df_and_board_actions AS (
      SELECT 
        person_id,
        person_external_id,
        supervision_type,
        case_type,
        supervising_officer_external_id,
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id,
        date_of_supervision,
        supervision_level,
        df.latest_incarceration_start_date,
        df.latest_supervision_start_date,
        state_code,
        -- Take the first board action date when there are multiple.
        -- This is so when we join on treatment referrals by start date, we're keeping all referrals since the first board action date
        -- mentioning required treatment
        MIN(DATE(CAST(BdActDateYear AS INT),CAST(BdActDateMonth AS INT),CAST(BdActDateDay AS INT))) AS board_action_date,
        MAX(treatment_required) as treatment_required,
        STRING_AGG(concatenated_string) AS concatenated_string,
      FROM df
      -- According to online documentation here (https://www.parole.pa.gov/Parole%20101/Questions%20about/Pages/Parole-Interview-Date.aspx)
      -- prepartion for parole starts approximately 8 months before an individual's minimum sentence date. So we only look at board actions
      -- 8 months before the latest supervision start date or later
      LEFT JOIN board_action_conditions
          ON board_action_conditions.ParoleNumber = df.person_external_id
          AND DATE(CAST(BdActDateYear AS INT),CAST(BdActDateMonth AS INT),CAST(BdActDateDay AS INT)) >= DATE_SUB(df.latest_supervision_start_date, INTERVAL 8 month)
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    ),
    df_and_board_actions_dedup AS (
        SELECT *,
            -- Generate flags for types of treatment found in the concatenated string, which has been aggregated across all board actions that joined
            CASE WHEN UPPER(concatenated_string) LIKE '%DRUG%' or UPPER(concatenated_string) LIKE '%ALCOHOL%' OR UPPER(concatenated_string) LIKE '%SUBSTANCE%' OR UPPER(concatenated_string) LIKE '%NARCOTICS%' THEN 'ALCOHOL/DRUG' END AS required_1,
            CASE WHEN UPPER(concatenated_string) LIKE '%MENTAL%' THEN 'MENTAL HEALTH' END AS required_2,
            CASE WHEN UPPER(concatenated_string) LIKE '%ASSISTANCE%' THEN 'ASSISTANCE' END AS required_3,
            CASE WHEN UPPER(concatenated_string) LIKE '%BCC%' THEN 'BCC REFERRALS' END AS required_4,
            CASE WHEN UPPER(concatenated_string) LIKE '%COMMUNITY%' THEN 'COMMUNITY SERVICE PROGRAMS' END AS required_5,
            CASE WHEN UPPER(concatenated_string) LIKE '%DAY REPORTING%' THEN 'DAY REPORTING' END AS required_6,
            CASE WHEN UPPER(concatenated_string) LIKE '%DOMESTIC%' OR UPPER(concatenated_string) LIKE '%BATTERER%' THEN 'DOMESTIC RELATIONS' END AS required_7,
            CASE WHEN UPPER(concatenated_string) LIKE '%EDUCATION%' THEN 'EDUCATION' END AS required_8,
            CASE WHEN UPPER(concatenated_string) LIKE '%EMPLOYMENT%' THEN 'EMPLOYMENT SERVICES' END AS required_9,
            CASE WHEN UPPER(concatenated_string) LIKE '%GAMBL%' THEN 'GAMBLING' END AS required_10,
            CASE WHEN UPPER(concatenated_string) LIKE '%HALFWAY%' THEN 'HALFWAY HOUSES' END AS required_11,
            CASE WHEN UPPER(concatenated_string) LIKE '%HEALTH SERVICE%' THEN 'HEALTH SERVICES' END AS required_12,
            CASE WHEN UPPER(concatenated_string) LIKE '%HOUSING%' THEN 'HOUSING' END AS required_13,
            CASE WHEN UPPER(concatenated_string) LIKE '%HUMAN RELATION%' THEN 'HUMAN RELATIONS' END AS required_14,
            CASE WHEN UPPER(concatenated_string) LIKE '%LEGAL PROGRAM%' THEN 'LEGAL PROGRAMS' END AS required_15,
            CASE WHEN UPPER(concatenated_string) LIKE '%PAROLE VIOLATION%' THEN 'PAROLE VIOLATION' END AS required_16,
            CASE WHEN UPPER(concatenated_string) LIKE '%PVCCC%' THEN 'PVCCC' END AS required_17,
            CASE WHEN UPPER(concatenated_string) LIKE '%VETERAN%' THEN 'VETERANS PROGRAM' END AS required_18,
            CASE WHEN UPPER(concatenated_string) LIKE '%SEX%' THEN 'SEX OFFENSE' END AS required_19,
            CASE WHEN UPPER(concatenated_string) LIKE '%VIOLENCE%' THEN 'VIOLENCE PREVENTION' END AS required_20,
            CASE WHEN UPPER(concatenated_string) LIKE '%ANGER%' THEN 'ANGER MANAGEMENT' END AS required_21,
        FROM df_and_board_actions 
        WHERE TRUE
        -- Deal with very small number of duplicates where people have different supervising officer/location
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY supervising_officer_external_id, supervision_type, CASE WHEN supervision_level = 'INTERNAL_UNKNOWN' THEN 1 ELSE 0 END, level_1_supervision_location_external_id, level_2_supervision_location_external_id) = 1
    ), 
    -- Unpivot all the treatment type flags so we can put them into a single array
    unpivoted AS (
        SELECT *
        FROM df_and_board_actions_dedup 
        UNPIVOT(treatment FOR requirement in (required_1,required_2,required_3,required_4,required_5,required_6,required_7,required_8,required_9,required_10,required_11,required_12,required_13,required_14,required_15,required_16,required_17,required_18,required_19,required_20,required_21))
    ), 
    all_treatments AS (
        -- Aggregate all the required treatments
        SELECT person_id,  ARRAY_AGG(treatment) as all_treatment_required
        FROM unpivoted 
        GROUP BY 1
    ), 
    all_treatments_with_other AS (
        SELECT * EXCEPT(all_treatment_required,required_1,required_2,required_3,required_4,required_5,required_6,required_7,required_8,required_9,required_10,required_11,required_12,required_13,required_14,required_15,required_16,required_17,required_18,required_19,required_20,required_21),
            CASE WHEN all_treatment_required IS NOT NULL THEN all_treatment_required
                 -- When we unpivot, we drop rows that have nulls across all treatment flags. This statement marks those as
                 -- "OTHER" if they are not missing a board action date
                 -- This includes a very small number of rows, usually where the string just says "OUT-PATIENT TREATMENT" without specifying a type
                 WHEN board_action_date IS NOT NULL THEN ["OTHER"]
                 END AS all_treatment_required 
        FROM df_and_board_actions_dedup 
        LEFT JOIN all_treatments 
            USING(person_id)
    )
    -- Finally, join on treatment data
    SELECT
      df_and_board_actions.* EXCEPT(concatenated_string ),
      disch.projected_end_date,
      agent.given_names AS supervising_officer_first_name,
      agent.surname AS supervising_officer_last_name,
      JSON_VALUE(person.full_name, '$.given_names') AS first_name,
      JSON_VALUE(person.full_name, '$.surname') AS last_name,
      level_1_supervision_location_name,
      level_2_supervision_location_name,
      TrtClassCode AS treatment_class_code,
      treatment.treatment_start_date,
      treatment.treatment_end_date,
      IF(LENGTH(TrtClassCode)=5,SUBSTR(TrtClassCode,0,3),SUBSTR(TrtClassCode,0,2)) AS classification_code,
      classification_description,
      concatenated_string,
    FROM all_treatments_with_other df_and_board_actions
      -- treatment data is unique on person_id and treatmentID so this join introduces duplicates for different bouts of treatment / referrals
    LEFT JOIN treatment
        ON df_and_board_actions.person_external_id = treatment.ParoleNumber
        AND treatment.treatment_start_date >= board_action_date
    LEFT JOIN `{project_id}.{analyst_dataset}.us_pa_raw_treatment_classification_codes`
        ON IF(LENGTH(TrtClassCode)=5,SUBSTR(TrtClassCode,0,3),SUBSTR(TrtClassCode,0,2)) = classification_code
    LEFT JOIN `{project_id}.reference_views.supervision_location_ids_to_names` ref
        ON df_and_board_actions.level_1_supervision_location_external_id = ref.level_1_supervision_location_external_id
      AND df_and_board_actions.level_2_supervision_location_external_id = ref.level_2_supervision_location_external_id
      AND df_and_board_actions.state_code = ref.state_code      
    LEFT JOIN 
        ( SELECT DISTINCT * EXCEPT(agent_id, agent_type) FROM `{project_id}.reference_views.augmented_agent_info`) agent
        ON df_and_board_actions.supervising_officer_external_id = agent.external_id
        AND df_and_board_actions.state_code = agent.state_code
    INNER JOIN `{project_id}.{base_dataset}.state_person` person
        ON df_and_board_actions.person_id = person.person_id
    LEFT JOIN `{project_id}.{analyst_dataset}.projected_discharges_materialized` disch
        ON df_and_board_actions.person_id = disch.person_id

    """

US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_PA_RAW_REQUIRED_TREATMENT_VIEW_NAME,
    view_query_template=US_PA_RAW_REQUIRED_TREATMENT_QUERY_TEMPLATE,
    description=US_PA_RAW_REQUIRED_TREATMENT_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    raw_dataset=US_PA_RAW_DATASET,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER.build_and_print()
