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
"""CTEs used across multiple states' client record queries."""

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_exclusive_clause,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    workflows_state_specific_supervision_level,
)
from recidiviz.calculator.query.state.views.workflows.us_id.shared_ctes import (
    us_id_latest_phone_number,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    us_tn_fines_fees_info,
    us_tn_supervision_type,
)
from recidiviz.task_eligibility.utils.preprocessed_views_query_fragments import (
    compartment_level_1_super_sessions_without_me_sccp,
)

_CLIENT_RECORD_SUPERVISION_CTE = f"""
    -- TODO(#22427) Once PersonParole has badge numbers, delete this and just use the table directly 
    ca_person_parole_imputed_badges AS (
        SELECT
            OffenderId, 
            COALESCE(pp.BadgeNumber, ap.BadgeNumber) as BadgeNumber,
            EarnedDischargeDate, ControllingDischargeDate
        FROM `{{project_id}}.{{us_ca_raw_data_up_to_date_dataset}}.PersonParole_latest` pp
        LEFT JOIN (
            SELECT
                ParoleRegion,
                ParoleDistrict,
                ParoleUnit,
                -- Change delimiter, trim middle initial
                REGEXP_REPLACE(REPLACE(ParoleAgentName,'|', ','), ' [A-Z]$', '') as ParoleAgentName,
                MIN(BadgeNumber) as BadgeNumber
            FROM `{{project_id}}.{{us_ca_raw_data_up_to_date_dataset}}.AgentParole_latest`
            WHERE BadgeNumber is not null
            GROUP BY ParoleRegion, ParoleDistrict, ParoleUnit, ParoleAgentName
        ) ap USING(ParoleRegion, ParoleDistrict, ParoleUnit, ParoleAgentName)
    ),

    supervision_cases AS (
        SELECT
          sessions.person_id,
          sessions.state_code,
          pei.person_external_id,
          COALESCE(tn_supervision_type.supervision_type, sessions.compartment_level_2) AS supervision_type,
            -- Pull the officer ID from compartment_sessions instead of supervision_officer_sessions
            -- to make sure we choose the officer that aligns with other compartment session attributes.
          #   There are officers with more than one legitimate external id. We are merging these ids and
          #   so must move all clients to the merged id.
          COALESCE(ca_pp.BadgeNumber, ids.external_id_mapped, sessions.supervising_officer_external_id_end) AS officer_id,
          sessions.supervision_district_name_end AS district,
          IFNULL(
            projected_end.projected_completion_date_max,
            PARSE_DATE("%F", SUBSTR(COALESCE(
                LEAST(ca_pp.EarnedDischargeDate, ca_pp.ControllingDischargeDate),
                ca_pp.EarnedDischargeDate,
                ca_pp.ControllingDischargeDate
            ), 0,10))
          ) AS expiration_date
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
        LEFT JOIN {{project_id}}.{{static_reference_tables_dataset}}.agent_multiple_ids_map ids
            ON sessions.supervising_officer_external_id_end = ids.external_id_to_map AND sessions.state_code = ids.state_code 
        INNER JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
            ON sessions.person_id = pei.person_id
            AND sessions.state_code = pei.state_code
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` projected_end
            ON sessions.state_code = projected_end.state_code
            AND sessions.person_id = projected_end.person_id
            AND CURRENT_DATE('US/Eastern')
                BETWEEN projected_end.start_date
                    AND {nonnull_end_date_exclusive_clause('projected_end.end_date')}
        -- Remove clients who previously had an active officer, but no longer do.
        INNER JOIN (
            SELECT DISTINCT
                state_code,
                person_id,
                -- TODO(#19840): Remove the end_date column for this subquery
                COALESCE(end_date, "9999-09-09") AS end_date,
            FROM `{{project_id}}.{{sessions_dataset}}.supervision_officer_sessions_materialized`
            -- TODO(#19840): Remove US_MI exclusion
            WHERE (state_code = "US_MI" OR end_date IS NULL)
                AND (state_code = "US_CA" OR supervising_officer_external_id IS NOT NULL)
        ) active_officer
            ON sessions.state_code = active_officer.state_code
            AND sessions.person_id = active_officer.person_id
        LEFT JOIN ca_person_parole_imputed_badges ca_pp
            ON pei.person_external_id = ca_pp.OffenderId
            AND sessions.state_code = "US_CA"
        LEFT JOIN (
            {us_tn_supervision_type()}
        ) tn_supervision_type
            ON sessions.person_id = tn_supervision_type.person_id
        WHERE sessions.state_code IN ({{workflows_supervision_states}})
          AND sessions.compartment_level_1 = "SUPERVISION"
          AND sessions.end_date IS NULL
          AND (sessions.state_code = "US_CA" OR sessions.supervising_officer_external_id_end IS NOT NULL)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY person_external_id,
            -- TODO(#19840): Remove end_date ordering
            active_officer.end_date DESC
        ) = 1
    ),
    """


_CLIENT_RECORD_SUPERVISION_LEVEL_CTE = f"""
    supervision_level_start AS (
        # This CTE selects the most recent supervision level for each person with an active supervision period,
        # prioritizing the highest level in cases where one person is currently assigned to multiple levels
        SELECT
            sl.person_id,
            sl.start_date as supervision_level_start,  
            {workflows_state_specific_supervision_level()} AS supervision_level,
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized` sl
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized` dataflow
            ON dataflow.person_id = sl.person_id
            AND dataflow.dataflow_session_id = sl.dataflow_session_id_start,
            UNNEST(session_attributes) as session_attributes
        WHERE sl.state_code IN ({{workflows_supervision_states}})
        AND sl.end_date IS NULL
        # This row partitions by person_id and supervision start_date to account for when we see multiple 
        # supervision_level_raw_text values in dataflow_sessions for the most_recent_active supervision level in 
        # supervision level sessions, and prioritizes the DIVERSION value 
        QUALIFY ROW_NUMBER() OVER(PARTITION BY dataflow.person_id, dataflow.start_date ORDER BY session_attributes.correctional_level = 'DIVERSION' DESC)=1
    ),
    """


_CLIENT_RECORD_SUPERVISION_SUPER_SESSIONS_CTE = f"""
    supervision_super_sessions AS (
        
        WITH {compartment_level_1_super_sessions_without_me_sccp()}
        
        # This CTE has 1 row per person with an active supervision period and the start_date corresponds to 
        # the earliest start date for dual supervision periods.
        SELECT
            person_id,
            start_date
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`
        WHERE state_code IN ({{workflows_supervision_states}})
        AND end_date IS NULL
        # TODO(#20872) - Remove 'US_ME' filter once super_sessions fixed
        AND state_code != 'US_ME'
 
        UNION ALL

        #TODO(#20872) - some liberty cases are being labeled as pending_custody, which
            #is causing supervision_super_sessions to get the wrong start dates. Once this is 
            #fixed, we should be able to go back to supervision_super_sessions

        SELECT 
            person_id, 
            start_date
        FROM partitioning_compartment_l1_ss_with_sccp
        WHERE state_code = 'US_ME'
        AND end_date IS NULL
    ),
    """

_CLIENT_RECORD_DISPLAY_IDS_CTE = """
    display_ids AS (
        # In most cases, the client ID we display to users is person_external_id, but some
        # states may want to display a different ID that isn't suitable as a pei 
        SELECT
            state_code,
            person_external_id,
            CASE state_code
                WHEN "US_CA" THEN ca_pp.Cdcno
                ELSE person_external_id
                END
                AS display_id
        FROM `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` pei
        LEFT JOIN `{project_id}.{us_ca_raw_data_up_to_date_dataset}.PersonParole_latest` ca_pp
            ON person_external_id=ca_pp.OffenderId
        WHERE
            state_code IN ({workflows_supervision_states})
    ),
"""

_CLIENT_RECORD_PHONE_NUMBERS_CTE = f"""
    phone_numbers AS (
        # TODO(#14676): Pull from state_person.phone_number once hydrated
        {us_id_latest_phone_number()}
        UNION ALL
        
        # TODO(#14676): Pull from state_person.phone_number once hydrated
        SELECT
            "US_ND" AS state_code,
            pei.person_external_id, 
            doc.PHONE AS phone_number
        FROM `{{project_id}}.{{us_nd_raw_data_up_to_date_dataset}}.docstars_offenders_latest` doc
        INNER JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
        ON doc.SID = pei.person_external_id AND pei.state_code = "US_ND"

        UNION ALL

        SELECT
            sp.state_code,
            pei.person_external_id,
            sp.current_phone_number
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
        INNER JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
            USING (person_id)
        WHERE
            sp.state_code IN ({{workflows_supervision_states}})
            AND sp.state_code NOT IN ("US_ID", "US_ND")
    ),
"""

_CLIENT_RECORD_EMAIL_ADDRESSES_CTE = """
    email_addresses AS (
        SELECT
            sp.state_code,
            pei.person_external_id,
            sp.current_email_address as email_address
        FROM `{project_id}.{normalized_state_dataset}.state_person` sp
        INNER JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` pei
            USING (person_id)
        WHERE
            sp.state_code IN ({workflows_supervision_states})
    ),
"""

_CLIENT_RECORD_FINES_FEES_INFO_CTE = f"""
    {us_tn_fines_fees_info()}
"""

_CLIENT_RECORD_LAST_PAYMENT_INFO_CTE = """
    fines_fees_payment_info AS (
        SELECT 
               pp.state_code,
               pei.person_external_id,
               pp.payment_date AS last_payment_date,
               pp.payment_amount AS last_payment_amount,
        FROM `{project_id}.{analyst_dataset}.payments_preprocessed_materialized` pp
        INNER JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` pei
            USING (person_id)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY payment_date DESC) = 1
    ),
"""

_CLIENT_RECORD_SPECIAL_CONDITIONS_CTE = """
    special_conditions AS (
        SELECT 
            pei.state_code,
            pei.person_external_id,
            ARRAY_AGG(conditions IGNORE NULLS) AS special_conditions_on_current_sentences
        FROM (
            SELECT person_id, conditions
            FROM `{project_id}.{normalized_state_dataset}.state_supervision_sentence`
            WHERE COALESCE(completion_date, projected_completion_date) >= CURRENT_DATE('US/Eastern')
            
            UNION ALL
            
            SELECT person_id, conditions
            FROM `{project_id}.{normalized_state_dataset}.state_incarceration_sentence`
            WHERE COALESCE(completion_date, projected_max_release_date) >= CURRENT_DATE('US/Eastern')
        )
        INNER JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` pei
            USING (person_id)
        GROUP BY
            1,2
    ),
"""

_CLIENT_RECORD_BOARD_CONDITIONS_CTE = """
    board_conditions AS (
        /* TN BoardAction data is unique on person, hearing date, hearing type, and staff ID. For our purposes, we keep 
     conditions where "final decision" is yes, and keep all distinct parole conditions on a given person/day
     Then we keep all relevant hearings that happen in someone's latest system session, and keep all codes since then */
        WITH latest_system_start AS (
            SELECT 
                person_external_id,
                ss.state_code,
                start_date AS latest_system_session_start_date
            FROM `{project_id}.{sessions_dataset}.system_sessions_materialized` ss
            INNER JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` pei
                USING (person_id)
            WHERE ss.state_code = 'US_TN' 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY session_id_end DESC) = 1
        ),
        unpivot_board_conditions AS (
            SELECT person_external_id, hearing_date, condition, Decode AS condition_description
            FROM (
                SELECT DISTINCT OffenderID AS person_external_id, 
                       CAST(HearingDate AS DATE) AS hearing_date, 
                       ParoleCondition1, 
                       ParoleCondition2, 
                       ParoleCondition3, 
                       ParoleCondition4, 
                       ParoleCondition5
                FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.BoardAction_latest`
                WHERE FinalDecision = 'Y'
            )
            UNPIVOT(condition for c in (ParoleCondition1, ParoleCondition2, ParoleCondition3, ParoleCondition4, ParoleCondition5))
            LEFT JOIN (
                SELECT *
                FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.CodesDescription_latest`
                WHERE CodesTableID = 'TDPD030'
            ) codes 
                ON condition = codes.Code
        )
        SELECT
            bc.person_external_id,
            ls.state_code, 
            ARRAY_AGG(STRUCT(condition, condition_description)) AS board_conditions
        FROM latest_system_start ls
        INNER JOIN unpivot_board_conditions bc
            ON ls.person_external_id = bc.person_external_id
            AND bc.hearing_date >= COALESCE(ls.latest_system_session_start_date,'1900-01-01')
        GROUP BY 1,2
    ),
"""


_CLIENT_RECORD_EMPLOYMENT_INFO_CTE = f"""
    employment_info AS (
        SELECT
            state_code,
            person_id,
            ARRAY_AGG(STRUCT(employer_name AS name, employer_address AS address, start_date as start_date) IGNORE NULLS ORDER BY start_date ASC) AS current_employers
        FROM (
            SELECT DISTINCT
                state_code,
                person_id,
                sep.employer_name,
                sep.start_date,
                NULL AS end_date,
            UPPER(a.StreetNumber || ' ' || a.StreetName || ', ' || jurisdiction.LocationName || ', ' || state_ids.state_abbreviation || ' ' || a.ZipCode) AS employer_address,
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_employment_period` sep
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_EmploymentHistory_latest` emp_hist
            ON sep.external_id = emp_hist.employmentHistoryId
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_Employer_Address_latest` ra
            USING(employerId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_Address_latest` a
            USING(AddressId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_Location_latest` state
            ON a.StateId = state.locationId
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_Location_latest` jurisdiction
            ON a.CountyId = jurisdiction.locationId
            LEFT JOIN `{{project_id}}.{{static_reference_tables_dataset}}.state_ids` state_ids
            ON state_ids.state_name = state.LocationName
            WHERE {today_between_start_date_and_nullable_end_date_clause('start_date', 'end_date')}
        )
        WHERE state_code IN ("US_IX")
        GROUP BY state_code, person_id
    ),
"""


def years_and_months_template(column_name: str) -> str:
    return f"""
    IF( FLOOR(DATE_DIFF(CURRENT_DATE('US/Eastern'), {column_name}, MONTH) / 12) > 0, 
        CAST(FLOOR(DATE_DIFF(CURRENT_DATE('US/Eastern'), {column_name}, MONTH) / 12) AS string) || 
            IF(FLOOR(DATE_DIFF(CURRENT_DATE('US/Eastern'), {column_name}, MONTH) / 12) = 1, " year", " years"), 
        NULL) AS years_text,
    
    IF( MOD(DATE_DIFF(CURRENT_DATE('US/Eastern'), {column_name}, MONTH), 12) > 0, 
        CAST(MOD(DATE_DIFF(CURRENT_DATE('US/Eastern'), {column_name}, MONTH), 12) AS string) || 
            IF(MOD(DATE_DIFF(CURRENT_DATE('US/Eastern'), {column_name}, MONTH), 12) = 1, " month", " months"), 
        NULL) AS months_text,
    """


def milestone_text_template(milestone_text: str) -> str:
    return f"""
        CASE
            WHEN years_text IS NOT NULL AND months_text IS NOT NULL
            THEN CONCAT(years_text, ", ", months_text, '{milestone_text}')
            WHEN years_text IS NOT NULL
            THEN CONCAT(years_text, '{milestone_text}')
            WHEN months_text IS NOT NULL
            THEN CONCAT(months_text, '{milestone_text}')
            ELSE NULL
        END AS milestone_text,
    """


_CLIENT_RECORD_MILESTONES_CTE = f"""
    time_without_violation AS (
        SELECT
            state_code,
            person_id,
            violation_date,
            {years_and_months_template('violation_date')}
        FROM (
            SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY df.state_code, df.person_id order by violation_date desc) as rn
            FROM {{project_id}}.{{dataflow_metrics_dataset}}.most_recent_violation_with_response_metrics_materialized df
            ORDER BY person_id, rn
        )
        LEFT JOIN supervision_super_sessions ss
        USING(person_id)
        WHERE rn = 1
        AND violation_date > ss.start_date
        AND DATE_DIFF(CURRENT_DATE('US/Eastern'), violation_date, MONTH) > 0
    ),
    time_on_supervision AS (
        SELECT
            state_code,
            person_id,
            {years_and_months_template('ss.start_date')}
        FROM supervision_cases
        INNER JOIN supervision_super_sessions ss USING(person_id)
        WHERE DATE_DIFF(CURRENT_DATE('US/Eastern'), ss.start_date, MONTH) > 0
    ),
    time_with_employer AS (
        SELECT
            state_code,
            person_id,
            {years_and_months_template("current_employers[OFFSET(0)].start_date")}
        FROM employment_info
        LEFT JOIN supervision_super_sessions ss
        USING(person_id)
        WHERE DATE_DIFF(CURRENT_DATE('US/Eastern'), current_employers[OFFSET(0)].start_date, MONTH) > 0
        AND current_employers[OFFSET(0)].start_date > ss.start_date
    ),
    milestones AS (
        SELECT
            state_code,
            person_id,
            ARRAY_AGG(STRUCT(milestone_type AS type, milestone_text AS text) IGNORE NULLS ORDER BY milestone_priority ASC) AS milestones
        FROM (
            -- birthdays
            SELECT *
            FROM (
                SELECT
                    sc.state_code,
                    person_id,
                    IF(
                        EXTRACT(MONTH from sp.birthdate) = EXTRACT(MONTH from CURRENT_DATE('US/Eastern')) AND EXTRACT(DAY from sp.birthdate) <= EXTRACT(DAY from CURRENT_DATE('US/Eastern')),
                        "Birthday this month (" || FORMAT_DATE('%B %d', sp.birthdate) ||")",
                        NULL
                    ) AS milestone_text,
                    "BIRTHDAY_THIS_MONTH" as milestone_type,
                    1 AS milestone_priority,
                FROM supervision_cases sc
                LEFT JOIN {{project_id}}.{{normalized_state_dataset}}.state_person sp
                USING(state_code, person_id)
            )
            UNION ALL
            -- months without violation
            SELECT
                state_code,
                person_id,
                {milestone_text_template(" since last violation")}                
                "MONTHS_WITHOUT_VIOLATION" as milestone_type,
                2 AS milestone_priority
            FROM time_without_violation
            
            UNION ALL
            
            -- months on supervision
            SELECT
                state_code,
                person_id,
                {milestone_text_template(" on supervision")}  
                "MONTHS_ON_SUPERVISION" as milestone_type,
                3 AS milestone_priority
            FROM time_on_supervision
            
            UNION ALL
            -- months with the same employer

                SELECT
                    state_code,
                    person_id,
                    {milestone_text_template(" with the same employer")}
                    "MONTHS_WITH_CURRENT_EMPLOYER" as milestone_type,
                    4 AS milestone_priority
                FROM time_with_employer

            UNION ALL
                SELECT
                    state_code,
                    person_id,
                    "6+ months violation-free" as milestone_text,
                    "NO_VIOLATION_WITHIN_6_MONTHS" as milestone_type,
                    1 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_violation_free_6_to_8_months_materialized`
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                SELECT
                    state_code,
                    person_id,
                    "1+ year violation-free" as milestone_text,
                    "NO_VIOLATION_WITHIN_12_MONTHS" as milestone_type,
                    1 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_violation_free_12_to_14_months_materialized`
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                SELECT
                    state_code,
                    person_id,
                    "Found housing" as milestone_text,
                    "HOUSING_TYPE_IS_NOT_TRANSIENT" as milestone_type,
                    2 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_sustainable_housing_0_to_2_months_materialized`
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                SELECT
                    state_code,
                    person_id,
                    "Sustainable housing for 6+ months" as milestone_text,
                    "SUSTAINABLE_HOUSING_6_MONTHS" as milestone_type,
                    2 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_sustainable_housing_6_to_8_months_materialized`
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                SELECT
                    state_code,
                    person_id,
                    "Sustainable housing for 1+ year" as milestone_text,
                    "SUSTAINABLE_HOUSING_12_MONTHS" as milestone_type,
                    2 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_sustainable_housing_12_to_14_months_materialized`
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

        )
        WHERE state_code in ({{workflows_milestones_states}})
        AND milestone_text IS NOT NULL
        GROUP BY state_code, person_id
    ),
"""

_CLIENT_RECORD_INCLUDE_CLIENTS_CTE = """
    # For states where each transfer is a full historical transfer, we want to ensure only clients included
    # in the latest file are included in our tool
    include_clients AS (
        SELECT DISTINCT 
            person_id,
            state_code,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Offender_latest` tn_raw
        INNER JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` pei
            ON tn_raw.OffenderID = pei.person_external_id
            AND pei.state_code = "US_TN"
    
        UNION ALL
    
        SELECT DISTINCT 
            person_id,
            state_code,
        FROM `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_OFFENDER_latest` mi_raw
        INNER JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` pei
            ON mi_raw.offender_number = pei.person_external_id
            AND pei.state_code = "US_MI"
        
        UNION ALL
        
        SELECT DISTINCT
                person_id,
                state_code
        FROM `{project_id}.{normalized_state_dataset}.state_person` person
        WHERE
            person.state_code IN ({workflows_supervision_states})
            AND person.state_code NOT IN ("US_MI", "US_TN")
    ),
    """

_CLIENT_RECORD_JOIN_CLIENTS_CTE = """
    join_clients AS (
        SELECT DISTINCT
          sc.state_code,
          sc.person_id,
          sc.person_external_id,
          did.display_id,
          sp.full_name as person_name,
          sp.current_address as address,
          CAST(ph.phone_number AS INT64) AS phone_number,
          LOWER(ea.email_address) AS email_address,
          sc.supervision_type,
          sc.officer_id,
          sc.district,
          sl.supervision_level,
          sl.supervision_level_start,
          ss.start_date AS supervision_start_date,
          FIRST_VALUE(sc.expiration_date IGNORE NULLS) OVER (
            PARTITION BY sc.person_id
            ORDER BY sc.expiration_date DESC
          ) AS expiration_date,
          ff.current_balance,
          pp.last_payment_date,
          pp.last_payment_amount,
        FROM supervision_cases sc 
        INNER JOIN supervision_level_start sl USING(person_id)
        INNER JOIN supervision_super_sessions ss USING(person_id)
        INNER JOIN include_clients USING(person_id)
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person` sp USING(person_id)
        LEFT JOIN display_ids did
            ON sc.state_code = did.state_code
            AND sc.person_external_id = did.person_external_id
        LEFT JOIN phone_numbers ph
            -- join on state_code / person_external_id instead of person_id alone because state data
            -- may have multiple external_ids for a given person_id, and by this point in the
            -- query we've already decided which person_external_id we're using
            ON sc.state_code = ph.state_code
            AND sc.person_external_id = ph.person_external_id
        LEFT JOIN email_addresses ea
            ON sc.state_code = ea.state_code
            AND sc.person_external_id = ea.person_external_id
        LEFT JOIN fines_fees_balance_info ff
            ON sc.state_code = ff.state_code
            AND sc.person_external_id = ff.person_external_id
        LEFT JOIN fines_fees_payment_info pp
            ON sc.state_code = pp.state_code
            AND sc.person_external_id = pp.person_external_id
        
    ),
    """

_CLIENTS_CTE = """
    clients AS (
        # Values set to NULL are not applicable for this state
        SELECT
            person_external_id,
            display_id,
            c.state_code,
            person_name,
            officer_id,
            supervision_type,
            supervision_level,
            supervision_level_start,
            address,
            phone_number,
            email_address,
            supervision_start_date,
            expiration_date,
            district,
            ei.current_employers,
            opportunities_aggregated.all_eligible_opportunities,
            milestones,
            current_balance,
            last_payment_date,
            last_payment_amount,
            spc.special_conditions_on_current_sentences AS special_conditions,
            bc.board_conditions,
        FROM join_clients c
        LEFT JOIN opportunities_aggregated 
            USING (state_code, person_external_id)
        LEFT JOIN special_conditions spc
            USING (state_code, person_external_id)
        LEFT JOIN board_conditions bc
            USING (state_code, person_external_id)
        LEFT JOIN employment_info ei 
            USING (person_id)
        LEFT JOIN milestones mi 
            ON mi.state_code = c.state_code 
            and mi.person_id = c.person_id
        # TODO(#17138): Remove this condition if we are no longer missing person details post-ATLAS
        WHERE person_name IS NOT NULL
    )
    """


def full_client_record() -> str:
    return f"""
    {_CLIENT_RECORD_SUPERVISION_CTE}
    {_CLIENT_RECORD_SUPERVISION_LEVEL_CTE}
    {_CLIENT_RECORD_SUPERVISION_SUPER_SESSIONS_CTE}
    {_CLIENT_RECORD_DISPLAY_IDS_CTE}
    {_CLIENT_RECORD_PHONE_NUMBERS_CTE}
    {_CLIENT_RECORD_FINES_FEES_INFO_CTE}
    {_CLIENT_RECORD_LAST_PAYMENT_INFO_CTE}
    {_CLIENT_RECORD_SPECIAL_CONDITIONS_CTE}
    {_CLIENT_RECORD_BOARD_CONDITIONS_CTE}
    {_CLIENT_RECORD_EMAIL_ADDRESSES_CTE}
    {_CLIENT_RECORD_EMPLOYMENT_INFO_CTE}
    {_CLIENT_RECORD_MILESTONES_CTE}
    {_CLIENT_RECORD_INCLUDE_CLIENTS_CTE}
    {_CLIENT_RECORD_JOIN_CLIENTS_CTE}
    {_CLIENTS_CTE}
    """
