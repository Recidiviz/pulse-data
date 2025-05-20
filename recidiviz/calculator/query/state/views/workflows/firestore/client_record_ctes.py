# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
    list_to_query_string,
    nonnull_end_date_exclusive_clause,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    workflows_state_specific_supervision_level,
    workflows_state_specific_supervision_type,
)
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
    STATES_WITH_NO_INFERRED_OPEN_SPANS,
)
from recidiviz.calculator.query.state.views.workflows.firestore.generate_person_metadata_cte import (
    generate_person_metadata_cte,
)
from recidiviz.calculator.query.state.views.workflows.firestore.shared_state_agnostic_ctes import (
    CLIENT_OR_RESIDENT_RECORD_STABLE_PERSON_EXTERNAL_IDS_CTE_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_ca.shared_ctes import (
    US_CA_MOST_RECENT_CLIENT_DATA,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    us_tn_fines_fees_info,
)
from recidiviz.calculator.query.state.views.workflows.us_tx.shared_ctes import (
    US_TX_MAX_TERMINATION_DATES,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    compartment_level_1_super_sessions_without_me_sccp,
)
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    us_pa_supervision_super_sessions,
)
from recidiviz.utils.string import StrictStringFormatter

STATES_WITH_CLIENT_METADATA = [
    StateCode.US_UT,
]

STATES_WITH_ALTERNATE_OFFICER_SOURCES = list_to_query_string(
    ["US_CA", "US_OR"], quoted=True
)

STATES_WITH_OUT_OF_STATE_CLIENTS_INCLUDED = list_to_query_string(["US_IA"], quoted=True)

_CLIENT_RECORD_US_OR_CASELOAD_CTE = """
    us_or_caseloads AS (
        SELECT
            person_id,
            CASELOAD as caseload,
            county_code as oregon_county
        FROM `{project_id}.{us_or_raw_data_up_to_date_dataset}.RCDVZ_PRDDTA_OP013P_latest`
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id`
        ON RECORD_KEY = external_id
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_supervision_period`
        USING(person_id)
        WHERE id_type = "US_OR_RECORD_KEY"
        AND termination_date IS NULL
    ),
"""

_CLIENT_RECORD_SUPERVISION_CTE = f"""
    us_ca_most_recent_client_data AS (
        {US_CA_MOST_RECENT_CLIENT_DATA}
    ), 

    us_tx_max_termination_dates AS (
        {US_TX_MAX_TERMINATION_DATES}
    ),

    -- Pull the most recent |group_projected_full_term_release_date_max| for all person/sentence spans
    -- that do not start in the future
    -- For states in the |STATES_WITH_NO_INFERRED_OPEN_SPANS| list, only pull the sentence date if the
    -- span is currently open
    projected_completion_date_sessions AS (
        SELECT
            state_code,
            person_id,
            group_projected_full_term_release_date_max,
        FROM `{{project_id}}.{{sentence_sessions_dataset}}.person_projected_date_sessions_materialized`
        WHERE start_date <= CURRENT_DATE("US/Eastern")
            -- TODO(#41395): remove this restriction once supervision_projected_completion_date_spans is deprecated
            AND state_code NOT IN ({list_to_query_string(
                string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
                quoted=True,
            )})
            -- Only Infer the most recent sentence span is open if the state code is not in the
            -- "no inferred open spans" config list
            AND (
                IF(state_code IN ({list_to_query_string(
                        string_list=STATES_WITH_NO_INFERRED_OPEN_SPANS,
                        quoted=True,
                    )}),
                    end_date_exclusive IS NULL,
                    TRUE
                )
            )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY start_date DESC) = 1
    ),

    supervision_cases AS (
        SELECT
          sessions.person_id,
          sessions.state_code,
          COALESCE(state_specific_supervision_type.supervision_type, sessions.compartment_level_2) AS supervision_type,
            -- Pull the officer ID from compartment_sessions instead of supervision_officer_sessions
            -- to make sure we choose the officer that aligns with other compartment session attributes.
          #   There are officers with more than one legitimate external id. We are merging these ids and
          #   so must move all clients to the merged id.
          COALESCE(
            us_or_caseloads.caseload,
            ca_pp.BadgeNumber,
            ids.external_id_mapped,
            sessions.supervising_officer_external_id_end
          ) AS officer_id,
          COALESCE(oregon_county, sessions.supervision_district_name_end) AS district,
          -- Supervision expiration dates in UT should display as one day prior to the date in our data
          (CASE
            WHEN sessions.state_code = "US_UT" THEN DATE_SUB(projected_end.group_projected_full_term_release_date_max, INTERVAL 1 DAY)
            ELSE COALESCE(
                    projected_end.group_projected_full_term_release_date_max,
                    projected_end_v1.projected_completion_date_max,
                    LEAST(ca_pp.EarnedDischargeDate, ca_pp.ControllingDischargeDate),
                    ca_pp.EarnedDischargeDate,
                    ca_pp.ControllingDischargeDate,
                    us_tx_max_termination_dates.tx_max_termination_Date
                )
          END) AS expiration_date
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
        LEFT JOIN `{{project_id}}.{{static_reference_tables_dataset}}.agent_multiple_ids_map` ids
            ON sessions.supervising_officer_external_id_end = ids.external_id_to_map AND sessions.state_code = ids.state_code 
        LEFT JOIN projected_completion_date_sessions projected_end
            ON sessions.state_code = projected_end.state_code
            AND sessions.person_id = projected_end.person_id
        # TODO(#41395): Deprecate `supervision_projected_completion_date_spans` and pull all
        # sentence data through the sentence v2 views
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` projected_end_v1
            ON sessions.state_code = projected_end_v1.state_code
            AND sessions.person_id = projected_end_v1.person_id
            AND CURRENT_DATE('US/Eastern')
                BETWEEN projected_end_v1.start_date
                    AND {nonnull_end_date_exclusive_clause('projected_end_v1.end_date')}
        -- Remove clients who previously had an active officer, but no longer do.
        INNER JOIN (
            SELECT DISTINCT
                state_code,
                person_id
            FROM `{{project_id}}.{{sessions_dataset}}.supervision_officer_sessions_materialized`
            WHERE end_date IS NULL
                AND (state_code IN ({STATES_WITH_ALTERNATE_OFFICER_SOURCES}) OR supervising_officer_external_id IS NOT NULL)
        ) active_officer
            ON sessions.state_code = active_officer.state_code
            AND sessions.person_id = active_officer.person_id
        LEFT JOIN us_ca_most_recent_client_data ca_pp
            ON sessions.person_id = ca_pp.person_id
        LEFT JOIN us_or_caseloads
            ON sessions.person_id = us_or_caseloads.person_id
        LEFT JOIN us_tx_max_termination_dates
            ON sessions.person_id = us_tx_max_termination_dates.person_id
        LEFT JOIN (
            {workflows_state_specific_supervision_type()}
        ) state_specific_supervision_type
            ON sessions.person_id = state_specific_supervision_type.person_id
        WHERE sessions.state_code IN ({{workflows_supervision_states}})
          AND (sessions.compartment_level_1 = "SUPERVISION"
            OR (
                sessions.state_code IN ({STATES_WITH_OUT_OF_STATE_CLIENTS_INCLUDED})
                AND sessions.compartment_level_1 = "SUPERVISION_OUT_OF_STATE"
            )
          )
          AND sessions.end_date IS NULL
          AND (sessions.state_code IN ({STATES_WITH_ALTERNATE_OFFICER_SOURCES})
            OR sessions.supervising_officer_external_id_end IS NOT NULL)
          AND (sessions.state_code != "US_CA" OR ca_pp.BadgeNumber IS NOT NULL)
          AND (sessions.state_code != "US_OR" OR us_or_caseloads.caseload IS NOT NULL)
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
            AND dataflow.dataflow_session_id = sl.dataflow_session_id_end,
            UNNEST(session_attributes) as session_attributes
        WHERE sl.state_code IN ({{workflows_supervision_states}})
        AND sl.end_date IS NULL
        # This row partitions by person_id and supervision start_date to account for when we see multiple 
        # supervision_level_raw_text values in dataflow_sessions for the most_recent_active supervision level in 
        # supervision level sessions, and prioritizes the DIVERSION value 
        QUALIFY ROW_NUMBER() OVER(PARTITION BY dataflow.person_id, dataflow.start_date ORDER BY session_attributes.correctional_level = 'DIVERSION' DESC)=1
    ),
    """


_CLIENT_RECORD_CASE_TYPE_CTE = """
    case_type AS (
        SELECT
            state_code,
            person_id,
            case_type,
            case_type_raw_text,
        FROM `{project_id}.{sessions_dataset}.compartment_sub_sessions_materialized`
        WHERE state_code IN ({workflows_supervision_states})
            AND CURRENT_DATE('US/Eastern') >= start_date
            AND CURRENT_DATE('US/Eastern') < COALESCE(end_date_exclusive, '9999-09-09')
    ),
    """


_CLIENT_RECORD_SUPERVISION_SUPER_SESSIONS_CTE = f"""
    supervision_super_sessions AS (
        
        WITH {compartment_level_1_super_sessions_without_me_sccp()}
        
        # TODO(#23716) - Remove this CTE once CA date in super_sessions is fixed
        , us_ca_most_recent_client_data AS (
            {US_CA_MOST_RECENT_CLIENT_DATA}
        )

        # This CTE has 1 row per person with an active supervision period and the start_date corresponds to 
        # the earliest start date for dual supervision periods.
        SELECT
            person_id,
            start_date
        FROM `{{project_id}}.{{sessions_dataset}}.prioritized_supervision_super_sessions_materialized`
        WHERE state_code IN ({{workflows_supervision_states}})
        AND end_date_exclusive IS NULL
        AND state_code NOT IN ('US_ME', 'US_CA', 'US_PA', 'US_IX')
        # TODO(#20872) - Remove 'US_ME' filter once super_sessions fixed
        # TODO(#23716) - Remove 'US_CA' filter once CA date in super_sessions is fixed
        # TODO(#31253) - Remove 'US_PA' filter once prioritized_super_sessions is fixed
        # TODO(#38913) - Remove 'US_IX' filter once PBH sessions have overlapping supervision sessions
        
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

        #TODO(#23716) - remove once CA date in super_sessions is fixed
        UNION ALL

        SELECT
            person_id,
            LastParoleDate AS start_date
        FROM us_ca_most_recent_client_data

        # TODO(#31253) - Move this PA-specific upstream of prioritized super sessions
        UNION ALL
        SELECT 
            person_id,
            release_date AS start_date,
        FROM ({us_pa_supervision_super_sessions()})
        WHERE end_date_exclusive IS NULL

        # TODO(#38913) - US_IX: infer parole board holds as supervision periods
        UNION ALL
        SELECT
            person_id,
            start_date,
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`
        WHERE end_date_exclusive IS NULL
            AND state_code = "US_IX"
    ),
    """

_CLIENT_RECORD_PHONE_NUMBERS_CTE = """
    phone_numbers AS (
        # TODO(#14676): Pull from state_person.phone_number once hydrated
        SELECT
            "US_ND" AS state_code,
            pei.person_id, 
            doc.PHONE AS phone_number
        FROM `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offenders_latest` doc        
        INNER JOIN (
            SELECT *
            FROM `{project_id}.normalized_state.state_person_external_id`
            WHERE state_code = "US_ND" AND id_type = "US_ND_SID"
        ) pei
        ON doc.SID = pei.external_id
        WHERE doc.PHONE IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY pei.person_id ORDER BY RECORDCRDATE DESC, phone_number) = 1

        UNION ALL

        SELECT
            sp.state_code,
            sp.person_id,
            REPLACE(sp.current_phone_number, '-', '') AS phone_number
        FROM `{project_id}.{normalized_state_dataset}.state_person` sp
        WHERE sp.state_code IN ({workflows_supervision_states})
            AND sp.state_code NOT IN ("US_ND")
    ),
"""

_CLIENT_RECORD_EMAIL_ADDRESSES_CTE = """
    email_addresses AS (
        SELECT
            sp.state_code,
            sp.person_id,
            sp.current_email_address as email_address
        FROM `{project_id}.{normalized_state_dataset}.state_person` sp
        WHERE
            sp.state_code IN ({workflows_supervision_states})
    ),
"""

_CLIENT_RECORD_PHYSICAL_ADDRESSES_CTE = """
    physical_addresses AS (
        SELECT
            state_code,
            person_id,
            full_address AS current_physical_residence_address_unstructured,
            TO_JSON_STRING(STRUCT(
                address_line_1,
                address_line_2,
                address_city,
                address_state,
                address_zip,
                address_country
            )) AS current_physical_residence_address_structured
        FROM  `{project_id}.reference_views.current_physical_residence_address_materialized`
    ),
"""

_CLIENT_RECORD_FINES_FEES_INFO_CTE = f"""
    {us_tn_fines_fees_info()}
"""

_CLIENT_RECORD_LAST_PAYMENT_INFO_CTE = """
    fines_fees_payment_info AS (
        SELECT 
               pp.state_code,
               pp.person_id,
               pp.payment_date AS last_payment_date,
               pp.payment_amount AS last_payment_amount,
        FROM `{project_id}.{analyst_dataset}.payments_preprocessed_materialized` pp
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY person_id 
            -- For payments on the same date, choose the largest payment to display
            ORDER BY payment_date DESC, payment_amount DESC
        ) = 1
    ),
"""

_CLIENT_RECORD_SPECIAL_CONDITIONS_CTE = """
    special_conditions AS (
        SELECT 
            state_code,
            person_id,
            ARRAY_AGG(conditions IGNORE NULLS ORDER BY conditions) AS special_conditions_on_current_sentences
        FROM (
            SELECT state_code, person_id, conditions
            FROM `{project_id}.{normalized_state_dataset}.state_supervision_sentence`
            WHERE COALESCE(completion_date, projected_completion_date) >= CURRENT_DATE('US/Eastern')
            
            UNION ALL
            
            SELECT state_code, person_id, conditions
            FROM `{project_id}.{normalized_state_dataset}.state_incarceration_sentence`
            WHERE COALESCE(completion_date, projected_max_release_date) >= CURRENT_DATE('US/Eastern')
        )
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
                ss.person_id,
                ss.state_code,
                start_date AS latest_system_session_start_date
            FROM `{project_id}.{sessions_dataset}.system_sessions_materialized` ss
            WHERE ss.state_code = 'US_TN' 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY session_id_end DESC) = 1
        ),
        unpivot_board_conditions AS (
            SELECT 
                person_id,
                hearing_date, condition, Decode AS condition_description
            FROM (
                SELECT 
                    DISTINCT 
                       person_id,
                       CAST(HearingDate AS DATE) AS hearing_date, 
                       ParoleCondition1, 
                       ParoleCondition2, 
                       ParoleCondition3, 
                       ParoleCondition4, 
                       ParoleCondition5
                FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.BoardAction_latest`
                JOIN (
                    SELECT *
                    FROM `{project_id}.normalized_state.state_person_external_id`
                    WHERE state_code = "US_TN" AND id_type = "US_TN_DOC"
                ) pei
                ON pei.external_id = OffenderID
                WHERE FinalDecision = 'Y'
            )
            UNPIVOT(condition for c in (ParoleCondition1, ParoleCondition2, ParoleCondition3, ParoleCondition4, ParoleCondition5))
            LEFT JOIN (
                SELECT Code, Decode
                FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.CodesDescription_latest`
                WHERE CodesTableID = 'TDPD030'
            ) codes 
                ON condition = codes.Code
        )
        SELECT
            ls.person_id,
            ls.state_code, 
            ARRAY_AGG(STRUCT(condition, condition_description) ORDER BY condition, condition_description) AS board_conditions
        FROM latest_system_start ls
        INNER JOIN unpivot_board_conditions bc
            ON ls.person_id = bc.person_id
            AND bc.hearing_date >= COALESCE(ls.latest_system_session_start_date,'1900-01-01')
        GROUP BY 1,2
    ),
"""


_CLIENT_RECORD_EMPLOYMENT_INFO_CTE = f"""
    employment_info AS (
        SELECT
            state_code,
            person_id,
            ARRAY_AGG(
                STRUCT(employer_name AS name, employer_address AS address, start_date as start_date, employment_status as employment_status)
                IGNORE NULLS
                ORDER BY start_date ASC, employer_name, employment_status, employer_address) AS current_employers
        FROM (
            SELECT DISTINCT
                state_code,
                person_id,
                sep.employer_name,
                sep.start_date,
                NULL AS end_date,
                sep.employment_status,
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
        WHERE state_code IN ("US_IX", "US_CA", "US_NE")
        GROUP BY state_code, person_id
    ),
"""

_CLIENT_RECORD_ACTIVE_SENTENCES_CTE = f"""
    active_sentences AS (
        SELECT
            sentence_period.state_code,
            sentence_period.person_id,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(
                        sentence_period.sentence_id,
                        sentence.date_imposed,
                        sentence.county_code,
                        sentence.offense_type,
                        sentence.is_sex_offense
                    ) ORDER BY sentence.date_imposed ASC, sentence_period.sentence_id ASC
                )
            ) AS active_sentences,
        FROM `{{project_id}}.sentence_sessions.sentence_serving_period_materialized` sentence_period
        LEFT JOIN `{{project_id}}.sessions.sentences_preprocessed_materialized` sentence
        USING(state_code, person_id, sentence_id)
        WHERE 
            {today_between_start_date_and_nullable_end_date_clause('sentence_period.start_date', 'sentence_period.end_date_exclusive')}
        GROUP BY 1,2
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
            FROM `{{project_id}}.{{dataflow_metrics_dataset}}.most_recent_violation_with_response_metrics_materialized` df
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
            ss.start_date as start_date,
            {years_and_months_template('ss.start_date')}
        FROM supervision_cases
        INNER JOIN supervision_super_sessions ss USING(person_id)
        WHERE DATE_DIFF(CURRENT_DATE('US/Eastern'), ss.start_date, MONTH) > 0
    ),
    time_with_employer AS (
        SELECT
            state_code,
            person_id,
            current_employers[OFFSET(0)].start_date as start_date,
            {years_and_months_template("current_employers[OFFSET(0)].start_date")}
        FROM employment_info
        LEFT JOIN supervision_super_sessions ss
        USING(person_id)
        WHERE DATE_DIFF(CURRENT_DATE('US/Eastern'), current_employers[OFFSET(0)].start_date, MONTH) > 0
        AND current_employers[OFFSET(0)].start_date > ss.start_date
    ),
    -- For each milestone, the milestone_date represents the earliest date in which the individual was
    -- eligible for a given milestone. For example, if an individual is eligible for the milestone BIRTHDAY_THIS_MONTH, 
    -- and their birthday is on 04/23/2024, the milestone date would be 04/01/2024.
    milestones AS (
        SELECT
            state_code,
            person_id,
            ARRAY_AGG(
                STRUCT(milestone_type AS type, milestone_text AS text, milestone_date AS milestone_date)
                IGNORE NULLS
                ORDER BY milestone_priority ASC, milestone_type
            ) AS milestones
        FROM (
            -- birthdays
            -- milestone_date is the first day of the month for the individual's birthday month
            SELECT *
            FROM (
                SELECT
                    sc.state_code,
                    person_id,
                    IF(
                        EXTRACT(MONTH from sp.birthdate) = EXTRACT(MONTH from CURRENT_DATE('US/Eastern')) AND EXTRACT(DAY from sp.birthdate) <= EXTRACT(DAY from CURRENT_DATE('US/Eastern')),
                        DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH),
                        NULL
                    ) AS milestone_date,
                    IF(
                        EXTRACT(MONTH from sp.birthdate) = EXTRACT(MONTH from CURRENT_DATE('US/Eastern')) AND EXTRACT(DAY from sp.birthdate) <= EXTRACT(DAY from CURRENT_DATE('US/Eastern')),
                        "Birthday this month (" || FORMAT_DATE('%B %d', sp.birthdate) ||")",
                        NULL
                    ) AS milestone_text,
                    "BIRTHDAY_THIS_MONTH" as milestone_type,
                    1 AS milestone_priority,
                FROM supervision_cases sc
                LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
                USING(person_id)
                WHERE sc.state_code NOT IN ('US_UT')
            )
            UNION ALL
            -- months without violation
            -- milestone_date is the earliest date in which the individual was eligible for X MONTHS_WITHOUT_VIOLATION
            -- where X is the number of months since their last violation
            SELECT
                state_code,
                person_id,
                DATE_ADD(violation_date, INTERVAL(DATE_DIFF(CURRENT_DATE('US/Eastern'), violation_date, MONTH)) MONTH) AS milestone_date,
                {milestone_text_template(" since last violation")}                
                "MONTHS_WITHOUT_VIOLATION" as milestone_type,
                2 AS milestone_priority
            FROM time_without_violation
            
            UNION ALL
            
            -- months on supervision
            -- milestone_date is the earliest date in which the individual was eligible for X MONTHS_ON_SUPERVISION
            -- where X is the number of month they have been on supervision
            SELECT
                state_code,
                person_id,
                DATE_ADD(start_date, INTERVAL (DATE_DIFF(CURRENT_DATE('US/Eastern'), start_date, MONTH)) MONTH) AS milestone_date,
                {milestone_text_template(" on supervision")}  
                "MONTHS_ON_SUPERVISION" as milestone_type,
                3 AS milestone_priority
            FROM time_on_supervision
            WHERE state_code NOT IN ('US_UT')
            
            UNION ALL
            -- months with the same employer
            -- milestone_date is the earliest date in which the individual was eligible for X MONTHS_WITH_CURRENT_EMPLOYER
            -- where X is the number of month they have been with the same employer

                SELECT
                    state_code,
                    person_id,
                    DATE_ADD(start_date, INTERVAL (DATE_DIFF(CURRENT_DATE('US/Eastern'), start_date, MONTH)) MONTH) AS milestone_date,
                    {milestone_text_template(" with the same employer")}
                    "MONTHS_WITH_CURRENT_EMPLOYER" as milestone_type,
                    4 AS milestone_priority
                FROM time_with_employer

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for NO_VIOLATION_WITHIN_6_MONTHS
                SELECT
                    state_code,
                    person_id,
                    kudos.start_date as milestone_date,
                    "6+ months violation-free" as milestone_text,
                    "NO_VIOLATION_WITHIN_6_MONTHS" as milestone_type,
                    10 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_violation_free_6_to_8_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for NO_VIOLATION_WITHIN_12_MONTHS
                SELECT
                    state_code,
                    person_id,
                    kudos.start_date as milestone_date,

                    "1+ year violation-free" as milestone_text,
                    "NO_VIOLATION_WITHIN_12_MONTHS" as milestone_type,
                    11 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_violation_free_12_to_14_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for HOUSING_TYPE_IS_NOT_TRANSIENT
                SELECT
                    state_code,
                    person_id,
                    kudos.start_date as milestone_date,
                    "Found housing" as milestone_text,
                    "HOUSING_TYPE_IS_NOT_TRANSIENT" as milestone_type,
                    21 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_sustainable_housing_0_to_2_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for SUSTAINABLE_HOUSING_6_MONTHS
                SELECT
                    state_code,
                    person_id,
                    kudos.start_date as milestone_date,
                    "Sustainable housing for 6+ months" as milestone_text,
                    "SUSTAINABLE_HOUSING_6_MONTHS" as milestone_type,
                    22 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_sustainable_housing_6_to_8_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for SUSTAINABLE_HOUSING_12_MONTHS
                SELECT
                    state_code,
                    person_id,
                    kudos.start_date as milestone_date,
                    "Sustainable housing for 1+ year" as milestone_text,
                    "SUSTAINABLE_HOUSING_12_MONTHS" as milestone_type,
                    23 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_sustainable_housing_12_to_14_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for GAINED_EMPLOYMENT
                SELECT
                state_code,
                person_id,
                kudos.start_date as milestone_date,
                "Gained employment" || (
                    -- When there is only one employer during the period, surface that employer name in the milestone
                    -- text by concatenating the employer name. Otherwise, ignore employer name.
                    case when array_length(json_value_array(reasons[0], '$.reason.status_employer_start_date')) = 1
                    then " with " || 
                    split(
                        json_value_array(reasons[0], '$.reason.status_employer_start_date')[SAFE_OFFSET(0)], '@@'
                    )[SAFE_OFFSET(1)]
                    else ""
                    end
                ) as milestone_text,
                "GAINED_EMPLOYMENT" as milestone_type,
                31 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_employment_0_to_2_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for EMPLOYED_6_MONTHS
                SELECT
                state_code,
                person_id,
                kudos.start_date as milestone_date,
                "Employed for 6+ months" || (
                    -- When there is only one employer during the period, surface that employer name in the milestone
                    -- text by concatenating the employer name. Otherwise, ignore employer name.
                    case when array_length(json_value_array(reasons[0], '$.reason.status_employer_start_date')) = 1
                    then " with " || 
                    split(
                        json_value_array(reasons[0], '$.reason.status_employer_start_date')[SAFE_OFFSET(0)], '@@'
                    )[SAFE_OFFSET(1)]
                    else ""
                    end
                ) as milestone_text,
                "EMPLOYED_6_MONTHS" as milestone_type,
                32 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_employment_6_to_8_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for US_UT_EMPLOYED_6_MONTHS
                SELECT
                state_code,
                person_id,
                emp.start_date as milestone_date,
                "Employed, studying, or with another source of income for 6+ months" as milestone_text,
                "US_UT_EMPLOYED_6_MONTHS" as milestone_type,
                32 AS milestone_priority
                FROM `{{project_id}}.{{task_eligibility_criteria_general}}.supervision_continuous_employment_for_6_months_materialized` emp
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND emp.meets_criteria

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for EMPLOYED_12_MONTHS
                SELECT
                state_code,
                person_id,
                kudos.start_date as milestone_date,
                "Employed for 1 year" || (
                    -- When there is only one employer during the period, surface that employer name in the milestone
                    -- text by concatenating the employer name. Otherwise, ignore employer name.
                    case when array_length(json_value_array(reasons[0], '$.reason.status_employer_start_date')) = 1
                    then " with " || 
                    split(
                        json_value_array(reasons[0], '$.reason.status_employer_start_date')[SAFE_OFFSET(0)], '@@'
                    )[SAFE_OFFSET(1)]
                    else ""
                    end
                ) as milestone_text,
                "EMPLOYED_12_MONTHS" as milestone_type,
                33 AS milestone_priority
                FROM `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_employment_12_to_14_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible
            
            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for PARTICIPATED_IN_PROGRAMMING_FOR_6_TO_8_MONTHS
                SELECT
                state_code,
                person_id,
                kudos.start_date as milestone_date,
                CONCAT( 
                    -- Generates a string stating how many programs the client has been
                    -- participating in with slightly different phrasing for folks who
                    -- are in multiple programs
                    CASE WHEN ARRAY_LENGTH(JSON_QUERY_ARRAY(reasons[0], '$.reason')) > 1
                    THEN
                        "Active in the following " || ARRAY_LENGTH(JSON_QUERY_ARRAY(reasons[0], '$.reason')) || " programs for 6 months: " 
                    ELSE
                        "Active in the following program for 6 months: "
                    END,
                    (
                        SELECT 
                            STRING_AGG(program_id, ', ' ORDER BY program_id)
                        FROM (
                            SELECT JSON_VALUE(program_reasons, '$.program_id') AS program_id
                            FROM UNNEST(JSON_QUERY_ARRAY(reasons[0], '$.reason')) as program_reasons
                        )
                    )
                )
                as milestone_text,
                "PARTICIPATED_IN_PROGRAMMING_FOR_6_TO_8_MONTHS" as milestone_type,
                40 as milestone_priority,
                from `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_program_participation_6_to_8_months_materialized` kudos
                WHERE
                    {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
                    AND is_eligible

            UNION ALL
                -- milestone_date is the earliest date in which the individual was eligible for PARTICIPATED_IN_PROGRAMMING_FOR_12_TO_14_MONTHS
                SELECT
                state_code,
                person_id,
                kudos.start_date as milestone_date,
                CONCAT( 
                    -- Generates a string stating how many programs the client has been
                    -- participating in with slightly different phrasing for folks who
                    -- are in multiple programs
                    CASE WHEN ARRAY_LENGTH(JSON_QUERY_ARRAY(reasons[0], '$.reason')) > 1
                    THEN
                        "Active in the following " || ARRAY_LENGTH(JSON_QUERY_ARRAY(reasons[0], '$.reason')) || " programs for 1 year: " 
                    ELSE
                        "Active in the following program for 1 year: "
                    END,
                    (
                        SELECT 
                            STRING_AGG(program_id, ', ' ORDER BY program_id)
                        FROM (
                            SELECT JSON_VALUE(program_reasons, '$.program_id') AS program_id
                            FROM 
                                UNNEST(JSON_QUERY_ARRAY(reasons[0], '$.reason')) as program_reasons
                        )
                    )
                )
                as milestone_text,
                "PARTICIPATED_IN_PROGRAMMING_FOR_12_TO_14_MONTHS" as milestone_type,
                41 as milestone_priority,
                from `{{project_id}}.{{us_ca_task_eligibility_spans_dataset}}.kudos_program_participation_12_to_14_months_materialized` kudos
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
        INNER JOIN (
            SELECT *
            FROM `{project_id}.normalized_state.state_person_external_id`
            WHERE state_code = "US_TN" AND id_type = "US_TN_DOC"
        ) pei
        ON tn_raw.OffenderID = pei.external_id

    
        UNION ALL
    
        SELECT DISTINCT 
            person_id,
            state_code,
        FROM `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_OFFENDER_latest` mi_raw
        INNER JOIN (
            SELECT *
            FROM `{project_id}.normalized_state.state_person_external_id`
            WHERE state_code = "US_MI" AND id_type = "US_MI_DOC"
        ) pei
        ON LPAD(mi_raw.offender_number, 7, "0") = pei.external_id
        
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

# TODO(#38551): Change the SAFE_CAST below (for phone numbers) back to CAST once we
# enforce in ingest that phone numbers in the data are actually shaped like phone
# numbers.
_CLIENT_RECORD_JOIN_CLIENTS_CTE = """
    join_clients AS (
        SELECT DISTINCT
          sc.state_code,
          sc.person_id,
          did.display_person_external_id,
          did.display_person_external_id_type,
          sp.full_name as person_name,
          # TODO(#42464): Update all frontend references to address to instead reference
          #  current_physical_residence_address_unstructured and delete this field
          #  from the client record.
          pa.current_physical_residence_address_unstructured AS address,
          pa.current_physical_residence_address_unstructured,
          pa.current_physical_residence_address_structured,
          SAFE_CAST(ph.phone_number AS INT64) AS phone_number,
          LOWER(ea.email_address) AS email_address,
          sc.supervision_type,
          sc.officer_id,
          sc.district,
          ct.case_type,
          ct.case_type_raw_text,
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
          COALESCE(m.metadata, '{{}}') AS metadata,
        FROM supervision_cases sc 
        INNER JOIN supervision_level_start sl USING(person_id)
        INNER JOIN supervision_super_sessions ss USING(person_id)
        INNER JOIN include_clients USING(person_id)
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person` sp USING(person_id)
        LEFT JOIN (
            SELECT state_code, person_id, display_person_external_id, display_person_external_id_type
            FROM `{project_id}.reference_views.product_display_person_external_ids_materialized`
            WHERE system_type = "SUPERVISION"
        ) did
            ON sc.person_id = did.person_id
        LEFT JOIN phone_numbers ph
            ON sc.person_id = ph.person_id
        LEFT JOIN email_addresses ea
            ON sc.person_id = ea.person_id
        LEFT JOIN fines_fees_balance_info ff
            ON sc.person_id = ff.person_id
        LEFT JOIN fines_fees_payment_info pp
            ON sc.person_id = pp.person_id
        LEFT JOIN case_type ct
            ON ct.state_code = sc.state_code
            AND ct.person_id = sc.person_id
        LEFT JOIN metadata m
            ON sc.person_id = m.person_id
        LEFT JOIN physical_addresses pa
            ON sc.state_code = pa.state_code
            AND sc.person_id = pa.person_id
        
    ),
    """

_CLIENTS_CTE = """
    clients AS (
        # Values set to NULL are not applicable for this state
        SELECT
            c.person_id,
            stable_person_external_ids.person_external_id,
            # TODO(#41556): Update frontend to reference display_person_external_id column
            #  and delete the ambiguously-named display_id column.
            c.display_person_external_id AS display_id,
            c.display_person_external_id,
            c.display_person_external_id_type,
            c.state_code,
            person_name,
            officer_id,
            case_type,
            case_type_raw_text,
            supervision_type,
            supervision_level,
            supervision_level_start,
            address,
            current_physical_residence_address_unstructured,
            current_physical_residence_address_structured,
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
            active_sentences.active_sentences,
            metadata,
        FROM join_clients c
        LEFT JOIN stable_person_external_ids
            USING (person_id)
        LEFT JOIN opportunities_aggregated 
            USING (person_id)
        LEFT JOIN special_conditions spc
            USING (person_id)
        LEFT JOIN board_conditions bc
            USING (person_id)
        LEFT JOIN employment_info ei 
            USING (person_id)
        LEFT JOIN active_sentences
            USING (person_id)
        LEFT JOIN milestones mi 
            USING (person_id)
        # TODO(#17138): Remove this condition if we are no longer missing person details post-ATLAS
        WHERE person_name IS NOT NULL
    )
    """


def full_client_record() -> str:
    stable_person_external_ids_cte = StrictStringFormatter().format(
        CLIENT_OR_RESIDENT_RECORD_STABLE_PERSON_EXTERNAL_IDS_CTE_TEMPLATE,
        system_type="SUPERVISION",
    )
    return f"""
    {_CLIENT_RECORD_US_OR_CASELOAD_CTE}
    {_CLIENT_RECORD_SUPERVISION_CTE}
    {_CLIENT_RECORD_SUPERVISION_LEVEL_CTE}
    {_CLIENT_RECORD_CASE_TYPE_CTE}
    {_CLIENT_RECORD_SUPERVISION_SUPER_SESSIONS_CTE}
    {stable_person_external_ids_cte}
    {_CLIENT_RECORD_PHONE_NUMBERS_CTE}
    {_CLIENT_RECORD_FINES_FEES_INFO_CTE}
    {_CLIENT_RECORD_LAST_PAYMENT_INFO_CTE}
    {_CLIENT_RECORD_SPECIAL_CONDITIONS_CTE}
    {_CLIENT_RECORD_BOARD_CONDITIONS_CTE}
    {_CLIENT_RECORD_EMAIL_ADDRESSES_CTE}
    {_CLIENT_RECORD_PHYSICAL_ADDRESSES_CTE}
    {_CLIENT_RECORD_EMPLOYMENT_INFO_CTE}
    {_CLIENT_RECORD_ACTIVE_SENTENCES_CTE}
    {_CLIENT_RECORD_MILESTONES_CTE}
    {_CLIENT_RECORD_INCLUDE_CLIENTS_CTE}
    {generate_person_metadata_cte("client", "include_clients", STATES_WITH_CLIENT_METADATA)}
    {_CLIENT_RECORD_JOIN_CLIENTS_CTE}
    {_CLIENTS_CTE}
    """
