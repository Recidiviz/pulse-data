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

_CLIENT_RECORD_SUPERVISION_CTE = f"""
    supervision_cases AS (
        SELECT
          sessions.person_id,
          sessions.state_code,
          pei.external_id AS person_external_id,
          sessions.compartment_level_2 AS supervision_type,
            -- Pull the officer ID from compartment_sessions instead of supervision_officer_sessions
            -- to make sure we choose the officer that aligns with other compartment session attributes.
          #   There are officers with more than one legitimate external id. We are merging these ids and
          #   so must move all clients to the merged id.
          IFNULL(ids.external_id_mapped, sessions.supervising_officer_external_id_end) AS officer_id,
          sessions.supervision_district_name_end AS district,
          projected_end.projected_completion_date_max AS expiration_date
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
        LEFT JOIN {{project_id}}.{{static_reference_tables_dataset}}.agent_multiple_ids_map ids
            ON sessions.supervising_officer_external_id_end = ids.external_id_to_map AND sessions.state_code = ids.state_code 
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON sessions.person_id = pei.person_id
            AND sessions.state_code = pei.state_code
            AND {{state_id_type}} = pei.id_type
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` projected_end
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
                AND supervising_officer_external_id IS NOT NULL
        ) active_officer
            ON sessions.state_code = active_officer.state_code
            AND sessions.person_id = active_officer.person_id
        WHERE sessions.state_code IN ({{workflows_supervision_states}})
          AND sessions.compartment_level_1 = "SUPERVISION"
          AND sessions.end_date IS NULL
          AND sessions.supervising_officer_external_id_end IS NOT NULL
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
    ),
    """


_CLIENT_RECORD_SUPERVISION_SUPER_SESSIONS_CTE = """
    supervision_super_sessions AS (
        # This CTE has 1 row per person with an active supervision period and the start_date corresponds to 
        # the earliest start date for dual supervision periods.
        SELECT
            person_id,
            start_date
        FROM `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized`
        WHERE state_code IN ({workflows_supervision_states})
        AND end_date IS NULL
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
            pei.external_id AS person_external_id, 
            doc.PHONE AS phone_number
        FROM `{{project_id}}.{{us_nd_raw_data_up_to_date_dataset}}.docstars_offenders_latest` doc
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON doc.SID = pei.external_id
        AND pei.id_type = "US_ND_SID"

        UNION ALL

        SELECT
            sp.state_code,
            pei.external_id AS person_external_id,
            sp.current_phone_number
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
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
            pei.external_id AS person_external_id,
            sp.current_email_address as email_address
        FROM `{project_id}.{normalized_state_dataset}.state_person` sp
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            USING (person_id)
        WHERE
            sp.state_code IN ({workflows_supervision_states})
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

_CLIENT_RECORD_MILESTONES_CTE = """
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
                LEFT JOIN {project_id}.{normalized_state_dataset}.state_person sp
                USING(state_code, person_id)
            )
            UNION ALL
            -- months without violation
            SELECT *
            FROM (
                SELECT
                    state_code,
                    person_id,
                    CAST(DATE_DIFF(CURRENT_DATE('US/Eastern'), violation_date, MONTH) as string) || " months since last violation" as milestone_text,
                    "MONTHS_WITHOUT_VIOLATION" as milestone_type,
                    2 AS milestone_priority
                FROM (
                    SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY df.state_code, df.person_id order by violation_date desc) as rn
                    FROM {project_id}.{dataflow_metrics_dataset}.most_recent_violation_with_response_metrics_materialized df
                    ORDER BY person_id, rn
                )
                LEFT JOIN supervision_super_sessions ss
                USING(person_id)
                WHERE rn = 1
                AND violation_date > ss.start_date
                AND DATE_DIFF(CURRENT_DATE('US/Eastern'), violation_date, MONTH) > 0
            )
            UNION ALL
            -- months on supervision
            SELECT *
            FROM (
                SELECT
                    state_code,
                    person_id,
                    CAST(DATE_DIFF(CURRENT_DATE('US/Eastern'), ss.start_date, MONTH) as string) || " months on supervision" as milestone_text,
                    "MONTHS_ON_SUPERVISION" as milestone_type,
                    3 AS milestone_priority
                FROM supervision_cases
                INNER JOIN supervision_super_sessions ss USING(person_id)
                WHERE DATE_DIFF(CURRENT_DATE('US/Eastern'), ss.start_date, MONTH) > 0
            )
            UNION ALL
            -- months with the same employer
            SELECT *
            FROM (
                SELECT
                    state_code,
                    person_id,
                    CAST(DATE_DIFF(CURRENT_DATE('US/Eastern'), current_employers[OFFSET(0)].start_date, MONTH) as string) || " months with the same employer" as milestone_text,
                    "MONTHS_WITH_CURRENT_EMPLOYER" as milestone_type,
                    4 AS milestone_priority
                FROM employment_info
                LEFT JOIN supervision_super_sessions ss
                USING(person_id)
                WHERE DATE_DIFF(CURRENT_DATE('US/Eastern'), current_employers[OFFSET(0)].start_date, MONTH) > 0
                AND current_employers[OFFSET(0)].start_date > ss.start_date
            )
        )
        WHERE state_code in ('US_IX')
        AND milestone_text IS NOT NULL
        GROUP BY state_code, person_id
    ),
"""

_CLIENT_RECORD_INCLUDE_CLIENTS_CTE = """
    # For states where each transfer is a full historical transfer, we want to ensure only clients included
    # in the latest file are included in our tool
    # TODO(#18193): This currently doesn't matter for TN because of a TN-specific template that will eventually be 
    # deprecated
    include_clients AS (
        SELECT DISTINCT 
            person_id,
            state_code,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Offender_latest` tn_raw
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON tn_raw.OffenderID = pei.external_id
            AND pei.state_code = "US_TN"
            AND pei.id_type = "US_TN_DOC"
    
        UNION ALL
    
        SELECT DISTINCT 
            person_id,
            state_code,
        FROM `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_OFFENDER_latest` mi_raw
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON mi_raw.offender_number = pei.external_id
            AND pei.state_code = "US_MI"
            AND pei.id_type = "US_MI_DOC"
        
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
        FROM supervision_cases sc 
        INNER JOIN supervision_level_start sl USING(person_id)
        INNER JOIN supervision_super_sessions ss USING(person_id)
        INNER JOIN include_clients USING(person_id)
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person` sp USING(person_id)
        LEFT JOIN phone_numbers ph
            -- join on state_code / person_external_id instead of person_id alone because state data
            -- may have multiple external_ids for a given person_id, and by this point in the
            -- query we've already decided which person_external_id we're using
            ON sc.state_code = ph.state_code
            AND sc.person_external_id = ph.person_external_id
        LEFT JOIN email_addresses ea
            ON sc.state_code = ea.state_code
            AND sc.person_external_id = ea.person_external_id
    ),
    """

_CLIENTS_CTE = """
    clients AS (
        # Values set to NULL are not applicable for this state
        SELECT
            person_external_id,
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
            NULL AS current_balance,
            NULL AS last_payment_amount,
            CAST(NULL AS DATE) AS last_payment_date,
            CAST(NULL AS ARRAY<string>) AS special_conditions,
            CAST(NULL AS ARRAY<STRUCT<condition STRING, condition_description STRING>>) AS board_conditions,
            district,
            ei.current_employers,
            opportunities_aggregated.all_eligible_opportunities,
            milestones
        FROM join_clients c
        LEFT JOIN opportunities_aggregated USING (state_code, person_external_id)
        LEFT JOIN employment_info ei USING (person_id)
        LEFT JOIN milestones mi  ON mi.state_code = c.state_code and mi.person_id = c.person_id
        # TODO(#17138): Remove this condition if we are no longer missing person details post-ATLAS
        WHERE person_name IS NOT NULL
    )
    """


def full_client_record() -> str:
    return f"""
    {_CLIENT_RECORD_SUPERVISION_CTE}
    {_CLIENT_RECORD_SUPERVISION_LEVEL_CTE}
    {_CLIENT_RECORD_SUPERVISION_SUPER_SESSIONS_CTE}
    {_CLIENT_RECORD_PHONE_NUMBERS_CTE}
    {_CLIENT_RECORD_EMAIL_ADDRESSES_CTE}
    {_CLIENT_RECORD_EMPLOYMENT_INFO_CTE}
    {_CLIENT_RECORD_MILESTONES_CTE}
    {_CLIENT_RECORD_INCLUDE_CLIENTS_CTE}
    {_CLIENT_RECORD_JOIN_CLIENTS_CTE}
    {_CLIENTS_CTE}
    """
