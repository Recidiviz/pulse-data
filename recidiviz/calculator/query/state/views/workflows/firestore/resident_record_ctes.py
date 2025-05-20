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
"""CTEs used to create resident record query."""
from typing import List

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
    revert_nonnull_start_date_clause,
)
from recidiviz.calculator.query.state.views.workflows.firestore.shared_state_agnostic_ctes import (
    CLIENT_OR_RESIDENT_RECORD_STABLE_PERSON_EXTERNAL_IDS_CTE_TEMPLATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.utils.us_mo_query_fragments import current_bed_stay_cte
from recidiviz.utils.string import StrictStringFormatter

STATES_WITH_RESIDENT_METADATA = [
    StateCode.US_AR,
    StateCode.US_AZ,
    StateCode.US_MA,
    StateCode.US_MO,
    StateCode.US_ME,
    StateCode.US_IX,
    StateCode.US_ND,
]

_RESIDENT_RECORD_INCARCERATION_CTE = """
    incarceration_cases AS (
        SELECT
            dataflow.state_code,
            dataflow.person_id,
            sp.full_name AS person_name,
            sp.gender AS gender,
            CASE
                WHEN dataflow.state_code IN ({level_2_state_codes}) THEN COALESCE(locations.level_2_incarceration_location_external_id, dataflow.facility)
                WHEN dataflow.state_code = "US_AR" THEN COALESCE(locations.level_1_incarceration_location_name, dataflow.facility)
                ELSE dataflow.facility
            END AS facility_id,
            sessions.start_date as admission_date
        FROM `{project_id}.{dataflow_dataset}.most_recent_incarceration_population_span_metrics_materialized` dataflow
        INNER JOIN `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` sessions
            ON dataflow.state_code = sessions.state_code
            AND dataflow.person_id = sessions.person_id
            AND sessions.compartment_level_1 = "INCARCERATION"
            AND sessions.end_date IS NULL
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person` sp 
            ON dataflow.person_id = sp.person_id
        LEFT JOIN `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names` locations
            ON dataflow.state_code = locations.state_code
            AND dataflow.facility = locations.level_1_incarceration_location_external_id
        LEFT JOIN `{project_id}.{reference_views_dataset}.location_metadata_materialized` location_metadata
            ON dataflow.state_code = location_metadata.state_code
            AND dataflow.facility = location_metadata.location_external_id
        WHERE dataflow.state_code IN ({workflows_incarceration_states}) AND dataflow.included_in_state_population
            AND dataflow.end_date_exclusive IS NULL
            AND NOT (dataflow.state_code = "US_AR"
                    AND location_metadata.location_type != "STATE_PRISON")
            AND NOT (dataflow.state_code = "US_TN"
                    AND locations.level_2_incarceration_location_external_id IN ({us_tn_excluded_facility_ids}))
            AND NOT (dataflow.state_code = "US_IX"
                    AND locations.level_2_incarceration_location_external_id IN ({us_ix_excluded_facility_types}))
    ),
"""

_RESIDENT_RECORD_INCARCERATION_DATES_CTE = f"""
    incarceration_dates AS (
        WITH ME_sis as (
          SELECT *,
            SPLIT(sis.external_id, '-')[OFFSET(1)] as term_id,
          FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_sentence` sis
          WHERE sis.state_code = 'US_ME'
        ), ME_sis_with_longest_release_date as (
          SELECT 
              person_id, 
              term_id, 
              MAX(projected_max_release_date) AS longest_proj_max_release_date
          FROM ME_sis sis
          GROUP BY person_id, term_id
        ), ME_sis_with_life_sentences_adjusted AS (
          SELECT
            person_id,
            term_id,
            CASE longest_proj_max_release_date 
            WHEN DATE('9999-12-31') THEN DATE('9999-12-01')
            ELSE longest_proj_max_release_date
            END as longest_proj_max_release_date
          FROM ME_sis_with_longest_release_date
        ),
        final_ME_terms as (
          SELECT 
            t.* EXCEPT (end_date), 
            t.end_date as term_end_date,
            COALESCE(end_date, sis.longest_proj_max_release_date) as end_date
          FROM `{{project_id}}.{{analyst_dataset}}.us_me_sentence_term_materialized` t
          LEFT JOIN ME_sis_with_life_sentences_adjusted sis on 
            t.person_id = sis.person_id
            AND t.term_id = sis.term_id
        )
        -- Adding a TN specific admission date that is admission to TDOC facility, in addition to overall incarceration
        -- admission date
        SELECT 
            ic.*,
            MAX(t.projected_completion_date_max) 
                    OVER(w) AS release_date,
            -- TODO(#31703): Make this a state agnostic field
            MAX(c.start_date) 
                    OVER(w) AS us_tn_facility_admission_date,
        FROM
            incarceration_cases ic
        LEFT JOIN (
            SELECT person_id, state_code, start_date
            FROM `{{project_id}}.sessions.custodial_authority_sessions_materialized`
            WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date
                AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
                AND custodial_authority = "STATE_PRISON"
        ) c
            USING(person_id, state_code)
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_projected_completion_date_spans_materialized` t
          ON ic.person_id = t.person_id
              AND ic.state_code = t.state_code
              AND CURRENT_DATE('US/Eastern') 
                BETWEEN t.start_date AND {nonnull_end_date_exclusive_clause('t.end_date_exclusive')} 
        WHERE ic.state_code IN ("US_TN")
        WINDOW w as (PARTITION BY ic.person_id)
        
        UNION ALL
        
        SELECT 
            ic.* EXCEPT(admission_date),
            MAX(t.start_date) 
                    OVER(w) AS admission_date,
            MAX({nonnull_end_date_clause('t.end_date')}) 
                    OVER(w) AS release_date,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
            --TODO(#16175) ingest intake and release dates
        FROM
            incarceration_cases ic
        -- Use raw_table to get admission and release dates
        LEFT JOIN final_ME_terms t
          ON ic.person_id = t.person_id
          -- subset the possible start and end_dates to those consistent with
          -- the current date
              AND CURRENT_DATE('US/Eastern') 
                    BETWEEN {nonnull_start_date_clause('t.start_date')} 
                        AND {nonnull_end_date_clause('t.end_date')} 
              AND t.status='1' -- only 'Active terms'
        WHERE ic.state_code="US_ME"
        WINDOW w as (PARTITION BY ic.person_id)

        UNION ALL

        SELECT
            ic.* EXCEPT(admission_date),
            NULL AS admission_date,
            NULL AS release_date,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
        FROM incarceration_cases ic
        WHERE state_code="US_MO"
        
        UNION ALL

        SELECT 
            ic.*,
            MAX(t.projected_completion_date_max) 
                    OVER(w) AS release_date,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
        FROM
            incarceration_cases ic
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_projected_completion_date_spans_materialized` t
          ON ic.person_id = t.person_id
              AND ic.state_code = t.state_code
              AND CURRENT_DATE('US/Eastern') 
                BETWEEN t.start_date AND {nonnull_end_date_exclusive_clause('t.end_date_exclusive')} 
        WHERE ic.state_code NOT IN ("US_AZ", "US_ME", "US_MO", "US_TN")
        WINDOW w as (PARTITION BY ic.person_id)

        UNION ALL

        # TODO(#33748): migrate this to the v2 projected end date view
        SELECT
            ic.*,
            sed_date AS release_date,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
        FROM
            incarceration_cases ic
        LEFT JOIN `{{project_id}}.workflows_views.us_az_resident_metadata_materialized` t
        USING
            (state_code, person_id)
        WHERE ic.state_code = "US_AZ"
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            full_name AS person_name,
            gender,
            facility_id,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
            CAST(NULL AS DATE) AS admission_date,
            CAST(NULL AS DATE) AS release_date
        FROM `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` ex
        JOIN `{{project_id}}.normalized_state.state_person` sp
            USING(state_code, person_id) 
        LEFT JOIN 
            --TODO(#42455): Pull facility_id from incarceration periods instead of this subquery when ingested
            (
            SELECT 
                commit_no AS person_external_id,
                'US_MA' AS state_code,
                CUR_INST AS facility_id
            FROM `{{project_id}}.{{us_ma_raw_data_up_to_date_dataset}}.egt_report_latest`
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_external_id ORDER BY RPT_RUN_DATE DESC, PERIOD_ENDING DESC) = 1
            )
            USING(state_code, person_external_id)
        WHERE state_code = 'US_MA'
            AND system_type = 'INCARCERATION'
        ),
"""

_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_CTE = f"""
    incarceration_cases_wdates AS (
        SELECT 
            * EXCEPT(release_date, admission_date),
            {revert_nonnull_start_date_clause('admission_date')} AS admission_date, 
            {revert_nonnull_end_date_clause('release_date')} AS release_date
        FROM incarceration_dates
        GROUP BY 1,2,3,4,5,6,7,8
    ),
"""

_RESIDENT_RECORD_CUSTODY_LEVEL_CTE = f"""
    custody_level AS (
        SELECT
            pei.person_id,
            UPPER(cs.CLIENT_SYS_DESC) AS custody_level,
        FROM `{{project_id}}.{{us_me_raw_data_dataset}}.CIS_112_CUSTODY_LEVEL` cl
        INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_1017_CLIENT_SYS_latest` cs
            ON cl.CIS_1017_CLIENT_SYS_CD = cs.CLIENT_SYS_CD
        INNER JOIN (
            SELECT *
            FROM `{{project_id}}.normalized_state.state_person_external_id`
            WHERE state_code = "US_ME" AND id_type = "US_ME_DOC"
        ) pei
        ON cl.CIS_100_CLIENT_ID = pei.external_id
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY CAST(LEFT(cl.CUSTODY_DATE, 19) AS DATETIME) DESC
        ) = 1
        
        UNION ALL
        
        SELECT 
            pei.person_id,
            BL_ICA AS custody_level
        FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK015_latest` tak015
        INNER JOIN (
            SELECT *
            FROM `{{project_id}}.normalized_state.state_person_external_id`
            WHERE state_code = "US_MO" AND id_type = "US_MO_DOC"
        ) pei
        ON BL_DOC = pei.external_id
        -- We want to keep the latest Custody Assessment date. When there are two assessments on the same day,
        -- we deduplicate using CNO which is part of the primary key. Finally, there's still a very small number of
        -- duplicates where the same person has the same BL_IC and BL_CNO, but different cycle numbers, so we further
        -- prioritize the latest cycle
        QUALIFY ROW_NUMBER() OVER(PARTITION BY BL_DOC ORDER BY
                                                        SAFE.PARSE_DATE('%Y%m%d', tak015.BL_IC) DESC,
                                                        tak015.BL_CNO DESC,
                                                        tak015.BL_CYC DESC) = 1
        UNION ALL

        SELECT
            person_id,
            custody_level,
        FROM `{{project_id}}.{{sessions_dataset}}.custody_level_sessions_materialized`
        WHERE state_code NOT IN ("US_ME", "US_MO")
        AND CURRENT_DATE('US/Eastern') 
            BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')} 
    ),
"""

_RESIDENT_RECORD_HOUSING_UNIT_CTE = f"""
    {current_bed_stay_cte()},
    housing_unit AS (
      SELECT
        person_id,
        CAST(NULL AS STRING) AS facility_id,
        housing_unit AS unit_id
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` 
      WHERE
          release_date IS NULL
          AND state_code NOT IN ('US_MO')
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY admission_date DESC) = 1

      UNION ALL

      SELECT
        person_id,
        CAST(NULL AS STRING) AS facility_id,
        IF(complex_number=building_number, complex_number, complex_number || " " || building_number) AS unit_id
      FROM current_bed_stay
    ),
"""

_RESIDENT_RECORD_OFFICER_ASSIGNMENTS_CTE = """
    officer_assignments AS (
        SELECT DISTINCT
            state_code,
            incarceration_staff_assignment_external_id as officer_id,
            person_id
        FROM `{project_id}.{sessions_dataset}.incarceration_staff_assignment_sessions_preprocessed_materialized`
        WHERE
            state_code IN ('US_ME', 'US_ND', 'US_AZ', 'US_IX')
            AND end_date_exclusive IS NULL
            AND relationship_priority = 1
    ),
"""

_RESIDENT_PORTION_NEEDED_CTE = """
    -- TODO(#30584): Once `dashboards` uses ME metadata field, this can be removed
    portion_needed AS (
      SELECT state_code, person_id,
      JSON_VALUE(reason, '$.x_portion_served')
          AS portion_served_needed,
      DATE(JSON_VALUE(reason, '$.eligible_date'))
          AS portion_needed_eligible_date,
      FROM `{project_id}.{us_me_task_eligibility_criteria_dataset}.served_x_portion_of_sentence_materialized`
          AS served_x
      WHERE CURRENT_DATE("US/Eastern")
      BETWEEN served_x.start_date AND IFNULL(DATE_SUB(served_x.end_date, INTERVAL 1 DAY), "9999-12-31")
    ),
"""

_RESIDENT_MONTHS_REMAINING_NEEDED_CTE = """
    -- TODO(#30584): Once `dashboards` uses ME metadata field, this can be removed
    months_remaining AS (
        SELECT state_code, person_id,
        DATE(JSON_VALUE(reason, '$.eligible_date'))
            AS months_remaining_eligible_date,
        FROM `{project_id}.{us_me_task_eligibility_criteria_dataset}.x_months_remaining_on_sentence_materialized` 
          AS months_remaining
      WHERE CURRENT_DATE("US/Eastern")
      BETWEEN months_remaining.start_date AND IFNULL(DATE_SUB(months_remaining.end_date, INTERVAL 1 DAY), "9999-12-31")
    ),
"""


def generate_resident_metadata_cte(states_with_metadata: List[StateCode]) -> str:
    """
    Given a list of state codes, generates a CTE that maps from person_id to
    json-formatted metadata blob when available.

    State-specific metadata is expected to live in `workflows_views.us_xx_resident_metadata`.

    This CTE will take all columns in the state-specific views for each state and pack them
    into a json blob in the `metadata` column. The state-specific views should only
    have one entry per person, but this CTE does some deduping to prevent run-time errors.

    When no metadata is found for a person, they do not have an entry in this CTE. Hence,
    we make sure to set a fallback for the metadata blob to `{}` when joining the
    rest of the resident record to this CTE.
    """
    dedup_clauses = ", ".join(
        f"""
        deduped_{state_code.value.lower()}_metadata AS (
            SELECT *
            FROM `{{project_id}}.{{workflows_dataset}}.{state_code.value.lower()}_resident_metadata_materialized`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id) = 1
        )
    """
        for state_code in states_with_metadata
    )

    metadata_to_json_clauses = "    UNION ALL\n".join(
        f"""
        SELECT
            ic.person_id,
            TO_JSON_STRING(
                (SELECT AS STRUCT m.* EXCEPT (person_id), "{state_code.value.upper()}" AS state_code
                    FROM deduped_{state_code.value.lower()}_metadata m
                    WHERE ic.person_id = m.person_id)
            ) as metadata,
        FROM incarceration_cases_wdates ic
        INNER JOIN  deduped_{state_code.value.lower()}_metadata
        USING (person_id)
        WHERE ic.state_code = "{state_code.value.upper()}"
    """
        for state_code in states_with_metadata
    )

    return f"""
    metadata AS (
        WITH {dedup_clauses}

        {metadata_to_json_clauses}
    ),
    """


_RESIDENT_RECORD_JOIN_RESIDENTS_CTE = """
    join_residents AS (
        SELECT DISTINCT
            ic.state_code,
            ic.person_name,
            ic.person_id,
            ic.gender,
            officer_id,
            ic.facility_id,
            unit_id,
            custody_level.custody_level,
            ic.admission_date,
            ic.release_date,
            ic.us_tn_facility_admission_date,
            COALESCE(m.metadata, '{{}}') as metadata,
        FROM
            incarceration_cases_wdates ic
        LEFT JOIN custody_level
            USING(person_id)
        LEFT JOIN housing_unit hu
          USING(person_id)
        LEFT JOIN metadata m
          USING(person_id)
        LEFT JOIN officer_assignments
          USING(state_code, person_id)
    ),
"""

_RESIDENTS_CTE = """
    residents AS (
        SELECT
            stable_person_external_ids.person_external_id,
            # TODO(#41556): Update frontend to reference display_person_external_id column
            #  and delete the ambiguously-named display_id column.
            display_person_external_ids.display_person_external_id AS display_id,
            display_person_external_ids.display_person_external_id,
            display_person_external_ids.display_person_external_id_type,
            state_code,
            person_name,
            person_id,
            gender,
            officer_id,
            facility_id,
            unit_id,
            CONCAT(facility_id, "-_-", IFNULL(unit_id, "")) as facility_unit_id,
            custody_level,
            admission_date,
            release_date,
            metadata,
            us_tn_facility_admission_date,
            opportunities_aggregated.all_eligible_opportunities,
            portion_served_needed,
            portion_needed_eligible_date AS us_me_portion_needed_eligible_date,
            GREATEST(portion_needed_eligible_date, months_remaining_eligible_date) AS sccp_eligibility_date,
        FROM join_residents
        LEFT JOIN stable_person_external_ids
            USING (person_id)
        LEFT JOIN opportunities_aggregated USING (state_code, person_id)
        LEFT JOIN portion_needed USING (state_code, person_id)
        LEFT JOIN months_remaining USING (state_code, person_id)
        LEFT JOIN (
            SELECT state_code, person_id, display_person_external_id, display_person_external_id_type
            FROM `{project_id}.reference_views.product_display_person_external_ids_materialized`
            WHERE system_type = "INCARCERATION"
        ) display_person_external_ids        
            USING (state_code, person_id)
    )
"""


def full_resident_record() -> str:
    stable_person_external_ids_cte = StrictStringFormatter().format(
        CLIENT_OR_RESIDENT_RECORD_STABLE_PERSON_EXTERNAL_IDS_CTE_TEMPLATE,
        system_type="INCARCERATION",
    )
    return f"""
    {_RESIDENT_RECORD_INCARCERATION_CTE}
    {_RESIDENT_RECORD_INCARCERATION_DATES_CTE}
    {_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_CTE}
    {_RESIDENT_RECORD_CUSTODY_LEVEL_CTE}
    {_RESIDENT_RECORD_HOUSING_UNIT_CTE}
    {_RESIDENT_RECORD_OFFICER_ASSIGNMENTS_CTE}
    {_RESIDENT_PORTION_NEEDED_CTE}
    {_RESIDENT_MONTHS_REMAINING_NEEDED_CTE}
    {generate_resident_metadata_cte(STATES_WITH_RESIDENT_METADATA)}
    {_RESIDENT_RECORD_JOIN_RESIDENTS_CTE}
    {stable_person_external_ids_cte}
    {_RESIDENTS_CTE}
    """
