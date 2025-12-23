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
"""Helper SQL fragments that import raw tables for AZ
"""
from typing import Optional

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)


def no_current_or_prior_convictions(
    statutes_list: Optional[list] = None,
    exclude_statutes: bool = False,
    additional_where_clauses: Optional[str] = None,
    reasons_field_name: str = "ineligible_offenses",
    past_convictions_cause_ineligibility: bool = True,
) -> str:
    """
    Returns a query template that describes spans of time when someone is ineligible due to a current or
    past conviction for a specific offense.

    Args:
        statutes_list (list): The statute(s) to be included in the where clause
        additional_where_clauses (str): Any additional logic not captured by a statutes filter
        exclude_statutes (bool): If True, the statutes in statutes_list will be excluded from the query.
            This means they will be not be marked ineligible and their eligibility will be
            determined by the meets_criteria_default clause in the view builder.
        reasons_field_name (str): The name of the field in the output that contains the reasons
        past_convictions_cause_ineligibility (bool): If True, past convictions will cause
            someone to become ineligible forever. If False, only convictions when
            served will cause ineligibility.
    """
    if additional_where_clauses:
        if not (
            additional_where_clauses.startswith("AND")
            or additional_where_clauses.startswith("OR")
        ):
            raise ValueError(
                "additional_where_clauses must start with 'AND' or 'OR' to ensure proper SQL syntax"
            )
    # If statutes_list is None, we will not filter on statutes
    if statutes_list is None:
        statutes_list = []
    assert isinstance(statutes_list, list), "statutes_list must be of type list"
    # If exclude_statutes_list is True, we will exclude the statutes in the list
    not_clause = ""
    if exclude_statutes:
        not_clause = "NOT"
    # If neither statutes_list nor additional_where_clause are provided, raise an error
    if not statutes_list and not additional_where_clauses:
        raise ValueError(
            "Either 'statutes_list' or 'additional_where_clause' must be provided."
        )
    # If no_past_convictions is True, we will only look at current convictions
    if past_convictions_cause_ineligibility:
        end_date = "CAST(NULL AS DATE)"
    else:
        end_date = "span.end_date"

    return f"""
    WITH
      ineligible_spans AS (
          SELECT
            span.state_code,
            span.person_id,
            span.start_date,
            {end_date} AS end_date,
            charge.description,
            FALSE AS meets_criteria,
          FROM
            `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
            UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
          INNER JOIN
            `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
          USING
            (state_code,
              person_id,
              sentences_preprocessed_id)
          LEFT JOIN
              `{{project_id}}.{{normalized_state_dataset}}.state_charge_v2_state_sentence_association` assoc
            ON
              assoc.state_code = sent.state_code
              AND assoc.sentence_id = sent.sentence_id
            LEFT JOIN
              `{{project_id}}.{{sessions_dataset}}.charges_preprocessed` charge
            ON
              charge.state_code = assoc.state_code
              AND charge.charge_v2_id = assoc.charge_v2_id
          WHERE
            span.state_code = 'US_AZ'
            -- Statutes filter
            {f"AND {not_clause} (" + " OR ".join([f"charge.statute LIKE '%{s}%'" for s in statutes_list]) + ")" if statutes_list else ""}
            -- Additional where clauses
            {f"{additional_where_clauses}" if additional_where_clauses else ""}),

    {create_sub_sessions_with_attributes('ineligible_spans')}

    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT( ARRAY_AGG(DISTINCT description ORDER BY description) AS {reasons_field_name})) AS reason,
        ARRAY_AGG(DISTINCT description ORDER BY description) AS {reasons_field_name},
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
    """


def early_release_completion_event_query_template(
    release_type: str, release_is_overdue: bool
) -> str:
    """Return the query template used for AZ early release completion events"""
    if release_type not in ("TPR", "DTP"):
        raise NotImplementedError(
            f"Unsupported release_type |{release_type}|, expecting TPR or DTP"
        )
    if release_is_overdue:
        release_date_condition = "eligible_release_date < release_date"
    else:
        release_date_condition = (
            f"release_date <= {nonnull_end_date_clause('eligible_release_date')}"
        )
    return f"""
SELECT
    state_code,
    person_id,
    release_date AS completion_event_date,
FROM
    `{{project_id}}.analyst_data.us_az_early_releases_from_incarceration_materialized`
WHERE release_type = "{release_type}"
    AND {release_date_condition}
"""


def us_az_sentences_preprocessed_query_template() -> str:
    """Returns the query template used for AZ sentences preprocessed"""
    return """
    SELECT 
        sent.state_code,
        sent.person_id,
        sent.sentence_group_external_id,
        serving_sessions.start_date,
        serving_sessions.end_date_exclusive AS end_date,
        sent.statute,
        sent.description,
    FROM `{project_id}.sentence_sessions.sentence_serving_period_materialized` serving_sessions
    INNER JOIN `{project_id}.sentence_sessions.sentences_and_charges_materialized` sent
        USING (state_code, person_id, sentence_id)
    WHERE sentence_type = 'STATE_PRISON'
        AND sent.state_code = 'US_AZ'
"""


def home_plan_information_for_side_panel_notes() -> str:
    return """
    SELECT
        -- US_AZ_PERSON_ID
        hp.person_id AS external_id,
        "Home Plan Information" AS criteria,
        plan_status AS note_title,
        IF(is_homeless_request = 'Y', "Request to release as homeless", "") AS note_body,
        -- Capture the date of the latest change
        SAFE_CAST(start_date AS DATE) AS event_date,
    FROM `{project_id}.analyst_data.us_az_home_plan_preprocessed_materialized` hp
    WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date, '9999-12-31')"""


def functional_literacy_enrollment_side_panel_notes(opp_name: str) -> str:
    assert opp_name.upper() in ("TPR", "DTP"), "Opportunity Name must be one of TPR/DTP"
    return f"""
    SELECT
        external_id,
        criteria,
        note_title,
        note_body,
        event_date,
    FROM 
        (
        WITH union_cte AS (
            SELECT
                peid.external_id,
                "Mandatory Literacy Enrollment Information" AS criteria,
                "Currently Enrolled" AS note_title,
                " " AS note_body,
                start_date AS event_date,
            FROM
            #TODO(#33858): Ingest into state task deadline or find some way to view this historically
              `{{project_id}}.normalized_state.state_program_assignment` spa
            LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
            ON peid.person_id = spa.person_id
                AND peid.state_code = 'US_AZ'
                AND peid.id_type = 'US_AZ_PERSON_ID'
            WHERE participation_status = "IN_PROGRESS"
            AND program_id LIKE '%FUNCTIONAL LITERACY%' 
            AND discharge_date IS NULL
            #TODO(#33737): Look into multiple span cases for residents participating in MAN-LIT programs
            QUALIFY ROW_NUMBER() OVER (PARTITION BY peid.external_id ORDER BY start_date ASC) = 1
            
            UNION ALL 
            
            SELECT
                peid.external_id,
                "Mandatory Literacy Enrollment Information" AS criteria,
                "Completed" AS note_title,
                " " AS note_body,
                start_date AS event_date,
            FROM ({meets_mandatory_literacy(opp_name)} ) comp
            LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
            ON peid.person_id = comp.person_id
                AND peid.state_code = 'US_AZ'
                AND peid.id_type = 'US_AZ_PERSON_ID'
            WHERE CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
            -- Adding this in to take the most recent case, in the odd case where current date = start/end date
            QUALIFY ROW_NUMBER() OVER (PARTITION BY peid.external_id ORDER BY start_date) = 1
            )
            # We create an ordering so that in any case where someone has both enrollment & completion exist, it always
            # prioritizes the row indicating completion of mandatory literacy
            SELECT
                *
            FROM
                (SELECT
                    union_cte.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY CASE
                            WHEN note_title = "Completed" THEN 1
                            ELSE 2 # WHEN note_title = "Currently Enrolled"
                            END
                        ) AS rn,
                    FROM union_cte
                )
            WHERE rn = 1
        )
    """


def agreement_form_status_side_panel_notes(opp_name: str) -> str:
    if opp_name.upper() not in ("TPR", "DTP"):
        raise NotImplementedError(
            f"Unsupported release_type |{opp_name}|, expecting TPR or DTP"
        )
    return f"""
    SELECT
      DISTINCT 
      PERSON_ID AS external_id,
      "Agreement Form Signature Status" AS criteria,
      CASE WHEN (SIG_STATUS = 'SIGNED')
            THEN 'Signed'
        -- Cutoff value for a resident to reconsider signing the agreement if they are eligible for early release
        WHEN (SIG_STATUS = 'NOT SIGNED, NOT DECLINED') AND  DATE_DIFF(CURRENT_DATE('EST'),start_date, DAY) >= 180
            THEN 'Not Signed' 
        WHEN (SIG_STATUS = 'NOT SIGNED, NOT DECLINED') AND  DATE_DIFF(CURRENT_DATE('EST'),start_date, DAY) < 180
            THEN 'Not Signed' 
        WHEN (SIG_STATUS = 'REFUSED TO SIGN')
            THEN 'Refusal to Sign'
        WHEN (SIG_STATUS = 'DECLINED TO PARTICIPATE')
            THEN 'Declined 180+ Days Ago'
      END AS note_title,
      "" as note_body,
      start_date AS event_date
    FROM
      `{{project_id}}.analyst_data.us_az_agreement_form_signatures_materialized`
    WHERE end_date_exclusive IS NULL 
        AND program = '{opp_name.upper()}'
    """


def acis_date_not_set_criteria_builder(
    criteria_name: str, description: str, task_subtype: str
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a criteria builder for the ACIS TPR/DTP date not set criteria

    Args:
        criteria_name (str): The name of the criteria
        description (str): The description of the criteria
        task_subtype (str): The task subtype to filter on. Could be 'STANDARD TRANSITION RELEASE'
            or 'DRUG TRANSITION RELEASE'

    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: The criteria builder"""

    assert task_subtype in [
        "STANDARD TRANSITION RELEASE",
        "DRUG TRANSITION RELEASE",
    ], "task_subtype must be 'STANDARD TRANSITION RELEASE' or 'DRUG TRANSITION RELEASE'"
    if task_subtype == "STANDARD TRANSITION RELEASE":
        task = "TPR"
        acis_date = "acis_tpr_date"
    else:
        task = "DTP"
        acis_date = "acis_dtp_date"

    _REASONS_FIELDS = [
        ReasonsField(
            name=f"{task.lower()}_latest_acis_update_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description=f"{task}: Most recent datetime ACIS date was set",
        ),
        ReasonsField(
            name=f"{task.lower()}_latest_acis_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description=f"{task}: Most recent ACIS date",
        ),
    ]
    _TBL = "`{project_id}.{analyst_data}.us_az_projected_dates_materialized`"
    _QUERY_TEMPLATE = f"""
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      FALSE AS meets_criteria,
      TO_JSON(STRUCT(
            {acis_date} AS {task.lower()}_latest_acis_date,
            start_date AS {task.lower()}_latest_acis_update_date
        )) AS reason,
     start_date AS {task.lower()}_latest_acis_update_date,
     {acis_date} AS {task.lower()}_latest_acis_date,
    FROM
      # We want to aggregate over our projected dates view to focus aggregation over the ACIS date rather than all dates
      # as is done in the original view
      ({aggregate_adjacent_spans(_TBL, attribute=acis_date)})
    WHERE {acis_date} IS NOT NULL
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_AZ,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=_REASONS_FIELDS,
        analyst_data=ANALYST_VIEWS_DATASET,
    )


def almost_eligible_tab_logic(opp_name: str) -> str:
    """Returns the Almost Eligible logic reused for TPR & DTP Opportunities"""
    assert opp_name.upper() in ["TPR", "DTP"], "Opportunity Name must be TPR or DTP"
    return f"""
    -- Fast track: ACIS TPR/DTP date within 1 days and 30 days
    -- Approved by Time Comp (Eligible Now): ACIS TPR/DTP date within 30 to 180 days
    SELECT
        * EXCEPT(criteria_reason),
        CASE
            WHEN SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_{opp_name.lower()}_date') AS DATE)
                    < DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY)
                THEN "FAST_TRACK"
            ELSE "APPROVED_BY_TIME_COMP"
        END AS metadata_tab_description,
    CASE
            WHEN SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_{opp_name.lower()}_date') AS DATE)
                    < DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY)
                THEN "FAST_TRACK"
            ELSE "APPROVED_BY_TIME_COMP"
        END AS metadata_tab_name,
    FROM acis_{opp_name.lower()}_date_approaching,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    WHERE SAFE_CAST(JSON_VALUE(criteria_reason, '$.criteria_name') AS STRING) = "US_AZ_INCARCERATION_WITHIN_6_MONTHS_OF_ACIS_{opp_name.upper()}_DATE"
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_{opp_name.lower()}_date') AS DATE) BETWEEN 
            DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY) AND DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 180 DAY)

    UNION ALL

    -- Almost eligible section 1: Projected {opp_name.lower()} date within 7-180 days
    -- Almost eligible section 1: Projected {opp_name.lower()} date within 7-180 days AND missing man lit
    -- Almost eligible section 2: Projected {opp_name.lower()} date within 181-365 days AND missing at most one other criteria
    -- (functional literacy XOR no felony detainers)
    # TODO(#33958) - recidiviz_xxx_date_approaching needs to be split into section 1 and 2
    SELECT 
        * EXCEPT(criteria_reason),
        CASE
            -- Tab 1: Upcoming {opp_name} date in the next 7-180 days
            WHEN
                SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.recidiviz_{opp_name.lower()}_date') AS DATE)
                    <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 180 DAY)
                THEN CASE
                    -- Missing mandatory literacy (check criteria names for DTP & TPR)
                    WHEN "US_AZ_ENROLLED_IN_OR_MEETS_MANDATORY_LITERACY_{opp_name.upper()}" IN UNNEST(ineligible_criteria)

                        THEN "ALMOST_ELIGIBLE_MISSING_MANLIT_BETWEEN_7_AND_180_DAYS"
                    -- Only missing the date requirement by 7-180 days
                    ELSE "ALMOST_ELIGIBLE_BETWEEN_7_AND_180_DAYS"
                END
            -- Tab 2: {opp_name} date in the next 181-365 days
            WHEN ARRAY_LENGTH(ineligible_criteria) = 1
                -- Only missing the date requirements by 181-365 days
                THEN "ALMOST_ELIGIBLE_BETWEEN_181_AND_365_DAYS"
            ELSE "ALMOST_ELIGIBLE_MISSING_CRITERIA_AND_BETWEEN_181_AND_365_DAYS"
        END AS metadata_tab_description,
        IF(
            -- If the projected {opp_name} date is within 180 days: Almost Eligible Tab 1
            SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.recidiviz_{opp_name.lower()}_date') AS DATE)
                <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 180 DAY),
            "ALMOST_ELIGIBLE_1",
            -- Else: Almost Eligible Tab 2 (181-365 days)
            "ALMOST_ELIGIBLE_2"
        ) AS metadata_tab_name,
    FROM recidiviz_{opp_name.lower()}_date_approaching,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    WHERE
        -- Pull out the criteria that has the projected {opp_name} date in the reasons field
        JSON_VALUE(criteria_reason, '$.criteria_name') = "US_AZ_WITHIN_7_DAYS_OF_RECIDIVIZ_{opp_name.upper()}_DATE"
        -- TODO(#42817): move this logic to TES almost eligible conditions once almost eligible refactor is completed
        -- Clients with an active detainer must have more than 180 days until their {opp_name} date to be displayed
        -- in the tool
        AND (
            "US_AZ_NO_ACTIVE_FELONY_DETAINERS" NOT IN UNNEST(ineligible_criteria)
            OR SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.recidiviz_{opp_name.lower()}_date') AS DATE)
                    > DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 180 DAY)
        )
    
    UNION ALL
    
    -- Add in the Overdue for ACIS group
    SELECT
        *,
        "OVERDUE" AS metadata_tab_description,
        "OVERDUE" AS metadata_tab_name,
    FROM 
        acis_{opp_name.lower()}_date_overdue
    """


def within_x_time_of_date(
    opp_name: str, time_interval: int, date_part: str = "MONTH"
) -> str:
    """Returns a query finding if the chosen opportunity date is within x time (defaults to x months)"""
    assert opp_name.upper() in ["TPR", "DTP"], "Opportunity Name must be TPR or DTP"
    return f"""
        WITH critical_date_spans AS (
            SELECT
                state_code,
                person_id,
                start_date AS start_datetime,
                end_date AS end_datetime,
                projected_{opp_name.lower()}_date AS critical_date
            FROM `{{project_id}}.{{analyst_views_dataset}}.us_az_projected_dates_materialized`
            ),
        {critical_date_has_passed_spans_cte(
        meets_criteria_leading_window_time=time_interval,
        date_part=date_part,
    )}
        SELECT
            cd.state_code,
            cd.person_id,
            cd.start_date,
            cd.end_date,
            cd.critical_date_has_passed AS meets_criteria,
            TO_JSON(STRUCT(
                cd.critical_date AS recidiviz_{opp_name.lower()}_date
            )) AS reason,
            cd.critical_date AS recidiviz_{opp_name.lower()}_date,
        FROM critical_date_has_passed_spans cd
        """


def meets_mandatory_literacy(opp_name: str) -> str:
    """Returns spans of time during which someone has either completed or been exempted
    from completing a mandatory literacy program.

    Exemptions from mandatory literacy only apply during the specific incarceration period
    (DOC_ID) where they were granted. Actual completions (program completions and TABE passes)
    apply in perpetuity across all future incarcerations.
    """
    # TODO(#53389): Move this logic to ingest.
    assert opp_name.upper() in ("TPR", "DTP"), "Opportunity Name must be one of TPR/DTP"
    if opp_name.upper() == "TPR":
        _TABLE = "AZ_DOC_TRANSITION_PRG_EVAL"
        _ELIG_TABLE = "AZ_DOC_TRANSITION_PRG_ELIG"
        _ID_MAP = "TRANSITION_PRG_ELIGIBILITY_ID"
    else:
        _TABLE = "AZ_DOC_DRUG_TRANSITION_PRG_EVAL"
        _ELIG_TABLE = "AZ_DOC_DRUG_TRAN_PRG_ELIG"
        _ID_MAP = "DRUG_TRAN_PRG_ELIGIBILITY_ID"
    return f"""
    WITH 
    /*
    INCARCERATION PERIODS: Used to bound exemption spans to specific incarceration sessions.
    */
    sentence_serving_period AS (
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
        FROM `{{project_id}}.sentence_sessions.sentence_serving_period_materialized` ss
        WHERE state_code = 'US_AZ'
    ),
    
    /*
    PROGRAM ASSIGNMENTS - ACTUAL COMPLETIONS (not exemptions)
    These are people who successfully completed the functional literacy program.
    Actual completions apply in perpetuity (end_date = NULL), so they meet the
    standard in all future incarcerations.
    */ 
    manlit_ingest_completed AS (
        SELECT
          state_code,
          person_id,
          discharge_date as start_date,
          CAST(NULL AS DATE) AS end_date,  -- NULL = applies forever
          TRUE AS meets_criteria,
          discharge_date AS latest_functional_literacy_date,
          'Program Assignment - Completed' AS data_location
        #TODO(#33858): Ingest into state task deadline or find some way to view this historically
        FROM
          `{{project_id}}.normalized_state.state_program_assignment`
        WHERE state_code = 'US_AZ'
      AND participation_status = 'DISCHARGED_SUCCESSFUL'
        AND program_id LIKE '%LITERACY%'
        -- -- Filter out exemptions: those have the EXEMPTION field populated
        AND JSON_EXTRACT(referral_metadata, '$.EXEMPTION') = '""' 
        #TODO(#33737): Look into multiple span cases for residents who have completed in MAN-LIT programs
        QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY start_date ASC) = 1
    ),
    /*
    TABE QUALIFICATION - ACTUAL TEST PASSES
    TABE (Tests of Adult Basic Education) assesses reading, language, and math skills.
    To meet mandatory literacy via TABE, a person must score grade level 8 or higher
    on ALL THREE sections. These are actual test passes (no exemptions possible).
    Once passed, this applies in perpetuity (end_date = NULL).
    */
    tabe_qualification AS (
        WITH
        -- Find the earliest date each person passed each section
        section_earliest_pass_date AS (
            SELECT
                person_id,
                MIN(CASE WHEN passed_reading_flag = 1 THEN assessment_date ELSE NULL END) AS earliest_reading_pass_date,
                MIN(CASE WHEN passed_language_flag = 1 THEN assessment_date ELSE NULL END) AS earliest_language_pass_date,
                MIN(CASE WHEN passed_math_flag = 1 THEN assessment_date ELSE NULL END) AS earliest_math_pass_date
            FROM (
                SELECT
                    sa.person_id,
                    sa.assessment_date,
                    -- Flag if score >= grade level 8 (passing) for each section
                    CASE WHEN SAFE_CAST(JSON_EXTRACT_SCALAR(assessment_metadata, '$.READING_SCORE') AS NUMERIC) >= 8 THEN 1 ELSE 0 END AS passed_reading_flag,
                    CASE WHEN SAFE_CAST(JSON_EXTRACT_SCALAR(assessment_metadata, '$.LANGUAGE_SCORE') AS NUMERIC) >= 8 THEN 1 ELSE 0 END AS passed_language_flag,
                    CASE WHEN SAFE_CAST(JSON_EXTRACT_SCALAR(assessment_metadata, '$.MATH_SCORE') AS NUMERIC) >= 8 THEN 1 ELSE 0 END AS passed_math_flag
                FROM
                    `{{project_id}}.normalized_state.state_assessment` sa
                WHERE
                    state_code = 'US_AZ'
                    AND sa.assessment_class = 'EDUCATION'
                    AND sa.assessment_type = 'TABE'
            )
            WHERE
                -- Only consider rows where at least one section was passed
                passed_reading_flag = 1 
                OR passed_language_flag = 1 
                OR passed_math_flag = 1
            GROUP BY
                person_id
        )
        SELECT
            "US_AZ" AS state_code,
            person_id, 
            overall_all_sections_passed_date AS start_date,
            CAST(NULL AS DATE) AS end_date,  -- NULL = applies forever
            TRUE AS meets_criteria,
            overall_all_sections_passed_date AS latest_functional_literacy_date,
            'DOC_EDUCATION_TABE' AS data_location,
        FROM (
            SELECT
            person_id,
            -- The date when ALL sections were passed is the LATEST of the three earliest pass dates
            -- (e.g., if Reading passed in Jan, Language in Feb, Math in March, they met the
            -- requirement in March when all three were complete)
            GREATEST(earliest_reading_pass_date, earliest_language_pass_date, earliest_math_pass_date) AS overall_all_sections_passed_date
            FROM
                section_earliest_pass_date
            WHERE
                -- All three sections must have been passed at some point
                earliest_reading_pass_date IS NOT NULL
                AND earliest_language_pass_date IS NOT NULL
                AND earliest_math_pass_date IS NOT NULL
            GROUP BY
                1,2
        )
    ),
    /*
    PROGRAM ASSIGNMENTS - EXEMPTIONS
    These are people who received an exemption from the literacy requirement.
    These exemptions are tied to a specific incarceration session and should end when 
    the session ends.
    */
    manlit_ingest_exemption AS (
         SELECT DISTINCT
          pa.state_code,
          pa.person_id,
          discharge_date as start_date,
          ssp.end_date_exclusive as end_date,
          TRUE AS meets_criteria,
          discharge_date AS latest_functional_literacy_date,
          'Program Assignment - Exemption' AS data_location
        #TODO(#33858): Ingest into state task deadline or find some way to view this historically
        FROM
          `{{project_id}}.normalized_state.state_program_assignment` pa
        JOIN sentence_serving_period ssp
        ON(pa.person_id = ssp.person_id and pa.state_code = ssp.state_code 
        -- Use the end date of the incarceration span being served when this exemption was introduced
        AND pa.start_date BETWEEN ssp.start_date and {nonnull_end_date_exclusive_clause("ssp.end_date_exclusive")})
        WHERE pa.state_code = 'US_AZ'
        AND program_id LIKE '%LITERACY%'
        AND JSON_EXTRACT(referral_metadata, '$.EXEMPTION') != '""' 
        -- Keep the earliest exemption for each DOC_ID where one exists
        QUALIFY ROW_NUMBER() OVER (PARTITION BY pa.state_code, pa.person_id ORDER BY start_date ASC) = 1
        ),
    /*
    ACIS PROGRAM EVALUATION - EXEMPTIONS
    When LITERACY_EXCEPTION = 'Y', the person has an exemption. 
    These exemptions are tied to a specific incarceration session and should end when 
    the session ends.
    */
    manlit_prg_eval_exemption AS (
        SELECT
          pei.state_code,
          pei.person_id,
          PARSE_DATE('%m/%d/%Y', SPLIT(eval.CREATE_DTM, ' ')[OFFSET(0)]) AS exemption_date,
          ssp.end_date_exclusive,
          TRUE AS meets_criteria,
          PARSE_DATE('%m/%d/%Y', SPLIT(eval.CREATE_DTM, ' ')[OFFSET(0)]) AS latest_functional_literacy_date,
          'Program Evaluation - Exemption' AS data_location
        FROM
          `{{project_id}}.us_az_raw_data_up_to_date_views.{_TABLE}_latest` eval
        INNER JOIN 
        `{{project_id}}.us_az_raw_data_up_to_date_views.{_ELIG_TABLE}_latest` map_to_docid
        USING ({_ID_MAP})
        LEFT JOIN `{{project_id}}.us_az_raw_data_up_to_date_views.DOC_EPISODE_latest` doc_ep
        USING(DOC_ID)
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            ON doc_ep.PERSON_ID = pei.external_id
            AND pei.state_code = 'US_AZ'
            AND pei.id_type = 'US_AZ_PERSON_ID'
        JOIN sentence_serving_period ssp
            ON(pei.person_id = ssp.person_id and pei.state_code = ssp.state_code 
            -- Use the end date of the incarceration span being served when this exemption was introduced
            AND  PARSE_DATE('%m/%d/%Y', SPLIT(eval.CREATE_DTM, ' ')[OFFSET(0)]) BETWEEN ssp.start_date and {nonnull_end_date_exclusive_clause("ssp.end_date_exclusive")})
        WHERE LITERACY_EXCEPTION = 'Y'
        -- Take earliest exemption per person per DOC_ID
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pei.state_code, pei.person_id, map_to_docid.DOC_ID 
            ORDER BY exemption_date ASC
        ) = 1
    ),   /*
    DOC PRIORITY REPORT - EXEMPTIONS
    This report tracks when people meet the literacy standard. However, some rows indicate
    meeting the standard via exemption (e.g. MEET_STANDARD = 'Yes' and LITERACY_STANDARD = 'None')
    We filter to only those exemptions. These apply until the end of the incarceration session.
    */
    priority_report_cleaned AS (
        SELECT DISTINCT 
            DOC_ID, 
            MEET_STANDARD, 
            LITERACY_STANDARD, 
            -- This value updates daily, so the latest view of the Priority Report table
            -- tends to have today as the DATE_CREATED. Use the full table and find the 
            -- first time a status appeared and the most recent time it appeared.
            MIN(DATE(DATE_CREATED)) as DATE_CREATED, 
            MAX(DATE(DATE_CREATED)) AS DATE_SEEN
        FROM `{{project_id}}.us_az_raw_data_views.DOC_PRIORITY_REPORT_all` priority_report 
        WHERE PROGRAM_TYPE_ID = '1' -- 'Academic Education'  
        GROUP BY 1,2,3
        -- Only keep the most recent set of values per DOC_ID (incarceration stint)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY DOC_ID ORDER BY DATE_SEEN DESC) = 1
    ),
    manlit_priority_report_exemption AS (
        SELECT
            pei.state_code,
            pei.person_id, 
            -- Use the earliest date someone was marked as meeting the standard
            DATE_CREATED AS start_date,
            ssp.end_date_exclusive AS end_date,  -- NULL = applies forever
            TRUE AS meets_criteria,
            DATE_CREATED AS latest_functional_literacy_date,
            'PRIORITY_REPORT - Exemption' AS data_location,
        FROM
            priority_report_cleaned
        LEFT JOIN `{{project_id}}.us_az_raw_data_up_to_date_views.DOC_EPISODE_latest` doc_ep
            USING(DOC_ID)
        LEFT JOIN `{{project_id}}.us_az_normalized_state.state_person_external_id` pei
            ON doc_ep.PERSON_ID = pei.external_id 
            AND pei.state_code = 'US_AZ'
            AND pei.id_type = 'US_AZ_PERSON_ID'
        JOIN sentence_serving_period ssp
            ON(pei.person_id = ssp.person_id and pei.state_code = ssp.state_code 
            -- Use the end date of the incarceration span being served when this exemption was introduced
            AND (priority_report_cleaned.DATE_CREATED BETWEEN ssp.start_date and {nonnull_end_date_exclusive_clause("ssp.end_date_exclusive")}))
        WHERE 
        -- Filter to exemption-based "passes": people marked as meeting the standard
        -- when they don't have a literacy standard.
            (MEET_STANDARD = 'Yes' AND LITERACY_STANDARD = 'None')
    ),
    /*
    UNION ALL DATA SOURCES
    Combine all literacy completion sources:
    - Actual completions (end_date = NULL, applies in perpetuity)
    - Exemptions (end_date = incarceration release date, limited to specific DOC_ID)
    */
    union_cte AS (
        -- ACTUAL COMPLETIONS: apply forever (end_date = NULL)
        SELECT * FROM manlit_ingest_completed 
        UNION ALL
        SELECT * FROM tabe_qualification
        UNION ALL
        -- EXEMPTIONS: limited to specific incarceration period (end_date set)
        SELECT * FROM manlit_ingest_exemption
        UNION ALL
        SELECT * FROM manlit_prg_eval_exemption
        UNION ALL 
        SELECT * FROM manlit_priority_report_exemption
    ),
    {create_sub_sessions_with_attributes('union_cte')}
    /*
    FINAL OUTPUT
    Create sub-sessions from overlapping spans and deduplicate to keep the latest
    literacy date and data source for each unique span.
    */
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,  -- NULL for actual completions (perpetual), date for exemptions (limited)
        meets_criteria,
        TO_JSON(STRUCT(
                latest_functional_literacy_date AS latest_functional_literacy_date,
                data_location AS latest_data_location
            )) AS reason,
        latest_functional_literacy_date AS latest_functional_literacy_date,
        data_location AS latest_data_location,
    FROM
        sub_sessions_with_attributes
    WHERE start_date IS DISTINCT FROM end_date
    -- For overlapping spans, keep the one with the most recent literacy date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id, start_date, meets_criteria ORDER BY latest_functional_literacy_date DESC) = 1
    """


def enrolled_in_mandatory_literacy(enrollment_meets_criteria: bool) -> str:
    """Returns spans of time someone is enrolled in a mandatory literacy program

    enrollment_meets_criteria: True if enrollment in mandatory literacy meets eligibility requirements, False otherwise
    """
    assert isinstance(enrollment_meets_criteria, bool), "Please input True or False"
    return f"""
    SELECT
      state_code,
      person_id,
      start_date,
      COALESCE(discharge_date, CAST(NULL AS DATE)) AS end_date,
      {enrollment_meets_criteria} AS meets_criteria,
      TO_JSON(STRUCT(
                start_date AS enrollment_date
            )) AS reason,
      start_date AS enrollment_date,
    FROM
    #TODO(#33858): Ingest into state task deadline or find some way to view this historically
      `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment`
    WHERE state_code = 'US_AZ'
    AND participation_status_raw_text IN ('PARTICIPATING')
    AND program_id LIKE '%MAN%LIT%'
    # Fixing the issue of zero-day spans while still keeping all participating individuals
    # This also takes care of cases when the start_date happens after the discharge_date
    # TODO(#34798): Remove erroneous cases where start_date > discharge_date
    AND start_date < {nonnull_end_date_clause('discharge_date')}  
    #TODO(#33737): Look into multiple span cases for residents participating in MAN-LIT programs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY start_date ASC) = 1
    """


def no_denial_in_current_incarceration(opp_name: str) -> str:
    assert opp_name.upper() in ("TPR", "DTP"), "Opportunity Name must be one of TPR/DTP"
    if opp_name.upper() == "TPR":
        _TABLE = "AZ_DOC_TRANSITION_PRG_REVIEW"
        _ELIG_TABLE = "AZ_DOC_TRANSITION_PRG_ELIG"
        _ID_MAP = "TRANSITION_PRG_ELIGIBILITY_ID"
        task_subtype = "STANDARD TRANSITION RELEASE"
    else:
        _TABLE = "AZ_DOC_DRUG_TRAN_PRG_REVIEW"
        _ELIG_TABLE = "AZ_DOC_DRUG_TRAN_PRG_ELIG"
        _ID_MAP = "DRUG_TRAN_PRG_ELIGIBILITY_ID"
        task_subtype = "DRUG TRANSITION RELEASE"
    return f"""
        WITH denials AS (
            SELECT 
                peid.state_code,
                peid.person_id,
                DATE_ADD(IFNULL(
                SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', dtp.CREATE_DTM) AS DATE),
                SAFE_CAST(LEFT(dtp.CREATE_DTM, 10) AS DATE)), INTERVAL 1 DAY)
                AS start_date,
                denial.description AS denied_reason,
                dtp.DENIED_OTHER_COMMENT AS denied_comment,
                -- Setting dummy values for the following which, when using the variables from the second section of 
                --the union to filter in a later CTE, will not affect the output of this first section
                'BYPASS' as task_metadata,
                CAST('9999-12-31' AS DATE) as update_datetime,
                CAST('9999-12-31' AS DATE) as eligible_date,
            FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.{_TABLE}_latest` dtp
            LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.{_ELIG_TABLE}_latest` tpe
                USING({_ID_MAP})
            LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` doc_ep
                USING(DOC_ID)
            LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.LOOKUPS_latest` denial
                ON(DENIED_REASON_ID = denial.LOOKUP_ID)
            INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
                ON doc_ep.PERSON_ID = peid.external_id
                AND peid.state_code = 'US_AZ'
                AND id_type = 'US_AZ_PERSON_ID'
            WHERE dtp.DENIED_REASON_ID IS NOT NULL
            
            UNION ALL

            SELECT
                state_code,
                person_id,
                DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', update_datetime) AS DATE), INTERVAL 1 DAY) AS start_date,
                "Denied ACIS TPR" AS denied_reason,
                "No TPR Date" AS denied_comment,
                -- Keeping the following for use in filtering in later CTEs
                task_metadata,
                update_datetime,
                eligible_date,
            FROM
                `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
            WHERE
                state_code = 'US_AZ'
                AND task_subtype = "{task_subtype}"
        ),
        
        most_recent_statuses AS (
            SELECT 
                den.state_code,
                den.person_id,
                den.start_date,
                iss.end_date_exclusive AS end_date,
                den.denied_reason,
                den.denied_comment,
                task_metadata,
                eligible_date,
            FROM denials den
            INNER JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
            ON iss.person_id = den.person_id
                AND iss.state_code = den.state_code
                AND den.start_date BETWEEN iss.start_date AND {nonnull_end_date_exclusive_clause('iss.end_date_exclusive')}
                AND iss.state_code = 'US_AZ'
            -- Filters to grab the most recent denied reason status for a given resident in a given incarceration stint
            QUALIFY ROW_NUMBER() OVER (PARTITION BY den.person_id, den.state_code, iss.incarceration_super_session_id 
                                        ORDER BY den.update_datetime DESC ) = 1
        ),
        
        incarceration_with_denials_spans AS (
            SELECT 
                state_code,
                person_id,
                start_date,
                end_date,
                denied_reason,
                denied_comment,
            FROM most_recent_statuses 
            -- The first half of this where clause checks for denials in the native eligibility tables
            WHERE (JSON_EXTRACT(task_metadata, '$.status') = '"DENIED"' 
                        OR JSON_EXTRACT(task_metadata, '$.status') = '"APPROVED"' AND eligible_date is null)
            -- This second half brings in all residents from the first half of the above denials cte who remain unaffected
                  OR (task_metadata = 'BYPASS')
        ),
        
        {create_sub_sessions_with_attributes('incarceration_with_denials_spans')}
        
        SELECT 
            state_code,
            person_id,
            start_date, 
            end_date,
            FALSE AS meets_criteria,
            TO_JSON(STRUCT(
                STRING_AGG(denied_reason, ' - ' ORDER BY denied_reason) AS denied_reason, 
                STRING_AGG(denied_comment, ' - ' ORDER BY denied_comment) AS denied_comment)) 
            AS reason,
            STRING_AGG(denied_reason, ' - ' ORDER BY denied_reason) AS denied_reason,
            STRING_AGG(denied_comment, ' - ' ORDER BY denied_comment) AS denied_comment
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
        """


def incarceration_past_early_release_date(
    opp_name: str, leading_window_time: int = 0, date_part: str = "DAY"
) -> str:
    """
    Args:
        opp_name: Name of the opportunity
        leading_window_time: X time before someone can be eligible
        date_part: one of ["MONTH", "DAY", "YEAR"]

    Returns:
        Spans of time when someone is within x months of their critical date. This defaults to spans of time
        before and after a critical date.
    """
    assert opp_name.upper() in ("TPR", "DTP"), "Opportunity Name must be one of TPR/DTP"
    if opp_name.upper() == "TPR":
        acis_date = "acis_tpr_date"
    else:
        acis_date = "acis_dtp_date"
    return f"""
    WITH critical_date_spans AS
    (
    SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date AS end_datetime,
        {acis_date} as critical_date
    FROM `{{project_id}}.analyst_data.us_az_projected_dates_materialized` dates
    WHERE {acis_date} IS NOT NULL
    ),
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time=leading_window_time, date_part=date_part)}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(critical_date AS {acis_date})) AS reason,
        critical_date AS {acis_date},
    FROM critical_date_has_passed_spans
    """


FELONY_ARSON_STATUTES = [
    "13-1703",  # ARSON OF STRUCTURE/PROPRTY
    "13-1703-1",  # ARSON OF STRUCTURE/PROPRTY
    "13-1703-2",  # ARSON OF STRUCTURE/PROPRTY
    "13-1703-NONE",  # ARSON OF STRUCTURE/PROPRTY
    "13-1704",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-1",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-9",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-A",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-E",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-NONE",  # ARSON OF OCCUPD STRUCTURE
    "13-1705",  # ARSON OF OCCUPD JAIL/PRSN
    "13-1705-NONE",  # ARSON OF OCCUPD JAIL/PRSN
    "13-1706",  # BURNING OF WILDLANDS
    "13-1706-NONE",  # BURNING OF WILDLANDS
]

ARSON_STATUTES = [
    "13-231-NONE",  # ARSON FIRST DEGREE
    "13-1705-NONE",  # ARSON OF OCCUPD JAIL/PRSN
    "13-1704-9",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-E",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-1",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-NONE",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-A",  # ARSON OF OCCUPD STRUCTURE
    "13-1703-2",  # ARSON OF STRUCTURE/PROPRTY
    "13-1703-1",  # ARSON OF STRUCTURE/PROPRTY
    "13-1703-NONE",  # ARSON OF STRUCTURE/PROPRTY
    "13-232-NONE",  # ARSON SECOND DEGREE
    "13-233-NONE",  # ARSON THIRD DEGREE
    "13-236-NONE",  # ARSON UNOCCUPIED STRUCTURE
]

DOMESTIC_VIOLENCE_STATUTES = [
    "13-3601",  # DOMESTIC VIOLENCE
]

HOMICIDE_AND_MURDER_STATUTES = [
    "13-1101",  # NEGLIGENT HOMICIDE
    "13-1102",  # NEGLIGENT HOMICIDE
    "13-1103",  # NEGLIGENT HOMICIDE
    "13-1104",  # MURDER 2ND DEGREE
    "13-1105",  # MURDER 1ST DEGREE
]

_ADDL_INELIGIBLE_VIOLENT_STATUTES = [
    "13-1102",  # NEGLIGENT HOMICIDE
    "13-1209",  # DRIVE BY SHOOTING
    "13-1211",  # DISCHARGE FIREARM AT STRUCTURE
    "13-1304",  # KIDNAPPING
    "13-491",  # KIDNAPPING, NEW CODE
    "13-1408",  # ADULTERY
    "13-1508",  # BURGLARY 1ST DEGREE
    "13-1904",  # ARMED ROBBERY
    "13-3107",  # UNLAWFUL DISCHARGE OF FIREARMS
    "13-3623",  # CHILD/ADULT ABUSE
    "28-661",  # ACCIDENTS INVOLVING DEATH OR PERSONAL INJURY
]
