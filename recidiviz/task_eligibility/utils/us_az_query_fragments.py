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
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
    critical_date_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    task_deadline_critical_date_update_datetimes_cte,
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
    return """  SELECT
        peid.external_id,
        "Home Plan Information" AS criteria,
        plan_status AS note_title,
        IF(is_homeless_request = 'Y', "Request to release as homeless", "") AS note_body,
        -- We use update_date to capture the latest change, the start_date
        -- refers to the start of compartment_sessions.
        SAFE_CAST(LEFT(update_date, 10) AS DATE) AS event_date,
    FROM `{project_id}.analyst_data.us_az_home_plan_preprocessed_materialized` hp
    LEFT JOIN `{project_id}.normalized_state.state_person_external_id` peid
    ON peid.person_id = hp.person_id
        AND peid.state_code = 'US_AZ'
        AND peid.id_type = 'US_AZ_PERSON_ID'
    WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date_exclusive, '9999-12-31')"""


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
    else:
        task = "DTP"

    _REASONS_FIELDS = [
        ReasonsField(
            name=f"{task.lower()}_statutes",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description=f"{task}: Relevant statutes associated with the transition release",
        ),
        ReasonsField(
            name=f"{task.lower()}_descriptions",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description=f"{task}: Descriptions of relevant statutes associated with the transition release",
        ),
        ReasonsField(
            name=f"{task.lower()}_latest_acis_update_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description=f"{task}: Most recent date ACIS date was set",
        ),
    ]

    _QUERY_TEMPLATE = f"""
    WITH acis_set_date AS (
        SELECT
            state_code,
            person_id,
            JSON_EXTRACT_SCALAR(task_metadata, '$.sentence_group_external_id') AS sentence_group_external_id,
            SAFE_CAST(MIN(update_datetime) AS DATE) AS acis_set_date,
        FROM `{{project_id}}.normalized_state.state_task_deadline`
        WHERE task_type = 'DISCHARGE_FROM_INCARCERATION' 
            AND task_subtype = '{task_subtype}'
            AND state_code = 'US_AZ' 
            AND eligible_date IS NOT NULL 
            AND eligible_date > '1900-01-01'
        GROUP BY state_code, person_id, task_metadata, sentence_group_external_id
    ),

    sentences_preprocessed AS (
        {us_az_sentences_preprocessed_query_template()}
    ),

    sentences_with_an_acis_date AS (
        -- This identifies all sentences who have already had a TPR date set.
        SELECT 
            sent.person_id,
            sent.state_code,
            asd.acis_set_date AS start_date,
            sent.end_date,
            sent.statute,
            sent.description,
            asd.acis_set_date,
        FROM sentences_preprocessed sent
        INNER JOIN acis_set_date asd
            ON sent.person_id = asd.person_id
                AND sent.state_code = asd.state_code
                and sent.sentence_group_external_id = asd.sentence_group_external_id
        -- We don't pull sentences where their end_date is the same date as the acis_set_date
        WHERE asd.acis_set_date != {nonnull_end_date_clause('sent.end_date')}
    ),

    {create_sub_sessions_with_attributes('sentences_with_an_acis_date')}
    
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        False AS meets_criteria,
        TO_JSON(STRUCT(
            STRING_AGG(statute, ', ' ORDER BY statute) AS {task.lower()}_statutes,
            STRING_AGG(description, ', ' ORDER BY description) AS {task.lower()}_descriptions,
            MAX(acis_set_date) AS {task.lower()}_latest_acis_update_date
        )) AS reason,
        STRING_AGG(statute, ', ' ORDER BY statute) AS {task.lower()}_statutes,
        STRING_AGG(description, ', ' ORDER BY description) AS {task.lower()}_descriptions,
        MAX(acis_set_date) AS {task.lower()}_latest_acis_update_date
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_AZ,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=_REASONS_FIELDS,
    )


def almost_eligible_tab_logic(opp_name: str) -> str:
    """Returns the Almost Eligible logic reused for TPR & DTP Opportunities"""
    assert opp_name.upper() in ["TPR", "DTP"], "Opportunity Name must be TPR or DTP"
    return f"""
    -- Fast track: ACIS TPR/DTP date within 1 days and 30 days
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
    WHERE "US_AZ_INCARCERATION_PAST_ACIS_{opp_name.upper()}_DATE" IN UNNEST(ineligible_criteria)
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.criteria_name') AS STRING) = "US_AZ_INCARCERATION_PAST_ACIS_{opp_name.upper()}_DATE"
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_{opp_name.lower()}_date') AS DATE) BETWEEN 
            DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY) AND DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY)

    UNION ALL

    -- Almost eligible section 1: Projected TPR date within 7-180 days
    -- Almost eligible section 1: Projected TPR date within 7-180 days AND missing man lit
    -- Almost eligible section 2: Projected TPR date within 181-365 days AND missing at most one other criteria
    -- (functional literacy XOR no felony detainers)
    # TODO(#33958) - recidiviz_xxx_date_approaching needs to be split into section 1 and 2
    SELECT 
        * EXCEPT(criteria_reason),
        CASE
            WHEN is_eligible THEN "ALMOST_ELIGIBLE_BETWEEN_7_AND_180_DAYS"
            WHEN ARRAY_LENGTH(ineligible_criteria) = 1
                THEN CASE
                    WHEN "US_AZ_MEETS_FUNCTIONAL_LITERACY_{opp_name.upper()}" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_MISSING_MANLIT_BETWEEN_7_AND_180_DAYS"
                    WHEN "US_AZ_WITHIN_6_MONTHS_OF_RECIDIVIZ_{opp_name.upper()}_DATE" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_BETWEEN_181_AND_365_DAYS"
                    END
            ELSE "ALMOST_ELIGIBLE_MISSING_CRITERIA_AND_BETWEEN_181_AND_365_DAYS"
        END AS metadata_tab_description,
        CASE
            WHEN is_eligible THEN "ALMOST_ELIGIBLE_1"
            WHEN ARRAY_LENGTH(ineligible_criteria) = 1
                THEN CASE
                    WHEN "US_AZ_MEETS_FUNCTIONAL_LITERACY_{opp_name.upper()}" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_1"
                    WHEN "US_AZ_WITHIN_6_MONTHS_OF_RECIDIVIZ_{opp_name.upper()}_DATE" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_2"
                    END
            ELSE "ALMOST_ELIGIBLE_2"
        END AS metadata_tab_name,
    FROM recidiviz_{opp_name.lower()}_date_approaching,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    WHERE
        SAFE_CAST(JSON_VALUE(criteria_reason, '$.criteria_name') AS STRING) = "US_AZ_WITHIN_6_MONTHS_OF_RECIDIVIZ_{opp_name.upper()}_DATE"
        AND (
            is_almost_eligible
            OR (
                -- Only include residents with more than 7 days until the projected date if all other criteria are met
                is_eligible
                AND DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 7 DAY)
                    < SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.recidiviz_{opp_name.lower()}_date') AS DATE)
            )
        )
    """


def within_x_months_of_date(opp_name: str, time_interval: int) -> str:
    """Returns a query finding if the chosen opportunity date is within x months"""
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
        date_part="MONTH",
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
    WITH manlit_ingest AS (
        SELECT
          state_code,
          person_id,
          discharge_date as start_date,
          CAST(NULL AS DATE) AS end_date,
          TRUE AS meets_criteria,
          discharge_date AS latest_functional_literacy_date,
          'Program Assignment' AS data_location
        #TODO(#33858): Ingest into state task deadline or find some way to view this historically
        FROM
          `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment`
        WHERE state_code = 'US_AZ'
        AND (participation_status_raw_text IN ('COMPLETED')
            -- This catches cases where a resident has been exempted from Mandatory Literacy for any reason
            OR JSON_EXTRACT(referral_metadata, '$.EXEMPTION') != '""')
        AND program_id LIKE '%MAN%LIT%'
        #TODO(#33737): Look into multiple span cases for residents who have completed in MAN-LIT programs
        QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY start_date ASC) = 1
    ),
    #TODO(#34752): Make mandatory literacy data pull more accurate
    manlit_prg_eval AS (
        SELECT
          pei.state_code,
          pei.person_id, 
          -- We use the earliest possible CREATE_DTM case when we see a 'Y' for Mandatory Literacy Status
          -- CREATE_DTM is associated with all aspects of the chosen _TABLE and therefore we may see multiple
          -- CREATE_DTMs for the same literacy status due to another variable of someone's eligibility changing
          MIN(PARSE_DATE('%m/%d/%Y', SPLIT(eval.CREATE_DTM, ' ')[OFFSET(0)] )) AS start_date,
          CAST(NULL AS DATE) AS end_date,
          TRUE AS meets_criteria,
          MIN(PARSE_DATE('%m/%d/%Y', SPLIT(eval.CREATE_DTM, ' ')[OFFSET(0)] )) AS latest_functional_literacy_date,
          'PRG_EVAL' AS data_location,
        FROM
          `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.{_TABLE}_latest` eval
        INNER JOIN 
        `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.{_ELIG_TABLE}_latest` map_to_docid
        USING ({_ID_MAP})
        LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` doc_ep
        USING(DOC_ID)
        LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.PERSON_latest` person
        USING(PERSON_ID)
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON ADC_NUMBER = external_id 
            AND pei.state_code = 'US_AZ'
            AND pei.id_type = 'US_AZ_ADC_NUMBER'
        WHERE (MEETS_MANDITORY_LITERACY = 'Y' OR  LITERACY_EXCEPTION = 'Y')
        GROUP BY 1,2
    ),
    union_cte AS (
        SELECT
          *
        FROM
          manlit_ingest 
        UNION ALL
        SELECT
          *
        FROM
          manlit_prg_eval 
    ),
    {create_sub_sessions_with_attributes('union_cte')}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
                latest_functional_literacy_date AS latest_functional_literacy_date,
                data_location AS latest_data_location
            )) AS reason,
        latest_functional_literacy_date AS latest_functional_literacy_date,
        data_location AS latest_data_location,
    FROM
        sub_sessions_with_attributes
    -- Ensuring for every row, we grab the latest data location and latest date of meeting literacy
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id, start_date, meets_criteria ORDER BY latest_functional_literacy_date DESC) = 1
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
                IFNULL(
                SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', dtp.CREATE_DTM) AS DATE),
                SAFE_CAST(LEFT(dtp.CREATE_DTM, 10) AS DATE))
                AS start_date,
                denial.description AS denied_reason,
                dtp.DENIED_OTHER_COMMENT AS denied_comment,
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
                CAST(FORMAT_DATETIME('%Y-%m-%d', update_datetime) AS DATE) AS start_date,
                "Denied ACIS TPR" AS denied_reason,
                "No TPR Date" AS denied_comment,
            FROM
                `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
            WHERE
                state_code = 'US_AZ'
                AND task_subtype = "{task_subtype}"
                AND (JSON_EXTRACT(task_metadata, '$.status') = '"DENIED"'
                    OR JSON_EXTRACT(task_metadata, '$.status') = '"APPROVED"' AND eligible_date is null)
        ),
        
        incarceration_with_denials_spans AS (
            SELECT 
                den.state_code,
                den.person_id,
                den.start_date,
                iss.end_date_exclusive AS end_date,
                den.denied_reason,
                den.denied_comment
            FROM denials den
            LEFT JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
            ON iss.person_id = den.person_id
                AND iss.state_code = den.state_code
                AND den.start_date BETWEEN iss.start_date AND {nonnull_end_date_exclusive_clause('iss.end_date_exclusive')}
                AND iss.state_code = 'US_AZ'
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


def incarceration_past_early_release_date(opp_name: str) -> str:
    assert opp_name.upper() in ("TPR", "DTP"), "Opportunity Name must be one of TPR/DTP"
    if opp_name.upper() == "TPR":
        task_subtype = "STANDARD TRANSITION RELEASE"
    else:
        task_subtype = "DRUG TRANSITION RELEASE"
    _ADDITIONAL_WHERE_CLAUSE = f"""
                AND task_subtype = "{task_subtype}"
                AND state_code = 'US_AZ' 
                AND eligible_date IS NOT NULL 
                AND eligible_date > '1900-01-01'"""
    return f"""
        WITH
        {task_deadline_critical_date_update_datetimes_cte(
        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
        critical_date_column='eligible_date',
        additional_where_clause=_ADDITIONAL_WHERE_CLAUSE)
    },
        {critical_date_spans_cte()},
        critical_date_spans_within_100_days_of_date AS (
            SELECT 
                state_code,
                person_id,
                critical_date,
                start_datetime,
                -- If the end_datetime is within 100 days of the critical_date, use the end_datetime
                -- Otherwise, use the critical_date + 100 days. That way people only stay eligible
                -- for 100 days after their relevant date. After that, they've likely loss their 
                -- eligibility for a transition release.
                IF(
                    DATE_ADD(critical_date, INTERVAL 100 DAY) BETWEEN start_datetime AND IFNULL(end_datetime, '9999-12-31'),
                    LEAST(IFNULL(end_datetime, '9999-12-31'), 
                          DATE_ADD(critical_date, INTERVAL 100 DAY)),
                    end_datetime
                ) AS end_datetime,
            FROM critical_date_spans
            -- Drop row if critical_date or critical_date + 100 is not between start and end_date
            WHERE (critical_date BETWEEN start_datetime AND IFNULL(end_datetime, '9999-12-31') 
                OR DATE_ADD(critical_date, INTERVAL 100 DAY) BETWEEN start_datetime AND IFNULL(end_datetime, '9999-12-31'))
                -- The critical_date + 100 cannot be the start_datetime, this would create zero day spans
                AND DATE_ADD(critical_date, INTERVAL 100 DAY) != start_datetime
        ),
        {critical_date_has_passed_spans_cte(table_name='critical_date_spans_within_100_days_of_date')}
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            critical_date_has_passed AS meets_criteria,
            TO_JSON(STRUCT(critical_date AS acis_{opp_name.lower()}_date)) AS reason,
            critical_date AS acis_{opp_name.lower()}_date,
        FROM critical_date_has_passed_spans
    """


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
    "13-36",  # DOMESTIC VIOLENCE
]

HOMICIDE_AND_MURDER_STATUTES = [
    "13-1102",  # NEGLIGENT HOMICIDE
    "13-1104",  # MURDER 2ND DEGREE
    "13-1105",  # MURDER 1ST DEGREE
    "13-452",  # MURDER
    "13-453",  # MURDER
    "13-710",  # MURDER SECOND DEGREE
]

_ADDL_INELIGIBLE_VIOLENT_STATUTES = [
    "13-1102",  # NEGLIGENT HOMICIDE
    "13-1209",  # DRIVE BY SHOOTING
    "13-1211",  # DISCHARGE FIREARM AT STRUCTURE
    "13-1304",  # KIDNAPPING
    "13-1408",  # ADULTERY
    "13-1508",  # BURGLARY 1ST DEGREE
    "13-3107",  # UNLAWFUL DISCHARGE OF FIREARMS
    "13-3102",  # MISCONDUCT INVOLVING WEAPONS
    "13-3623",  # CHILD/ADULT ABUSE
    "28-661",  # ACCIDENTS INVOLVING DEATH OR PERSONAL INJURY
]
