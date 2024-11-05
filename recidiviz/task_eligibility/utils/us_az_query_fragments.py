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

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def no_current_or_prior_convictions(
    statute: Optional[str] | Optional[list] = None,
    description: Optional[list] = None,
    additional_where_clause: Optional[str] = None,
    negate_statute: bool = False,
) -> str:
    """Helper function for a denial reason for a current or prior conviction.
    Requires a state specific jargon due to charge_v2 change.

    Args:
        statute (str | list): The statute(s) to be included in the exclusion
        description (list): The charge descriptions to be included in the exclusion, typically specified in regex
    """
    if statute is None:
        statute = []
    if description is None:
        description = []
    negate_statute_string = ""
    if negate_statute:
        negate_statute_string = "NOT"
    assert isinstance(description, list), "description must be of type list"
    if not statute and not description and not additional_where_clause:
        raise ValueError(
            "Either 'statute', 'description' or 'additional_where_clause' must be provided."
        )

    return f"""
    WITH
      ineligible_spans AS (
          SELECT
            span.state_code,
            span.person_id,
            span.start_date,
            CAST(NULL AS DATE) AS end_date,
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
            {f"AND {negate_statute_string} charge.statute LIKE '%{statute}%'" if isinstance(statute, str) else
    f"AND {negate_statute_string} (" + " OR ".join([f"charge.statute LIKE '%{s}%'" for s in statute]) + ")" if statute
    else ""}
            {"AND (" + " OR ".join([f"charge.description LIKE '%{d}%'" for d in description]) + ")" if description 
    else ""}
            {f"AND {additional_where_clause}" if additional_where_clause else ""}),
      {create_sub_sessions_with_attributes('ineligible_spans')}
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            meets_criteria,
            TO_JSON(STRUCT( ARRAY_AGG(DISTINCT description) AS ineligible_offenses)) AS reason,
            ARRAY_AGG(DISTINCT description ORDER BY description) AS ineligible_offenses,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4,5
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
    # TODO(#33401): Migrate this to `sentence_sessions.sentence_spans`
    WITH sentence_status_snapshot AS (
        # Completed sentences
        SELECT 
            state_code,
            person_id,
            sentence_id,
            status,
            MIN(SAFE_CAST(status_update_datetime AS DATE)) AS status_update_datetime,
        FROM `{project_id}.normalized_state.state_sentence_status_snapshot`
        WHERE state_code = 'US_AZ'
            AND status = 'COMPLETED'
        GROUP BY 1,2,3,4
    ),
    sentence_length AS (
        SELECT 
            ssl.state_code,
            ssl.person_id,
            ssl.sentence_id,
            ssl.projected_completion_date_max_external,
        FROM `{project_id}.normalized_state.state_sentence_length` ssl
        LEFT JOIN sentence_status_snapshot sss
            USING(state_code, person_id, sentence_id)
        WHERE ssl.state_code = 'US_AZ'
            AND sss.status != 'COMPLETED'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ssl.state_code, ssl.person_id, ssl.sentence_id ORDER BY ssl.length_update_datetime DESC) = 1
    )
    SELECT 
        sent.state_code,
        sent.person_id,
        sent.sentence_group_external_id,
        sent.imposed_date AS start_date,
        # Make end_date exclusive
        DATE_ADD(sl.projected_completion_date_max_external, INTERVAL 1 DAY) AS end_date,
        sent.statute,
        sent.description,
    FROM `{project_id}.sentence_sessions.sentences_and_charges_materialized` sent
    LEFT JOIN sentence_length sl
        USING(state_code, person_id, sentence_id)
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

    _REASONS_FIELDS = [
        ReasonsField(
            name="statutes",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Relevant statutes associated with the transition release",
        ),
        ReasonsField(
            name="descriptions",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Descriptions of relevant statutes associated with the transition release",
        ),
        ReasonsField(
            name="latest_acis_update_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Most recent date ACIS date was set",
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
            STRING_AGG(statute, ', ' ORDER BY statute) AS statutes,
            STRING_AGG(description, ', ' ORDER BY description) AS descriptions,
            MAX(acis_set_date) AS latest_acis_update_date
        )) AS reason,
        STRING_AGG(statute, ', ' ORDER BY statute) AS statutes,
        STRING_AGG(description, ', ' ORDER BY description) AS descriptions,
        MAX(acis_set_date) AS latest_acis_update_date
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
