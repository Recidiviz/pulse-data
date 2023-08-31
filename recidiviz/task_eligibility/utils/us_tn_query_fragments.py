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
"""
Helper SQL queries for Tennessee
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)


def detainers_cte() -> str:
    """Helper method that returns a CTE getting detainer information in TN"""

    return f"""
        -- As discussed with TTs in TN, a detainer is "relevant" until it has been lifted, so we use that as
        -- our end date
        SELECT
            state_code,
            person_id,
            DATE(DetainerReceivedDate) AS start_date,
            DATE(DetainerLiftDate) AS end_date,
            DetainerFelonyFlag AS detainer_felony_flag,
            DetainerMisdemeanorFlag AS detainer_misdemeanor_flag,
            CASE WHEN DetainerFelonyFlag = 'X' THEN 5
                 WHEN DetainerMisdemeanorFlag = 'X' THEN 3
                 END AS detainer_score
        FROM 
            `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Detainer_latest` dis
        INNER JOIN
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            dis.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            (DetainerFelonyFlag = 'X' OR DetainerMisdemeanorFlag = 'X')
            AND 
            {nonnull_end_date_exclusive_clause('DATE(DetainerLiftDate)')} > {nonnull_end_date_exclusive_clause('DATE(DetainerReceivedDate)')}
        
        """


def keep_contact_codes(
    codes_cte: str,
    comments_cte: str,
    where_clause_codes_cte: str,
    output_name: str = "output",
    keep_last: bool = False,
) -> str:

    """
    Helper function to join on contact codes with comments, filter to specific codes, and either keep all codes of a
    specific type or the latest one. Useful for TEPE form and side-bar

    codes_cte: String that contains a CTE with contact notes that can be filtered on ContactNoteType
    comments_cte: String that contains a CTE with contact comments
    where_clause_codes_cte: String used to filter to specific contact types
    output_name: Optional parameter for Struct containing contact date, type, comment
    keep_last: Optional parameter defaulting to false. If true, only the latest contact date for a given person is kept,
              otherwise all are
    """

    qualify = ""
    if keep_last:
        qualify = "QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1"

    return f"""
        SELECT
            person_id,
            STRUCT(
              contact_date,
              contact_type,
              contact_comment
            ) AS {output_name},
            contact_date,
            contact_type,
            contact_comment,
        FROM (
            SELECT
                person_id,
                contact_date,
                STRING_AGG(DISTINCT contact_type, ", ") AS contact_type,
            FROM {codes_cte}
            {where_clause_codes_cte}
            GROUP BY 1,2
        )
        LEFT JOIN (
            SELECT
                person_id,
                contact_date,
                STRING_AGG(DISTINCT contact_comment, ", ") AS contact_comment,
            FROM {comments_cte}
            GROUP BY 1,2        
        )
        USING(person_id, contact_date)
        {qualify}
    """


def at_least_X_time_since_latest_assessment(
    assessment_type: str,
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
    Args:
        assessment_type (str): Type of assessment
        date_interval (int): Number of weeks, months, etc between assessments
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
    Returns:
        f-string: Spans of time where the criteria is NOT met, because X time has not passed between assessments
    """

    return f"""
    WITH assessment_sessions_cte AS
    (
        SELECT
            state_code,
            person_id,
            /* The logic behind the start and end date is the following - during the classification process we see a 
            few different relevant dates: CAFDate, ClassificationDate, and ClassificationDecisionDate. CAFDate is the
            date of the CAF form, and what we ingest as assessment_date. ClassificationDate is the date the
            classification was submitted, and ClassificationDecisionDate is when a decision was made. 
            
            We want the span when someone is ineligible to begin on classification_decision_date; this ensures that
            someone will continue to show up as eligible even after a form has been submitted for them, until the final
            hearing is done and a decision is made.
            
            We want the span to end 12 months after the ClassificationDate, as per what Counselors have told us they use
            to evaluate when a person needs an annual reclass.
            
            Since a person is considered "eligible" during the whole month where their assessment is due, we use a
            DATE_TRUNC() for the end date so that someone becomes eligible in the month of their assessment due date.
            
            This also improves tracking historical spans in situations when an annual assessment may be filled out
            prior to the due date, since we consider someone eligible the starting the first of the month when their
            assessment is due 
            */
            classification_decision_date AS start_date,
            DATE_TRUNC(DATE_ADD(assessment_date, INTERVAL {date_interval} {date_part}), MONTH) AS end_date,
            FALSE AS meets_criteria,
            assessment_date,
        FROM
            (
                SELECT asmt.* EXCEPT(assessment_date),
                        classification_decision_date,
                        COALESCE(
                            DATE(NULLIF(JSON_EXTRACT_SCALAR(assessment_metadata,"$.CLASSIFICATIONDATE"),"")),
                            assessment_date
                        ) AS assessment_date,
                FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` asmt
                LEFT JOIN (
                    SELECT person_id, 
                            DATE(CAFDate) AS caf_date, 
                            OverrideReason,
                            DATE(ClassificationDecisionDate) AS classification_decision_date, 
                    FROM
                        `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.Classification_latest` classification
                    INNER JOIN
                        `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
                    ON
                        classification.OffenderID = pei.external_id
                    AND
                        pei.state_code = 'US_TN'
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderId, CAFDate ORDER BY ClassificationSequenceNumber DESC) = 1
                ) caf_raw
                    ON asmt.person_id = caf_raw.person_id
                    AND asmt.assessment_date = caf_raw.caf_date
            )
        WHERE
            assessment_type = "{assessment_type}"
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_type, assessment_date ORDER BY assessment_score DESC) = 1
    )
    ,
    {create_sub_sessions_with_attributes('assessment_sessions_cte')}
    ,
    dedup_cte AS
    (
        SELECT
            *,
        FROM sub_sessions_with_attributes
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
            ORDER BY assessment_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is eligible. A
    new session exists either when a person becomes eligible, or if a person has an additional assessment in the 
    specified date interval, which changes the "assessment_date" value.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['assessment_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(assessment_date AS most_recent_assessment_date,
                        DATE_ADD(assessment_date, INTERVAL 12 MONTH) AS assessment_due_date
                        )
                ) AS reason
    FROM sessionized_cte
    """
