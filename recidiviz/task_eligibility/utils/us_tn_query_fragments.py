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
