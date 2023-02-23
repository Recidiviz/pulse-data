# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper SQL fragments for ME
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause


def cis_319_after_csswa(table: str = "sub_sessions_with_attributes") -> str:
    """Helper method to drop repeated subsessions and incorrect
    final spans. Meant to be used after create_sub_sessions_with_attributes (csswa).
    """

    return f"""
    -- Drop repeated subsessions and drop completed and end_date=NULL
    SELECT * EXCEPT(null_end_date_and_completed)
    FROM (SELECT 
             * EXCEPT(start_date, end_date),
             start_date AS start_datetime,
             end_date AS end_datetime,
             -- Completed terms that are the last subsession of one resident
             IF((end_date IS NULL AND status = '2'),
                True,
                False)
             AS null_end_date_and_completed
         FROM {table}
         -- Drop subsessions when repeated on the basis of status (drop 'Completed' first)
         -- and critical date (drop more recent ones first)
         QUALIFY ROW_NUMBER() 
                 OVER(PARTITION BY state_code, person_id, start_date, end_date 
                 ORDER BY status, {nonnull_end_date_clause('critical_date')} DESC) = 1)
    WHERE null_end_date_and_completed IS False
    """


def cis_204_notes_cte(criteria_name: str) -> str:

    """Helper method that returns a query that pulls
    up case notes with the format needed to be aggregated

    Args:
        criteria_name (str): Criteria name for the notes
    """
    return f"""SELECT
        n.Cis_100_Client_Id AS external_id,
        "{criteria_name}" AS criteria,
        n.Short_Note_Tx AS note_title,
        n.Note_Tx AS note_body,
        DATE(SAFE_CAST(n.Note_Date AS DATETIME)) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_204_GEN_NOTE_latest` n
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2041_NOTE_TYPE_latest` ncd
        ON ncd.Note_Type_Cd = n.Cis_2041_Note_Type_Cd
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2040_CONTACT_MODE_latest` cncd
        ON n.Cis_2040_Contact_Mode_Cd = cncd.Contact_Mode_Cd"""
