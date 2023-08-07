#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""CTE Logic that is shared across US_TN Workflows queries."""

from recidiviz.task_eligibility.utils.preprocessed_views_query_fragments import (
    client_specific_fines_fees_balance,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    get_current_offenses,
)


def us_tn_fines_fees_info() -> str:
    return f"""
    fines_fees_balance_info AS (
        SELECT 
               ff.state_code,
               ff.person_id,
               pei.person_external_id,
               ff.current_balance,
        FROM ({client_specific_fines_fees_balance(unpaid_balance_field="compartment_level_0_unpaid_balance")}) ff
        INNER JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
            USING (person_id)
        WHERE ff.state_code = "US_TN"
            AND fee_type = "SUPERVISION_FEES"
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_external_id, state_code ORDER BY start_date DESC) = 1
    ),
    """


def us_tn_get_current_offense_information() -> str:
    return f"""
        SELECT person_id,
         ARRAY_AGG(DISTINCT off.docket_number IGNORE NULLS) AS docket_numbers,
         ARRAY_AGG(off.offense IGNORE NULLS) AS current_offenses,
         ARRAY_AGG(
                DISTINCT
                CASE WHEN codes.Decode IS NOT NULL THEN CONCAT(off.conviction_county, ' - ', codes.Decode)
                    ELSE off.conviction_county END
                IGNORE NULLS
                ) AS conviction_counties,
        ARRAY_AGG(NULLIF(off.judicial_district, "EXTERNAL_UNKNOWN") IGNORE NULLS) AS judicial_district,
        MIN(off.sentence_start_date) AS sentence_start_date,
        MAX(off.expiration_date) AS expiration_date,
      FROM 
        ({get_current_offenses()}) off
      LEFT JOIN (
                SELECT *
                FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.CodesDescription_latest`
                WHERE CodesTableID = 'TDPD130'
            ) codes
      ON off.conviction_county = codes.Code
      GROUP BY 1


    
    """
