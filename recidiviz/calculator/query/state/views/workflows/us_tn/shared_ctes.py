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
    get_sentences_current_span,
)


def us_tn_fines_fees_info() -> str:
    return f"""
    fines_fees_balance_info AS (
        SELECT 
               ff.state_code,
               ff.person_id,
               pei.person_external_id,
               ff.current_balance,
        FROM ({client_specific_fines_fees_balance(unpaid_balance_field="unpaid_balance_within_supervision_session")}) ff
        INNER JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
            USING (person_id)
        -- This line helps specify which fee_type we're getting in TN
        WHERE ff.state_code != "US_TN" OR fee_type = "SUPERVISION_FEES"
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_external_id, state_code ORDER BY start_date DESC) = 1
    ),
    """


def us_tn_get_offense_information(in_projected_completion_array: bool = True) -> str:
    return f"""
        SELECT person_id,
         ARRAY_AGG(DISTINCT off.docket_number IGNORE NULLS ORDER BY off.docket_number) AS docket_numbers,
         ARRAY_AGG(off.offense IGNORE NULLS ORDER BY off.offense) AS current_offenses,
         ARRAY_AGG(
                DISTINCT
                CASE WHEN codes.Decode IS NOT NULL THEN CONCAT(off.conviction_county, ' - ', codes.Decode)
                    ELSE off.conviction_county END
                IGNORE NULLS
                ORDER BY
                    CASE WHEN codes.Decode IS NOT NULL THEN CONCAT(off.conviction_county, ' - ', codes.Decode)
                        ELSE off.conviction_county END
                ) AS conviction_counties,
        ARRAY_AGG(
            NULLIF(off.judicial_district, "EXTERNAL_UNKNOWN")
            IGNORE NULLS
            ORDER BY NULLIF(off.judicial_district, "EXTERNAL_UNKNOWN")
        ) AS judicial_district,
        MIN(off.sentence_start_date) AS sentence_start_date,
        MAX(off.expiration_date) AS expiration_date,
      FROM 
        ({get_sentences_current_span(in_projected_completion_array=in_projected_completion_array)}) off
      LEFT JOIN (
                SELECT *
                FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.CodesDescription_latest`
                WHERE CodesTableID = 'TDPD130'
            ) codes
      ON off.conviction_county = codes.Code
      GROUP BY 1
      """


def us_tn_supervision_type() -> str:
    return """
    # See Case Type for these mappings https://app.gitbook.com/o/-MS0FZPVqDyJ1aem018G/s/-MRvK9sMirb5JcYHAkjo-887967055/state-ingest-catalog/us_tn/raw_data/assignedstaff
    SELECT person_id,
           CASE SPLIT(supervision_type_raw_text,'-')[SAFE_OFFSET(2)]
                WHEN "PPO" THEN "PROBATION"
                WHEN "TNP" THEN "PAROLE"
                WHEN "DIV" THEN "DIVERSION"
                WHEN "ISC" THEN "ISC FROM OTHER JURISDICTION"
                WHEN "DET" THEN "DETERMINATE RLSE PROBATIONER"
                WHEN "MIS" THEN "MISDEMEANOR PROBATIONER"
                WHEN "SAI" THEN "SPECIAL ALT INCARCERATION UNIT"
            ELSE NULL END AS supervision_type,                   
    FROM 
        `{project_id}.{normalized_state_dataset}.state_supervision_period`
    WHERE
        state_code = "US_TN"
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY start_date DESC) = 1
    """
