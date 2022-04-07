# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""List of people in ND actively on supervision along with their projected completion date"""

US_ND_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE = """
    -- First CTE gets latest projected end date for everyone currently on supervision
    us_nd_max_dates AS (
      SELECT
        caseload.supervision_type,
        caseload.state_code,
        case_type,
        caseload.person_id,
        caseload.supervising_officer_external_id,
        caseload.supervising_district_external_id,
        caseload.date_of_supervision,
        person_external_id,
        supervision_level,
        -- Take max of is_life - if there's a concurrent sentence where one is a life sentence and one isn't, we take the max value which is true
        MAX(COALESCE(incarceration_sentence.is_life,false)) as is_life,
        MAX(supervision_sentence.projected_completion_date) AS max_parole_to_date,
      FROM `{project_id}.{dataflow_dataset}.most_recent_single_day_supervision_population_metrics_materialized` caseload
      -- The projected completion date is pulled from this table because it needs to correspond to the PAROLE_TO date, 
      -- which is ingested into supervision sentence. Using dataflow metrics would mean we are sometimes using PAROLE_TO 
      -- and sometimes the sentence expiration date from state_incarceration_sentence
      # TODO(#9271): Use projected_end_date from dataflow once state specific override is implemented
      LEFT JOIN `{project_id}.{base_dataset}.state_incarceration_sentence` incarceration_sentence
        ON caseload.person_id = incarceration_sentence.person_id
      LEFT JOIN `{project_id}.{base_dataset}.state_supervision_sentence` supervision_sentence
        ON caseload.person_id = supervision_sentence.person_id 
      WHERE caseload.state_code = 'US_ND' 
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ), 
    us_nd as (   
        SELECT DISTINCT 
            max_dates.state_code,
            max_dates.person_id,
            max_dates.person_external_id,
            max_dates.case_type,
            max_dates.supervision_type,
            max_dates.supervision_level,
            max_dates.supervising_officer_external_id,
            max_dates.supervising_district_external_id,
            max_dates.date_of_supervision,  
            ref.level_1_supervision_location_name as district_name,
            max_parole_to_date AS projected_end_date,
            active_revocation,
        FROM us_nd_max_dates max_dates
        LEFT JOIN `{project_id}.{reference_dataset}.supervision_location_ids_to_names` ref
            ON max_dates.supervising_district_external_id = ref.level_1_supervision_location_external_id
            AND max_dates.state_code = ref.state_code
        -- This subquery identifies individuals in "active revocation" where TA_TYPE = 13
        # TODO(#9210): Remove this logic when ingest issue is resolved, since these should be getting marked as supervision periods ending in "ABSCONSION" which are excluded from this table
        LEFT JOIN (
                SELECT DATE(PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S%p', PAROLE_TO)) AS PAROLE_TO, 
                        1 AS active_revocation ,
                        pei.person_id
                FROM `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offendercasestable_latest`
                LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
                    ON SID = pei.external_id
                    AND pei.state_code = "US_ND"
                    AND pei.id_type = 'US_ND_SID'
                WHERE TA_TYPE = '13'
                QUALIFY ROW_NUMBER() OVER(partition by SID ORDER BY  PAROLE_TO DESC ) = 1
            ) docstars_cases
            ON max_dates.person_id = docstars_cases.person_id
            AND max_dates.max_parole_to_date = docstars_cases.PAROLE_TO
        WHERE COALESCE(is_life, false) = false 
    )
    """
