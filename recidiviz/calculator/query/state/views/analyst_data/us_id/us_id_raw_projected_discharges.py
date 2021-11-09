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
"""List of people in ID actively on supervision along with their projected completion date"""

US_ID_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE = """
    -- First CTE gets latest projected end date for everyone currently on supervision
    us_id_max_dates AS (
      SELECT
        caseload.supervision_type,
        case_type,
        caseload.state_code,
        caseload.person_id,
        caseload.supervising_officer_external_id,
        caseload.supervising_district_external_id,
        caseload.date_of_supervision,
        person_external_id,
        supervision_level,
        -- Take max of is_life - if there's a concurrent sentence where one is a life sentence and one isn't, we take the max value which is true
        MAX(COALESCE(incarceration_sentence.is_life,false)) as is_life,
        -- This logic takes the maximum of state_incarceration_sentence which has full term release dates for parole sentences
        -- and state_supervision_sentence which has full term release dates for probation sentences
        # TODO(#9211): Use projected_end_date from dataflow metrics once #9197 is resolved
        MAX(supervision_sentence.projected_completion_date) AS probation_max_completion_date,
        MAX(incarceration_sentence.projected_max_release_date) AS parole_max_completion_date,
      FROM `{project_id}.{dataflow_dataset}.most_recent_single_day_supervision_population_metrics_materialized` caseload 
      LEFT JOIN `{project_id}.{base_dataset}.state_supervision_sentence` supervision_sentence
        ON caseload.person_id = supervision_sentence.person_id 
      LEFT JOIN `{project_id}.{base_dataset}.state_incarceration_sentence` incarceration_sentence
        ON caseload.person_id = incarceration_sentence.person_id 
      WHERE caseload.state_code = 'US_ID'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ), 
    -- Add in additional information (name, PO info, district info)
    us_id_life_sent_info AS (
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
            supervising_district_external_id as district_name,
            GREATEST(COALESCE(parole_max_completion_date,probation_max_completion_date), COALESCE(probation_max_completion_date,parole_max_completion_date)) as projected_end_date,
            -- Some life sentences in US_ID are recorded as '9999' and exported and parsed as '09-09-9999'. This line flags those so they can be potentially excluded from eligibility lists
            # TODO(#9971): Use only is_life flag when logic for it has been updated to account for 9999 cases
            CASE WHEN
                GREATEST(COALESCE(parole_max_completion_date,probation_max_completion_date), COALESCE(probation_max_completion_date,parole_max_completion_date)) = '9999-09-09' 
                OR is_life
            THEN true
            ELSE false
            END AS life_sentence,
            # TODO(#9212): Figure out how to identify those currently in PV process but not reincarcerated
            NULL AS active_revocation,
        FROM us_id_max_dates max_dates
        WHERE max_dates.supervision_type IN ('PAROLE','DUAL','PROBATION','INFORMAL_PROBATION')
    ),
    us_id AS (
        SELECT * EXCEPT(life_sentence)
        FROM us_id_life_sent_info
        WHERE life_sentence = false
    
    )
    """
