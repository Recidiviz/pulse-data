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
"""List of people in MO actively on supervision along with their projected completion date"""

US_MO_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE = """
        -- First CTE gets latest projected end date for everyone currently on supervision
        us_mo_max_dates AS (
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
            -- This logic takes the maximum of state_incarceration_sentence and state_supervision_sentence
            # TODO(#9211): Use projected_end_date from dataflow metrics once #9197 is resolved
            # TODO(#9272): Improve projected_end_date to account for earned credits
            MAX(supervision_sentence.projected_completion_date) AS probation_max_completion_date,
            MAX(incarceration_sentence.projected_max_release_date) AS parole_max_completion_date,
          FROM `{project_id}.{dataflow_dataset}.most_recent_single_day_supervision_population_metrics_materialized` caseload
          LEFT JOIN `{project_id}.{base_dataset}.state_supervision_sentence` supervision_sentence
            ON caseload.person_id = supervision_sentence.person_id AND supervision_sentence.status = 'SERVING'
          LEFT JOIN `{project_id}.{base_dataset}.state_incarceration_sentence` incarceration_sentence
            ON caseload.person_id = incarceration_sentence.person_id AND incarceration_sentence.status = 'SERVING'
          WHERE caseload.state_code = 'US_MO' 
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ), 
        us_mo AS (
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
                level_1_supervision_location_name as district_name,
                GREATEST(COALESCE(parole_max_completion_date,probation_max_completion_date), COALESCE(probation_max_completion_date,parole_max_completion_date)) as projected_end_date,
                NULL AS active_revocation,
            FROM us_mo_max_dates max_dates 
            LEFT JOIN `{project_id}.reference_views.supervision_location_ids_to_names` ref
              ON max_dates.supervising_district_external_id = ref.level_1_supervision_location_external_id
              AND max_dates.state_code = ref.state_code
        )
        """
