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
"""List of people in PA actively on supervision along with their projected completion date"""

US_PA_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE = """
    us_pa_caseload AS (
      SELECT
        supervision_type,
        case_type,
        caseload.person_id,
        caseload.state_code,
        caseload.supervising_officer_external_id,
        caseload.supervising_district_external_id,
        caseload.date_of_supervision,
        person_external_id,
        supervision_level,
      FROM `{project_id}.{dataflow_dataset}.most_recent_single_day_supervision_population_metrics_materialized` caseload
      WHERE state_code = 'US_PA'
    ),
    # TODO(#9274): Use projected_end_date from dataflow once state specific override is implemented
    external_ids AS (
    SELECT
        person_id,
        external_id,
    FROM `{project_id}.{base_dataset}.state_person_external_id`
    INNER JOIN us_pa_caseload
        USING (person_id)
    WHERE id_type = 'US_PA_INMATE' 
    ),
    sentences AS (
      SELECT
        person_id,
        MAX(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y%m%d', max_expir_date) AS DATE)) AS max_release_date,
      FROM `{project_id}.us_pa_raw_data_up_to_date_views.dbo_Senrec_latest` senrec
      INNER JOIN external_ids
        ON senrec.curr_inmate_num = external_ids.external_id
      GROUP BY person_id 
    ),
    us_pa AS (
        SELECT DISTINCT
            caseload.state_code,
            caseload.person_id,
            caseload.person_external_id,
            caseload.case_type,
            caseload.supervision_type,
            caseload.supervision_level,
            caseload.supervising_officer_external_id,
            caseload.supervising_district_external_id,
            caseload.date_of_supervision,
            ref.level_2_supervision_location_name as district_name,
            max_release_date AS projected_end_date,
            NULL AS active_revocation,
        FROM us_pa_caseload caseload
        LEFT JOIN sentences
            USING(person_id)
        LEFT JOIN `{project_id}.reference_views.supervision_location_ids_to_names` ref
          ON caseload.supervising_district_external_id = ref.level_2_supervision_location_external_id
          AND caseload.state_code = ref.state_code
    )
    """
