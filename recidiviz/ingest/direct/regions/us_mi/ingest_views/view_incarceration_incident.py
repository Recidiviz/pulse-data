# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Query containing incarceration incident information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH all_info AS (
        SELECT
            incident.offender_booking_id, 
            incident.misconduct_incident_id,
            incident.miscond_violation_date,
            incident.miscond_written_date,
            ref_loc.name AS _incident_location_name,
            ref_micond_place_type.description AS _incident_place_type_description,
            incident.proposed_hearing_date,
            offense.description as _offense_description,
            offense.misconduct_offense_code,
            miscond_hearing_penalty_id,
            ref_penalty_type.description AS _penalty_type_description,
            CAST(penalty.penalty_days AS FLOAT64) + CEILING(CAST(penalty_hours AS FLOAT64)/24) AS penalty_days,
            CAST(penalty.effective_date AS DATETIME) AS effective_date,
            CAST(penalty.end_date AS DATETIME) AS end_date
        FROM {ADH_MISCONDUCT_INCIDENT} incident
        LEFT JOIN {ADH_LOCATION} ref_loc 
        ON incident.location_id = ref_loc.location_id
        LEFT JOIN {ADH_REFERENCE_CODE}
        ref_micond_place_type ON incident.incident_place_type_id = ref_micond_place_type.reference_code_id
        LEFT JOIN {ADH_MISCONDUCT_CHARGE} charge
        ON incident.misconduct_incident_id = charge.misconduct_incident_id
        LEFT JOIN {ADH_MISCONDUCT_OFFENSE} offense
        ON charge.misconduct_offense_id = offense.misconduct_offense_id
        LEFT JOIN {ADH_OFFENDER_MISCOND_HEARING} hearing
        ON charge.offender_miscond_hearing_id = hearing.offender_miscond_hearing_id
        LEFT JOIN {ADH_MISCOND_HEARING_PENALTY} penalty
        ON charge.offender_miscond_hearing_id = penalty.offender_miscond_hearing_id
        LEFT JOIN {ADH_REFERENCE_CODE} ref_penalty_type
        ON penalty.penalty_type_id = ref_penalty_type.reference_code_id
    ),
    penalties_info AS (
        SELECT
            misconduct_incident_id,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT<
                                miscond_hearing_penalty_id string,
                                _penalty_type_description string,
                                penalty_days FLOAT64,
                                effective_date DATETIME,
                                end_date DATETIME>
                            (
                                miscond_hearing_penalty_id,
                                _penalty_type_description,
                                penalty_days,
                                effective_date,
                                end_date
                            ) ORDER BY miscond_hearing_penalty_id)
            ) AS penalties
        FROM (
            SELECT DISTINCT 
                misconduct_incident_id,
                miscond_hearing_penalty_id,
                _penalty_type_description,
                penalty_days,
                effective_date,
                end_date
            FROM all_info
        ) sub1
        GROUP BY misconduct_incident_id
    ),
    offenses_info AS (
        SELECT
            misconduct_incident_id,
            STRING_AGG(DISTINCT _offense_description, '@@' ORDER BY _offense_description) AS _offense_description_list,
            STRING_AGG(DISTINCT misconduct_offense_code, '@@' ORDER BY misconduct_offense_code) AS _offense_code_list,
        FROM all_info
        GROUP BY misconduct_incident_id
    )
    SELECT DISTINCT
        offender_booking_id, 
        misconduct_incident_id,
        miscond_violation_date,
        miscond_written_date,
        _incident_location_name,
        _incident_place_type_description,
        proposed_hearing_date,
        _offense_description_list,
        _offense_code_list,
        penalties
    FROM all_info
    LEFT JOIN offenses_info USING(misconduct_incident_id)
    LEFT JOIN penalties_info USING(misconduct_incident_id)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="incarceration_incident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
