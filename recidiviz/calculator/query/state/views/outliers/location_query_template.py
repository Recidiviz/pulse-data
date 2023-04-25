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
"""Helpers for querying reference_views.location_metadata"""
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)


def location_query_template(
    location: str,
    parent_id: str,
) -> str:
    return f"""
    WITH 
    location_metadata AS (
        SELECT * except (location_metadata),
            JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_district_id') AS supervision_district_id,
            JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_district_name') AS supervision_district_name,
            JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_unit_id') AS supervision_unit_id,
            JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_unit_name') AS supervision_unit_name,
        FROM `{{project_id}}.{{reference_views_dataset}}.location_metadata_materialized`
    )
    
    SELECT 
        DISTINCT {location}_id AS external_id, 
        {location}_name AS name,
        state_code, {f"{parent_id}," if parent_id != "state_code" else ""}
    FROM location_metadata
    WHERE {location}_id IS NOT NULL
        AND state_code IN ({list_to_query_string(get_outliers_enabled_states(), quoted=True)}) 
    """
