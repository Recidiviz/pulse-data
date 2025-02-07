# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View logic to prepare US_IX counties and district info for PSI tools"""

US_IX_SENTENCING_COUNTIES_AND_DISTRICTS_TEMPLATE = """
SELECT
  state_code,
  location_name AS county,
  JSON_VALUE(location_metadata, '$.supervision_district_name') AS district
FROM
  `{project_id}.reference_views.location_metadata_materialized`
WHERE
  state_code = "US_IX"
  AND location_type = 'CITY_COUNTY'
  -- Things like UNKNOWN, OUT_OF_STATE, etc. have null districts
  AND JSON_VALUE(location_metadata, '$.supervision_district_name') IS NOT NULL
"""
