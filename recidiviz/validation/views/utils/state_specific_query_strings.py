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
"""State-specific query strings for validation views.
"""


def state_specific_dataflow_facility_name_transformation() -> str:
    """
    Facility name transformations for dataflow output when comparing against external validation data.

    For US_ME: The dataflow results will list SOUTHERN MAINE WOMEN'S REENTRY CENTER as a facility name, which is mapped from
    the value of the housing_unit in the ingest mapping CURRENT_STATUS_incarceration_periods_v2.yaml. However, US_ME's
    validation reports lists this facility as Maine Correctional Center because of how the facility was created in
    their system."""
    return """
        IFNULL(
            CASE WHEN state_code = 'US_PA' THEN UPPER(LEFT(facility, 3))
                -- TODO(#11192): Remove state-specific facility mapping when we have housing_unit in external validation data
                WHEN state_code = 'US_ME' 
                    THEN IF(facility = "SOUTHERN MAINE WOMEN'S REENTRY CENTER", "MAINE CORRECTIONAL CENTER", facility)
                 WHEN state_code = 'US_CO' 
                    AND facility IN ('ARTS','ATC','CM/DEN/CBT','COMCOR CC','CORECIVIC','DNVRGENHOS','GCCC','GEO RS','HILLTOP CC','ICCS-C','LCCC','MESA','RRK')  THEN 'COMMUNITY CORRECTIONS' 
                 WHEN state_code = 'US_CO' 
                    AND facility = 'ACCC' THEN 'FUG-INMATE' 
            ELSE facility END,
            'EXTERNAL_UNKNOWN'
        ) AS facility
    """
