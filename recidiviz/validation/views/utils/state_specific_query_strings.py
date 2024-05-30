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
            CASE
                -- TODO(#11192): Remove state-specific facility mapping when we have housing_unit in external validation data
                WHEN state_code = 'US_ME' 
                    THEN IF(facility = "SOUTHERN MAINE WOMEN'S REENTRY CENTER", "MAINE CORRECTIONAL CENTER", facility)
                 WHEN state_code = 'US_CO' 
                    AND facility IN ('ARTS','ATC','CM/DEN/CBT','COMCOR CC','CORECIVIC','DNVRGENHOS','GCCC','GEO RS','HILLTOP CC','ICCS-C','LCCC','MESA','RRK')  THEN 'COMMUNITY CORRECTIONS' 
                 WHEN state_code = 'US_CO' 
                    AND facility = 'ACCC' THEN 'FUG-INMATE' 
                WHEN state_code = 'US_MO' 
                    THEN 'EXTERNAL_UNKNOWN'
                    # TODO(#16114) - Remove hacky logic once we have non-facility level aggregate validation
                WHEN state_code = 'US_IX' AND facility like '%COUNTY JAIL%' then REPLACE(facility, "COUNTY JAIL", "COUNTY SHERIFF DEPARTMENT")
                WHEN state_code = 'US_IX' AND facility="IDAHO STATE CORRECTIONAL CENTER" THEN "IDAHO CORRECTIONAL CENTER - BOISE"
                WHEN state_code = 'US_IX' AND facility="IDAHO STATE CORRECTIONAL INSTITUTION" THEN "IDAHO STATE CORRECTIONAL INSTITUTION, BOISE"
                WHEN state_code = 'US_IX' AND facility="SOUTH IDAHO CORRECTIONAL INSTITUTION" THEN "SOUTH IDAHO CORRECTIONAL INSTITUTION, BOISE"
                WHEN state_code = 'US_IX' AND facility="IDAHO CORRECTIONAL INSTITUTION-OROFINO" THEN "IDAHO CORRECTIONAL INSTITUTION, OROFINO"
                WHEN state_code = 'US_IX' AND facility="IDAHO MAXIMUM SECURITY INSTITUTION" THEN "IDAHO MAXIMUM SECURITY INSTITUTION, BOISE"
                WHEN state_code = 'US_IX' AND facility="SAGUARO CORRECTIONAL CENTER ARIZONA" THEN "SAGUARO CORRECTIONAL CENTER, ARIZONA"
                WHEN state_code = 'US_IX' AND facility="NORTH IDAHO CORRECTIONAL INSTITUTION" THEN "NORTH IDAHO CORRECTIONAL INSTITUTION, COTTONWOOD"
                WHEN state_code = 'US_IX' AND facility="CORRECTIONAL ALTERNATIVE PLACEMENT PROGRAM" THEN "CORRECTIONAL ALTERNATIVE PLACEMENT PROGRAM - BOISE"
                WHEN state_code = 'US_IX' AND facility="MOUNTAIN VIEW TRANSFORMATION CENTER" THEN "CORRECTIONAL ALTERNATIVE PLACEMENT PROGRAM - BOISE"
                WHEN state_code = 'US_IX' AND facility="POCATELLO WOMEN'S CORRECTIONAL CENTER" THEN "POCATELLO WOMAN'S CORRECTIONAL CENTER, POCATELLO"
                WHEN state_code = 'US_IX' AND facility="ST. ANTHONY WORK CAMP" THEN "ST. ANTHONY WORK CENTER, ST. ANTHONY"
                WHEN state_code = 'US_IX' AND facility="TWIN FALLS COMMUNITY WORK CENTER" THEN "TWIN FALLS COMMUNITY WORK CENTER, TWIN FALLS"
                WHEN state_code = 'US_IX' AND facility="TWIN FALLS COMMUNITY REENTRY CENTER" THEN "TWIN FALLS COMMUNITY WORK CENTER, TWIN FALLS"
                WHEN state_code = 'US_IX' AND facility="EAST BOISE COMMUNITY REENTRY CENTER" THEN "EAST BOISE COMMUNITY WORK CENTER, BOISE"
                WHEN state_code = 'US_IX' AND facility="NAMPA COMMUNITY REENTRY CENTER" THEN "NAMPA COMMUNITY WORK CENTER, NAMPA"
                WHEN state_code = 'US_IX' AND facility="IDAHO FALLS COMMUNITY REENTRY CENTER" THEN "IDAHO FALLS COMMUNITY WORK CENTER, IDAHO FALLS"
                WHEN state_code = 'US_IX' AND facility="TREASURE VALLEY COMMUNITY REENTRY CENTER" THEN "SICI COMMUNITY WORK CENTER"
                WHEN state_code = 'US_IX' AND facility="US MARSHAL CUSTODY" THEN "U.S. MARSHALL CUSTODY"
                WHEN state_code = 'US_IX' AND facility="FEDERAL FACILITY" THEN "FEDERAL BUREAU OF PRISONS"
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'DOUGLAS' THEN 'ASPC-DOUGLAS'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'EYMAN' THEN 'ASPC-EYMAN'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'PERRYVILLE' THEN 'ASPC-PERRYVILLE-F'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'PHOENIX' THEN 'ASPC-PHOENIX'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'LEWIS' THEN 'ASPC-LEWIS'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'SAFFORD' THEN 'ASPC-SAFFORD'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'TUCSON' THEN 'ASPC-TUCSON'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'WINSLOW' THEN 'ASPC-WINSLOW'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'YUMA' THEN 'ASPC-YUMA'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'LA PALMA' THEN 'CONTRACT'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'KINGMAN' THEN 'CONTRACT'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'RED ROCK ELOY' THEN 'CONTRACT'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'MARANA' THEN 'CONTRACT'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'CACF' THEN 'CONTRACT'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'FLORENCE WEST' THEN 'CONTRACT'
                WHEN state_code = 'US_AZ' AND UPPER(facility) = 'PHOENIX WEST' THEN 'CONTRACT'
            ELSE facility END,
            'EXTERNAL_UNKNOWN'
        ) AS facility
    """
