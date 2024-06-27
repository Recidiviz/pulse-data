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
"""Reference table with metadata about specific locations in AR that can be associated 
with a person or staff member."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata_key import (
    LocationMetadataKey,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AR_LOCATION_METADATA_VIEW_NAME = "us_ar_location_metadata"

US_AR_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in AR that can be associated with a person or staff member.
See the 'AR Location Data' notion page for further documentation:

https://www.notion.so/recidiviz/AR-Location-Data-19ef2cc46465457a8ddf73b2f65625f1?pvs=4
"""

US_AR_LOCATION_METADATA_QUERY_TEMPLATE = f"""
with counties_to_areas AS (
    -- Maps AR counties to the supervision area they belong to, associating each party
    -- that has ORGANIZATIONTYPE 'Z2' (which are the counties in AR) with its numbered area.
    SELECT 
        PARTYID,
        UORGANIZATIONNAME AS county_name,
        CASE 
            WHEN UORGANIZATIONNAME in ('BENTON','WASHINGTON','MADISON') THEN 'FAYETTEVILLE P&P'
            WHEN UORGANIZATIONNAME in ('CARROLL','BOONE','NEWTON','MARION','SEARCY','BAXTER','FULTON','IZARD','STONE','VAN BUREN','CLEBURNE','INDEPENDENCE') THEN 'MOUNTAIN HOME'
            WHEN UORGANIZATIONNAME in ('RANDOLPH','LAWRENCE','SHARP','JACKSON','WHITE','PRAIRIE','LONOKE','WOODRUFF','CROSS','ST. FRANCIS') THEN 'SEARCY'
            WHEN UORGANIZATIONNAME in ('CLAY','GREENE','CRAIGHEAD','MISSISSIPPI','POINSETT','CRITTENDEN') THEN 'JONESBORO'
            WHEN UORGANIZATIONNAME in ('CRAWFORD','FRANKLIN','JOHNSON','SEBASTIAN','LOGAN','SCOTT','POLK','MONTGOMERY') THEN 'FORT SMITH'
            WHEN UORGANIZATIONNAME in ('POPE','CONWAY','FAULKNER','PERRY','YELL') THEN 'CONWAY'
            -- TODO(#30626): Area 8 will need to be split into 7 and 8. See ticket and
            -- comment below for more information.
            WHEN UORGANIZATIONNAME in ('PULASKI') THEN 'UNKNOWN'
            WHEN UORGANIZATIONNAME in ('SEVIER','HOWARD','PIKE','LITTLE RIVER','HEMPSTEAD','NEVADA','MILLER','LAFAYETTE','COLUMBIA') THEN 'TEXARKANA P & P'
            WHEN UORGANIZATIONNAME in ('GARLAND','SALINE','HOT SPRING','GRANT','CLARK','DALLAS','OUACHITA','CALHOUN') THEN 'HOT SPRINGS'
            WHEN UORGANIZATIONNAME in ('JEFFERSON','ARKANSAS','CLEVELAND','LINCOLN','DESHA','BRADLEY','DREW','UNION','ASHLEY','CHICOT','MONROE','LEE','PHILLIPS') THEN 'PINE BLUFF P&P'
            ELSE NULL 
        END AS county_area
    FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.ORGANIZATIONPROF_latest`
    WHERE ORGANIZATIONTYPE = 'Z2'
),
locs_used_as_offices AS (
    /*
    Some locations that don't have location_type = 'SUPERVISION_LOCATION' are still
    treated as supervision offices in the raw data. Therefore, we set supervision_office_id
    and supervision_office_name for all locations that show up as offices in the supervision
    data, rather than only setting these metadata fields for SUPERVISION_LOCATION type locations.
    */
    SELECT DISTINCT PPOFFICE 
    FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.SUPERVISIONEVENT_latest`
),
all_organizations AS (
    SELECT
        'US_AR' AS state_code,
        op.PARTYID AS location_external_id,
        op.UORGANIZATIONNAME AS location_name,
        CASE 
            WHEN op.ORGANIZATIONTYPE IN (
                'B5', -- Regional Jail
                'B6', -- County Jail Contract Condition
                'B7', -- County 309-In Jail
                'B8', -- County Jail Backup
                'BA', -- City Jail Contract Conditional
                'BB', -- City 309-In Jail
                'BC', -- City Jail Backup
                'E1' -- County Jail/Sheriff
            ) THEN 'COUNTY_JAIL'
            WHEN op.ORGANIZATIONTYPE = 'B1' THEN 'STATE_PRISON' -- State Prison Unit
            WHEN op.ORGANIZATIONTYPE = 'U5' THEN 'FEDERAL_PRISON' -- Federal Prison
            /* 
            Parole violator facilities are hardcoded using PARTYID, as they look like 
            county jails or CCCs based on their organization type, but serve a specific
            function not identifiable otherwise.
            */
            WHEN op.PARTYID IN (
                '1600087',
                '1245923',
                '0262913',
                '3623271',
                '4113301',
                '0435284'
            ) THEN 'PAROLE_VIOLATOR_FACILITY'
            WHEN op.ORGANIZATIONTYPE IN (
                'D5', -- Community Corrections Center
                'E9' -- Transitional Living
            ) THEN 'RESIDENTIAL_PROGRAM'
            WHEN op.ORGANIZATIONTYPE IN (
                'C9', -- ACC Interstate Compact,
                'D2', -- Region Office
                'D3', -- Area Office
                'D4', -- Satellite Office
                'D6', -- Day Reporting Center
                'D7', -- Drug Court
                'DH', -- Hope Court
                'DS' -- Swift Court
            ) THEN 'SUPERVISION_LOCATION'
            WHEN op.ORGANIZATIONTYPE IN (
                'BG', -- Inmate Hospital
                'E6', -- County Health Department
                'E7', --County Mental Health Dept
                'G1', -- Hospital (Outside)
                'G2', -- Clinic (Outside)
                'G3', -- Doctor's Office (Outside)
                'G4', -- Other Medical (Outside)
                'G5' -- Mental Health Treatmt(Outside)
            ) THEN 'MEDICAL_FACILITY'
            WHEN op.ORGANIZATIONTYPE IN (
                'E2', -- Arkansas State Court
                'U9' -- US District Court
            ) THEN 'COURT'
            WHEN op.ORGANIZATIONTYPE = 'ES' THEN 'LAW_ENFORCEMENT' -- State Police
            WHEN op.ORGANIZATIONTYPE IN (
                'A1', -- ADC Director's Office
                'A2', -- ADC Central Office Accounting
                'A3', -- Central Office Classification
                'A4', -- Central Office Security
                'A5', -- Central Office Programs
                'A6', -- Central Office Records
                'AS', -- Central Office SOSRA	
                'C1', -- ACC Director's Office	
                'C2', -- ACC Central Office Accounting
                'C3', -- ACC Residential Services
                'C5', -- ACC Institu. Parole Services
                'U6', -- FBI Office
                'U7' -- DEA Office
            ) -- 'P_' organizations are all government agencies (generally federal).
            OR op.ORGANIZATIONTYPE LIKE 'P%' THEN 'ADMINISTRATIVE'
            WHEN op.ORGANIZATIONTYPE IN (
                -- Note: the description for ORGANIZATIONTYPE 'Z2' specifies 'home state',
                -- but this value is only used for AR counties or 'Unknown'.
                'E4', -- City Name
                'Z2' -- County (in Home State)
            ) THEN 'CITY_COUNTY'
            WHEN op.ORGANIZATIONTYPE LIKE 'I%' THEN 'OUT_OF_STATE'
            ELSE NULL
        END AS location_type,
        TO_JSON(
            STRUCT(
                a.COUNTY AS {LocationMetadataKey.COUNTY_ID.value},
                rp.PARTYRELSTATUS = 'A' AS {LocationMetadataKey.IS_ACTIVE_LOCATION.value},
                op.ORGCOMMONID AS {LocationMetadataKey.LOCATION_ACRONYM.value},
                op3.PARTYID AS {LocationMetadataKey.FACILITY_GROUP_EXTERNAL_ID.value},
                op3.UORGANIZATIONNAME AS {LocationMetadataKey.FACILITY_GROUP_NAME.value},
                ha.facility_security_level AS {LocationMetadataKey.FACILITY_SECURITY_LEVEL.value},
                COALESCE(area_name,inferred_area_name) AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value},
                CASE COALESCE(area_name,inferred_area_name)
                    WHEN 'FAYETTEVILLE P&P' THEN '1'
                    WHEN 'MOUNTAIN HOME' THEN '2'
                    WHEN 'SEARCY' THEN '3'
                    WHEN 'JONESBORO' THEN '4'
                    WHEN 'FORT SMITH' THEN '5'
                    WHEN 'CONWAY' THEN '6'
                    WHEN 'LR PROBATION' THEN '7'
                    WHEN 'LITTLE ROCK PAROLE' THEN '8'
                    WHEN 'TEXARKANA P & P' THEN '9'
                    WHEN 'HOT SPRINGS' THEN '10'
                    WHEN 'PINE BLUFF P&P' THEN '11'
                    WHEN 'UNKNOWN' THEN 'UNKNOWN'
                    ELSE NULL
                END AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
                -- If a location is treated as a supervision office in the data, then we
                -- reuse the location_external_id and location_name as the supervision_office_id
                -- and supervision_office_name.
                CASE 
                    WHEN op.PARTYID IN (SELECT * FROM locs_used_as_offices) THEN op.PARTYID 
                    ELSE NULL
                END AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
                CASE 
                    WHEN op.PARTYID IN (SELECT * FROM locs_used_as_offices) THEN op.UORGANIZATIONNAME 
                    ELSE NULL
                END AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value}
            )
        ) AS location_metadata

    FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.ORGANIZATIONPROF_latest` op

    LEFT JOIN (
        /*
        Get the most recent '9AA' (location -> address) relationship based on PARTYRELEND
        (no coalescing is needed for open relationship periods, as non-existent dates are 
        encoded with a dummy date in the year 9999).

        The PARTYRELSTATUS of this relationship is used to check for active locations,
        and since the RELATEDPARTYID for '9AA' relationships is an address, it's used in the
        next join to obtain a location's active address (currently just being used for
        COUNTY_ID).
        */

        SELECT * 
        FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.RELATEDPARTY_latest`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PARTYID, PARTYRELTYPE ORDER BY PARTYRELEND DESC) = 1
    ) rp
    ON op.PARTYID = rp.PARTYID AND rp.PARTYRELTYPE = '9AA'

    LEFT JOIN `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.ADDRESS_latest` a
    ON rp.RELATEDPARTYID = a.ADDRESSID  
    /*
    Organizations have a department code, which corresponds to a parent organization (or
    the organization itself when there are no parents, such that PARTYID = ORGDEPTCODE).
    To identify a location's facility group, we sometimes need to go one level higher, as
    certain parent organizations are still too specific. By performing this join twice, we
    ensure the facility group is the top-level organization (and in cases where the second
    join isn't necessary, the first parent organization will simply be joined back to itself).

    This second join is most often needed for 'canteen' locations. For example, the party
    indicated by the ORGDEPTCODE for the 'Central AR DCC Canteen' location is 'Central AR CCC - Males',
    which is a parent location but not the top-level location we want for facility group. 
    Therefore, we need the second join to get the department code for 'Central AR CCC - Males',
    which returns 'DCC Central Office': the correct top-level facility group.
    */
    LEFT JOIN  `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.ORGANIZATIONPROF_latest` op2
    ON op.ORGDEPTCODE = op2.PARTYID

    LEFT JOIN  `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.ORGANIZATIONPROF_latest` op3
    ON op2.ORGDEPTCODE = op3.PARTYID
    /*
    Custody level is associated with housing areas within facilities, which is more granular than
    this reference view is designed for. Therefore, facility security level metadata 
    is only specified for locations with the same custody level designation across housing
    areas. Facilities containing mixed security levels are designated as 'MULTIPLE'.
    */
    LEFT JOIN (
        SELECT 
            PARTYID,
            CASE WHEN mixed_custody THEN 'MULTIPLE' 
            ELSE custody END AS facility_security_level
        FROM (
            SELECT 
                PARTYID, 
                COUNT(DISTINCT HIGHESTCUSTODYALLOWED) > 1 AS mixed_custody, 
                ANY_VALUE(HIGHESTCUSTODYALLOWED) AS custody
            FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.HOUSINGAREA_latest`
            GROUP BY PARTYID
        )
    ) ha
    ON op.PARTYID = ha.PARTYID
    /*
    Supervision locations can serve multiple counties, which should all fall within a 
    single supervision area. Supervision areas are groupings of multiple contiguous counties,
    hardcoded in the counties_to_areas CTE above. In the metadata, supervision_district
    represents the supervision area containing the counties served by a supervision location.

    Here, joins each office ID with an external reference table and then joins with the 
    area(s) served based on 'county served' relationships in the RELATEDPARTYTABLE. Data
    from the external reference table is more reliable and therefore prioritized first 
    when setting supervision district. Though supervision locations should serve a single 
    supervision area, those that serve more have their district set to 'UNKNOWN'. 

    Note that supervision area boundaries (and in turn, the set of counties / supervision 
    locations within the area) aren't fixed and have changed over time. The county-to-area
    mappings in counties_to_areas are current as of June 2024.
    */
    LEFT JOIN (
        SELECT DISTINCT 
            office_id, 
            area_name
        FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_OFFICE_LOCATIONS_latest` 
    ) office_ref
    ON op.PARTYID = office_ref.office_id

    LEFT JOIN (
        SELECT 
            rp_sup_areas.PARTYID,
            CASE 
                WHEN 
                    COUNT(DISTINCT cta.county_area) = 1 
                    THEN ANY_VALUE(CAST(cta.county_area AS STRING)) 
                WHEN 
                    COUNT(DISTINCT cta.county_area) > 1 
                    THEN 'UNKNOWN'
            ELSE NULL 
            END AS inferred_area_name
        FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.RELATEDPARTY_latest` rp_sup_areas
        LEFT JOIN counties_to_areas cta
        ON rp_sup_areas.RELATEDPARTYID = cta.PARTYID
        -- Only use relationships with an 'Active' status and a 'County Served' relationship type.
        WHERE PARTYRELSTATUS = 'A' AND PARTYRELTYPE='8BA'
        GROUP BY rp_sup_areas.PARTYID
    ) rp2
    ON op.PARTYID = rp2.PARTYID
) 

SELECT *
FROM all_organizations
WHERE location_type IS NOT NULL
"""

US_AR_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_AR_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_AR_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_AR_LOCATION_METADATA_DESCRIPTION,
    us_ar_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AR, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
