# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

"""View to prepare location records for Workflows for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    WORKFLOWS_LEVEL_1_INCARCERATION_LOCATION_QUERY_STRING,
    WORKFLOWS_LEVEL_2_INCARCERATION_LOCATION_QUERY_STRING,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LOCATION_RECORD_VIEW_NAME = "location_record"

LOCATION_RECORD_DESCRIPTION = """
    Location records to be exported to Firestore to power Workflows.
    """

LOCATION_RECORD_QUERY_TEMPLATE = """
    WITH
    facilities AS (
        SELECT DISTINCT
            rr.state_code,
            "INCARCERATION" AS system,
            "facilityId" AS id_type,
            rr.facility_id AS id,
            IFNULL(
                CASE
                    WHEN rr.state_code = "US_TN"
                        THEN UPPER(locations.level_2_incarceration_location_name)
                    WHEN rr.state_code IN ({level_1_state_codes})
                        THEN locations.level_1_incarceration_location_name
                    WHEN rr.state_code IN ({level_2_state_codes})
                        THEN locations.level_2_incarceration_location_name
                    END,
                IF(
                    rr.state_code = "US_IX",
                    INITCAP(rr.facility_id),
                    rr.facility_id
                )
            ) AS name,
        FROM `{project_id}.{workflows_dataset}.resident_record_materialized` rr
        LEFT JOIN `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names` locations
        ON rr.facility_id = locations.level_1_incarceration_location_external_id
            AND rr.state_code = locations.state_code
    ),
    crc_facilities AS (
        SELECT DISTINCT
            state_code,
            system,
            "crcFacilityId" AS id_type,
            -- Prefixing facility ids with CRC to distinguish between CRC Facility search records (id_type = 'crcFacilityId')
            -- and regular facility records (id_type = 'facilityId')
            'CRC ' || id AS id,
            name,
        FROM facilities
        WHERE (UPPER(id) LIKE '%COMMUNITY REENTRY CENTER%' OR UPPER(id) = 'TWIN FALLS COMMUNITY WORK CENTER' OR UPPER(id) = "POCATELLO WOMEN'S CORRECTIONAL CENTER")
        AND state_code = 'US_IX'
         
    ),
    facilityUnits AS (
        SELECT DISTINCT
            rr.state_code,
            "INCARCERATION" AS system,
            "facilityUnitId" AS id_type,
            rr.facility_unit_id AS id,
            IF(rr.unit_id IS NULL,
                f.name,
                CONCAT(f.name, " / ", rr.unit_id)) AS name
        FROM `{project_id}.{workflows_dataset}.resident_record_materialized` rr
        LEFT JOIN facilities f
        ON f.state_code = rr.state_code AND f.id = rr.facility_id
    )
    SELECT {columns} FROM facilities
    UNION ALL SELECT {columns} FROM crc_facilities
    UNION ALL SELECT {columns} FROM facilityUnits
"""

LOCATION_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=LOCATION_RECORD_VIEW_NAME,
    view_query_template=LOCATION_RECORD_QUERY_TEMPLATE,
    description=LOCATION_RECORD_DESCRIPTION,
    columns=[
        "state_code",
        "system",  # Must be one of the values of the SystemId type in recidiviz-dashboards
        "id_type",  # This should be the camel-cased name of the field in the resident/client record that the id comes from
        "id",
        "name",
    ],
    level_1_state_codes=WORKFLOWS_LEVEL_1_INCARCERATION_LOCATION_QUERY_STRING,
    level_2_state_codes=WORKFLOWS_LEVEL_2_INCARCERATION_LOCATION_QUERY_STRING,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_RECORD_VIEW_BUILDER.build_and_print()
