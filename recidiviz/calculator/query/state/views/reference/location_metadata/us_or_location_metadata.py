# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Reference table with metadata about specific locations in OR that can be associated
with a person or staff member.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata_key import (
    LocationMetadataKey,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_LOCATION_METADATA_VIEW_NAME = "us_or_location_metadata"

# TODO(#37673): Revisit this query and adjust as needed to ensure that we're getting
# the location type & metadata right for locations other than the county-level Community
# Corrections offices.
US_OR_LOCATION_METADATA_QUERY_TEMPLATE = f"""
    SELECT
        'US_OR' AS state_code,
        loc.LOCATION_CODE AS location_external_id,
        loc.LOCATION_NAME AS location_name,
        CASE
            WHEN (
                (loc.STATE IS NOT NULL AND loc.STATE!='OR')
                -- 'RCOM' = 'Returned to sending state - COMPACT cases only'
                OR loc.LOCATION_CODE='RCOM'
            ) THEN 'OUT_OF_STATE'
            WHEN loc.LOCATION_CODE='PVP' THEN 'PAROLE_VIOLATOR_FACILITY'
            WHEN (
                (loc.LOCATION_TYPE='C' AND loc.SUPERVISING_LOCATION='Y')
                /* We only want to count specific, physical places as supervision
                locations. To filter out locations that represent the status of a person
                more than where they are actually located, we require that at least one
                of `ADDRESS_LINE_ONE`, `CITY`, `STATE`, and `COUNTY` be non-null in
                order for us to count the location as a supervision location. */
                AND (COALESCE(loc.ADDRESS_LINE_ONE, loc.CITY, loc.STATE, loc.COUNTY) IS NOT NULL)
            ) THEN 'SUPERVISION_LOCATION'
            WHEN loc.LOCATION_TYPE='D' THEN 'ADMINISTRATIVE'
            WHEN (loc.LOCATION_TYPE='I' AND loc.SUPERVISING_LOCATION='Y') THEN 'STATE_PRISON'
            WHEN loc.LOCATION_TYPE='L' THEN 'COUNTY_JAIL'
            ELSE NULL
            END
        AS location_type,
        CASE
            /* Set up metadata for out-of-state locations. */
            WHEN (
                (loc.STATE IS NOT NULL AND loc.STATE!='OR')
                OR loc.LOCATION_CODE='RCOM'
            ) THEN
                TO_JSON(
                    STRUCT(
                        loc.DISCONTINUED_DATE IS NULL AS {LocationMetadataKey.IS_ACTIVE_LOCATION.value}
                    )
                )
            /* Set up metadata for supervision locations, including information about
            which district each location is in. */
            WHEN (
                (loc.LOCATION_TYPE='C' AND loc.SUPERVISING_LOCATION='Y')
                AND (COALESCE(loc.ADDRESS_LINE_ONE, loc.CITY, loc.STATE, loc.COUNTY) IS NOT NULL)
            ) THEN
                TO_JSON(
                    STRUCT(
                        COUNTY AS {LocationMetadataKey.COUNTY_ID.value},
                        cnty.COUNTY_NAME AS {LocationMetadataKey.COUNTY_NAME.value},
                        loc.DISCONTINUED_DATE IS NULL AS {LocationMetadataKey.IS_ACTIVE_LOCATION.value},
                        /* We treat each supervision location as an office and therefore
                        assign the `LOCATION_CODE` and `LOCATION_NAME` to be the office
                        ID and name. */
                        loc.LOCATION_CODE AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
                        loc.LOCATION_NAME AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
                        /* Senate Bill 1145 (1995) in OR shifted responsibility for many
                        felony cases from the state Department of Corrections to
                        counties. This bill looks to have been the origin of the
                        county-based Community Corrections system that remains in place
                        today.
                        For OR, we therefore treat counties as the "districts" of the
                        supervision system. Here, we assign supervision locations to
                        districts only if those locations remained active after
                        1997-01-01 (the date on which many of the provisions of SB 1145
                        appear to have taken effect). We do this because we don't want
                        to mistakenly assign locations that only existed in the old
                        system to districts of the "new" (1995) system. */
                        IF(
                            {nonnull_end_date_clause("SAFE_CAST(loc.DISCONTINUED_DATE AS DATE FORMAT 'MM/DD/YYYY')")}>='1997-01-01',
                            COUNTY,
                            NULL
                        ) AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
                        IF(
                            {nonnull_end_date_clause("SAFE_CAST(loc.DISCONTINUED_DATE AS DATE FORMAT 'MM/DD/YYYY')")}>='1997-01-01',
                            cnty.COUNTY_NAME,
                            NULL
                        ) AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
                    )
                )
            /* Set up metadata for all other locations. */
            ELSE
                TO_JSON(
                    STRUCT(
                        COUNTY AS {LocationMetadataKey.COUNTY_ID.value},
                        cnty.COUNTY_NAME AS {LocationMetadataKey.COUNTY_NAME.value},
                        loc.DISCONTINUED_DATE IS NULL AS {LocationMetadataKey.IS_ACTIVE_LOCATION.value}
                    )
                ) 
            END
        AS location_metadata,
    FROM `{{project_id}}.{{us_or_raw_data_up_to_date_dataset}}.RCDVZ_DOCDTA_TBLOCA_latest` loc
    LEFT JOIN `{{project_id}}.{{us_or_raw_data_up_to_date_dataset}}.RCDVZ_DOCDTA_TBCNTY_latest` cnty
        USING (COUNTY)
    -- exclude locations that look like test/sample data
    WHERE LOCATION_NAME!='Test location'
"""

US_OR_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_OR_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_OR_LOCATION_METADATA_QUERY_TEMPLATE,
    description=__doc__,
    us_or_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_OR,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
