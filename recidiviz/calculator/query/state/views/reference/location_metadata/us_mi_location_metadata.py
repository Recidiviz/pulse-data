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
"""Reference table with metadata about specific
locations in MI that can be associated with a person or staff member."""

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

US_MI_LOCATION_METADATA_VIEW_NAME = "us_mi_location_metadata"

US_MI_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in MI that can be associated with a person or staff member.
"""

# TODO(#24103) Incorporate work_site_id locations into location metadata

US_MI_LOCATION_METADATA_QUERY_TEMPLATE = f"""
select
  'US_MI' AS state_code,
  location_external_id,
  location_name,
  location_type,
  CASE WHEN location_type = "SUPERVISION_LOCATION" 
  THEN 
      TO_JSON(
          STRUCT(
            location_external_id AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
            UPPER(location_name) AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
            REGION_ID AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
            REGION_NAME AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
          )
        ) 
    ELSE NULL
    END AS location_metadata,
from (
  select
    location_id as location_external_id,
    UPPER(name) as location_name,
    description,
    CASE 
      WHEN UPPER(description) IN ("ABSCONDER RECOVERY UNIT") THEN "PAROLE_VIOLATOR_FACILITY"
      WHEN UPPER(description) IN ("CENTRAL OFFICE", "CFA REGIONAL OFFICE", "FEE COLLECTION UNIT", "FOA REGIONAL OFFICE", "FOA AREA OFFICE") THEN "ADMINISTRATIVE"
      WHEN UPPER(description) IN ("CORR. CTR", "CORR. CTR/PAR OFFICE", "CORR. CTR/TRV") THEN "STATE_PRISON"
      WHEN UPPER(description) IN ("COUNTRY", "INTERSTATE COMPACT OFFICE", "OUTSIDE MDOC JURISDICTION", "STATE") THEN "OUT_OF_STATE"
      WHEN UPPER(description) IN ("COUNTY") THEN "CITY_COUNTY"
      WHEN UPPER(description) IN ("COURT") THEN "COURT"
      WHEN UPPER(description) IN ("FEDERAL CORRECTIONAL INSTITUTION") THEN "FEDERAL_PRISON"
      WHEN UPPER(description) IN ("HOSPITAL", "MDOC HEALTH CARE") THEN "MEDICAL_FACILITY"
      WHEN UPPER(description) IN ("JAIL") THEN "COUNTY_JAIL"
      WHEN UPPER(description) IN ("LAW ENFORCEMENT") THEN "LAW_ENFORCEMENT"
      WHEN UPPER(description) IN ("PAROLE OFFICE", "PROB/PAR OFFICE", "PROB/PAR RESIDENTIAL CENTER", "PROBATION OFFICE") THEN "SUPERVISION_LOCATION"
      WHEN UPPER(description) IN ("PRISON", "SPECIAL ALT INCARCERATION UNIT", "SAI") THEN "STATE_PRISON"
      WHEN UPPER(description) IN ("RESIDENTIAL REENTRY PROGRAM") THEN "RESIDENTIAL_PROGRAM"
      WHEN UPPER(description) IN ("TRV") THEN "PAROLE_VIOLATOR_FACILITY"
      WHEN UPPER(description) IN ("REP OFFICE") THEN "SUPERVISION_LOCATION"
      WHEN UPPER(name) LIKE "%ELECTRONIC MONITORING%" THEN "SUPERVISION_LOCATION"
      END AS location_type,
    REGION_NAME, 
    REGION_ID
  from `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.ADH_LOCATION_latest` loc
  left join `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.ADH_REFERENCE_CODE_latest` ref on loc.location_type_id = ref.reference_code_id 
  left join `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest` regions on UPPER(loc.name)=regions.SUPERVISION_SITE
)
"""

US_MI_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_MI_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_MI_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_MI_LOCATION_METADATA_DESCRIPTION,
    us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
