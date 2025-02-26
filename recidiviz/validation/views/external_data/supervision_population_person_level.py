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
"""A view containing external data for person level supervision population to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.external_data import regions as external_data_regions

_LEGACY_QUERY_TEMPLATE = """
SELECT region_code, person_external_id, 'US_ID_DOC' as external_id_type, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_id_validation_dataset}.supervision_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, 'US_ID_DOC' as external_id_type, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_id_validation_dataset}.daily_summary_supervision`
UNION ALL
SELECT region_code, person_external_id, 'US_MO_DOC' as external_id_type, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_mo_validation_dataset}.supervision_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, 'US_PA_PBPP' as external_id_type, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_pa_validation_dataset}.supervision_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, 'US_ME_DOC' as external_id_type, date_of_supervision, district, supervising_officer, CAST(supervision_level AS STRING) AS supervision_level
FROM `{project_id}.{us_me_validation_dataset}.supervision_by_person_by_officer_view`
UNION ALL
SELECT
  'US_TN' as region_code,
  OffenderID as person_external_id,
  'US_TN_DOC' as external_id_type,
  EXTRACT(DATE FROM CAST(ReportingDate AS DATETIME)) as date_of_supervision,
  SiteID as district,
  LTRIM(StaffID, '*') as supervising_officer,
  NULL as supervision_level
FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.DailyCommunitySupervisionForRecidiviz_latest`
-- TODO(#10884): This validation data seems incorrect, so excluding it for now.
-- UNION ALL 
-- (SELECT
--     'US_ND' as region_code,
--     TRIM(SID) as person_external_id,
--     'US_ND_SID' as external_id_type,
--     DATE('2020-06-01') as date_of_supervision,
--     MIN(level_1_supervision_location_external_id) as district,
--     NULL as supervising_officer,
--     NULL as supervision_level
-- FROM `{project_id}.{us_nd_validation_dataset}.open_supervision_2020_06_01`
-- LEFT JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest`
--     ON SITE_NAME = level_1_supervision_location_name
-- GROUP BY SID)
"""

VIEW_ID = "supervision_population_person_level"


def get_supervision_population_person_level_view_builder() -> SimpleBigQueryViewBuilder:
    """Creates a builder that unions person level supervision validation data from all
    regions.
    """
    region_views = BigQueryViewCollector.collect_view_builders_in_module(
        builder_type=SimpleBigQueryViewBuilder,
        view_dir_module=external_data_regions,
        recurse=True,
        view_builder_attribute_name_regex=".*_VIEW_BUILDER",
    )

    region_subqueries = []
    region_dataset_params = {}
    # Gather all region views with a matching view id and union them in.
    for region_view in region_views:
        if region_view.view_id == VIEW_ID and region_view.should_deploy_in_project(
            metadata.project_id()
        ):
            dataset_param = f"{region_view.table_for_query.dataset_id}_dataset"
            region_dataset_params[
                dataset_param
            ] = region_view.table_for_query.dataset_id
            region_subqueries.append(
                f"""
SELECT
  region_code,
  person_external_id,
  external_id_type,
  date_of_supervision,
  district,
  supervising_officer,
  supervision_level
FROM `{{project_id}}.{{{dataset_param}}}.{region_view.table_for_query.table_id}`
"""
            )

    query_template = "\nUNION ALL\n".join(region_subqueries + [_LEGACY_QUERY_TEMPLATE])

    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
        view_id=VIEW_ID,
        view_query_template=query_template,
        description="Contains external data for person level supervision population to "
        "validate against. See http://go/external-validations for instructions on adding "
        "new data.",
        should_materialize=True,
        # Specify default values here so that mypy knows these are not used in the
        # dictionary below.
        projects_to_deploy=None,
        materialized_address_override=None,
        should_deploy_predicate=None,
        clustering_fields=None,
        us_id_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_ID
        ),
        us_me_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_ME
        ),
        us_mo_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_MO
        ),
        us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        us_nd_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_ND
        ),
        us_pa_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_PA
        ),
        us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
        ),
        **region_dataset_params,
    )


if __name__ == "__main__":
    with metadata.local_project_id_override(GCP_PROJECT_STAGING):
        get_supervision_population_person_level_view_builder().build_and_print()
