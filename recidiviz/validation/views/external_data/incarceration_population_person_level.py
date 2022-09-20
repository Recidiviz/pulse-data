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
"""A view containing external data for person level incarceration population to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.external_data import regions as external_data_regions

# This gets data for states with validation datasets that are still managed outside
# of version control.
_LEGACY_QUERY_TEMPLATE = """
-- Don't use facility from this ID data as it groups most of the jails together.
SELECT region_code, person_external_id, date_of_stay, NULL as facility
FROM `{project_id}.{us_id_validation_dataset}.incarceration_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, date_of_stay, facility
FROM `{project_id}.{us_id_validation_dataset}.daily_summary_incarceration`
UNION ALL 
SELECT region_code, person_external_id, date_of_stay, facility
FROM `{project_id}.{us_me_validation_dataset}.incarceration_population_person_level_view`
UNION ALL
SELECT region_code, person_external_id, date_of_stay, facility
FROM `{project_id}.{us_pa_validation_dataset}.incarceration_population_person_level_raw`
-- TODO(#10883): Ignoring this ND data for now because we are not sure that it is correct.
-- UNION ALL 
-- SELECT
--   'US_ND' as region_code,
--   Offender_ID as person_external_id,
--   DATE('2021-11-01') as date_of_stay,
--   Facility as facility
-- FROM `{project_id}.{us_nd_validation_dataset}.incarcerated_individuals_2021_11_01`
UNION ALL
SELECT
  'US_TN' as region_code,
  OffenderID as person_external_id,
  DATE(Date) as date_of_stay,
  Site as facility
FROM `{project_id}.{us_tn_raw_data_up_to_date_dateset}.TDPOP_latest`
UNION ALL
SELECT
  'US_PA' as region_code,
  person_external_id,
  date_of_stay,
  facility
FROM (
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-03-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_03_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-04-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_04_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-05-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_05_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-06-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_06_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-07-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_07_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-08-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_08_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-09-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_09_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-10-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_10_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-11-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_11_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-12-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_12_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2022-01-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2022_01_incarceration_population`
)
"""

VIEW_ID = "incarceration_population_person_level"


def get_incarceration_population_person_level_view_builder() -> SimpleBigQueryViewBuilder:
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
                  region_code, person_external_id, date_of_stay, facility
                FROM `{{project_id}}.{{{dataset_param}}}.{region_view.table_for_query.table_id}`
                """
            )

    query_template = "\nUNION ALL\n".join(region_subqueries + [_LEGACY_QUERY_TEMPLATE])

    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
        view_id=VIEW_ID,
        view_query_template=query_template,
        description="Contains external data for person level incarceration population to "
        "validate against. See http://go/external-validations for instructions on adding "
        "new data.",
        should_materialize=True,
        # Specify default values here so that mypy knows these are not used in the
        # dictionary below.
        projects_to_deploy=None,
        materialized_address_override=None,
        should_deploy_predicate=None,
        clustering_fields=None,
        # Query format arguments
        us_id_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_ID
        ),
        us_me_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_ME
        ),
        us_nd_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_ND
        ),
        us_pa_validation_dataset=dataset_config.validation_dataset_for_state(
            StateCode.US_PA
        ),
        us_tn_raw_data_up_to_date_dateset=raw_latest_views_dataset_for_region("us_tn"),
        **region_dataset_params,
    )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_incarceration_population_person_level_view_builder().build_and_print()
