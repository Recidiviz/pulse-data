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

"""A view comparing person-level incarceration population metrics in prod and staging
to the person-level values from external metrics provided by the state.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_NAME = (
    "incarceration_population_external_prod_staging_comparison"
)

INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_DESCRIPTION = """
Comparison of external, prod, and staging data on incarceration releases
"""

INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_QUERY_TEMPLATE = """
/*{description}*/
    /* TODO(#8153): Update views to only exist in or materialize in staging */
      SELECT
        region_code,
        date_of_stay,
        staging.external_data_person_external_id AS external_person_external_id,
        prod.internal_data_person_id AS prod_person_internal_id,
        staging.internal_data_person_id AS staging_person_internal_id,
        prod.external_facility AS external_facility,
        prod.internal_facility AS prod_facility,
        staging.internal_facility AS staging_facility,
      FROM `{prod_project_id}.{validation_views_dataset}.incarceration_population_person_level_external_comparison_materialized` prod
      FULL JOIN `{staging_project_id}.{validation_views_dataset}.incarceration_population_person_level_external_comparison_materialized` staging
      USING (region_code, external_data_person_external_id, date_of_stay) 

"""

INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    prod_project_id=GCP_PROJECT_PRODUCTION,
    staging_project_id=GCP_PROJECT_STAGING,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER.build_and_print()
