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

"""
A view comparing person-level supervision population metrics in prod and staging
to the person-level values from external metrics provided by the state.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_NAME = (
    "supervision_population_external_prod_staging_comparison"
)

SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_DESCRIPTION = """
Comparison of external, prod, and staging data on supervision population
"""

SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_QUERY_TEMPLATE = """
/*{description}*/
      /* TODO(#8153): Update views to only exist in or materialize in staging */
      SELECT
        # TODO(#7545): Update external, prod, and staging validation views to use internal person_id for supervision metrics
        region_code,
        date_of_supervision,
        prod.external_person_external_id AS external_person,
        prod.internal_person_external_id AS prod_person,
        staging.internal_person_external_id AS staging_person,
        prod.external_district AS external_district,
        prod.internal_district AS prod_district,
        staging.internal_district AS staging_district,
        prod.external_supervision_level AS external_supervision_level,
        prod.internal_supervision_level AS prod_supervision_level,
        staging.internal_supervision_level AS staging_supervision_level,
        prod.external_supervising_officer AS external_supervising_officer,
        prod.internal_supervising_officer AS prod_supervising_officer,
        staging.internal_supervising_officer AS staging_supervising_officer,
      FROM `{prod_project_id}.{validation_views_dataset}.supervision_population_person_level_external_comparison_materialized` prod
      FULL JOIN `{staging_project_id}.{validation_views_dataset}.supervision_population_person_level_external_comparison_materialized` staging
      USING (region_code, external_person_external_id, date_of_supervision)     
"""

SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    prod_project_id=GCP_PROJECT_PRODUCTION,
    staging_project_id=GCP_PROJECT_STAGING,
    projects_to_deploy={GCP_PROJECT_PRODUCTION},
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER.build_and_print()
