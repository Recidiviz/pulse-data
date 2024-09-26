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

"""A view comparing experiments_metadata_large in prod and staging.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

EXPERIMENTS_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_VIEW_NAME = (
    "experiment_assignments_large_prod_staging_comparison"
)

EXPERIMENTS_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_DESCRIPTION = """
A view comparing experiments_metadata_large in prod and staging.
"""

EXPERIMENTS_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_QUERY_TEMPLATE = """
      SELECT
        DISTINCT 
            experiment_id, state_code, unit_id, variant_id, unit_type, variant_date
      FROM `{prod_project_id}.manually_updated_source_tables.experiment_assignments_large` prod
      FULL JOIN `{staging_project_id}.manually_updated_source_tables.experiment_assignments_large` staging
      USING (experiment_id, state_code, unit_id, variant_id, unit_type, variant_date) 
"""

EXPERIMENT_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=EXPERIMENTS_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_VIEW_NAME,
    view_query_template=EXPERIMENTS_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_QUERY_TEMPLATE,
    description=EXPERIMENTS_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_DESCRIPTION,
    prod_project_id=GCP_PROJECT_PRODUCTION,
    staging_project_id=GCP_PROJECT_STAGING,
    projects_to_deploy={GCP_PROJECT_PRODUCTION},
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EXPERIMENT_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_VIEW_BUILDER.build_and_print()
