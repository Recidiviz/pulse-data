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
A view comparing person-level supervision termination metrics in prod and staging
to the person-level values from external metrics provided by the state.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config


SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_NAME = (
    "supervision_termination_external_prod_staging_comparison"
)

SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_DESCRIPTION = """
Comparison of external, prod, and staging data on supervision terminations
"""

SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_QUERY_TEMPLATE = """
/*{description}*/
      SELECT
        /* DISTINCT is used here because we have have duplicate events for that person-day, and if the external
        data entry maps to that individual, we'd overstate our matches. There is a separate validation for whether
        a single person has duplicate events on a given day */
        # TODO(#7545): Update external, prod, and staging validation views to use internal person_id for supervision metrics
        DISTINCT region_code,
        termination_date,
        prod.external_person_external_id AS external_person,
        prod.internal_person_external_id AS prod_person,
        staging.internal_person_external_id AS staging_person,
      FROM `{prod_project_id}.{validation_views_dataset}.supervision_termination_person_level_external_comparison` prod
      FULL JOIN `{staging_project_id}.{validation_views_dataset}.supervision_termination_person_level_external_comparison` staging
      USING (region_code, external_person_external_id, termination_date) 
"""

SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    prod_project_id=GCP_PROJECT_PRODUCTION,
    staging_project_id=GCP_PROJECT_STAGING,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER.build_and_print()
