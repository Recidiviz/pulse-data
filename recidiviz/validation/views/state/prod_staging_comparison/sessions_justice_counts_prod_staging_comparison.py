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

"""Part 2 of creating a view comparing sessions population counts in prod and staging
to the justice counts population counts. This view combines staging with prod sessions.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_VIEW_NAME = (
    "sessions_justice_counts_prod_staging_comparison"
)

SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_DESCRIPTION = """
Comparison of justice counts, sessions staging, and sessions prod data on prison, parole, and probation populations
"""

SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_QUERY_TEMPLATE = """
/* Joins the staging sessions + justice counts view with the prod sessions + justice counts view. */

    SELECT
      staging.* EXCEPT(sessions_value),
      staging.sessions_value AS staging_sessions_value,
      prod.sessions_value AS prod_sessions_value,
    FROM `{staging_project_id}.{validation_views_dataset}.sessions_justice_counts_comparison_materialized` staging 
    INNER JOIN `{prod_project_id}.{validation_views_dataset}.sessions_justice_counts_comparison_materialized` prod
    USING (state_code, metric, date_reported)
"""

SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_VIEW_NAME,
    view_query_template=SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_QUERY_TEMPLATE,
    description=SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
    staging_project_id=GCP_PROJECT_STAGING,
    prod_project_id=GCP_PROJECT_PRODUCTION,
    projects_to_deploy={GCP_PROJECT_PRODUCTION},
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_VIEW_BUILDER.build_and_print()
