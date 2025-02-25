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

"""A view revealing opportunity types with no rows in the `current_impact_funnel_status` view."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments_metadata import (
    dataset_config as experiments_dataset_config,
)
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_VIEW_NAME = (
    "current_impact_funnel_status_no_opportunity_records"
)

CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_DESCRIPTION = """Opportunity types that have a launch date in the past and do not have any clients/residents in `current_impact_funnel_status`"""

CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_QUERY_TEMPLATE = """
SELECT
  state_code AS region_code, opportunity_type
FROM `{project_id}.{reference_views_dataset}.workflows_opportunity_configs_materialized`
INNER JOIN `{project_id}.{experiments_dataset}.officer_assignments_materialized` assignments
    USING (state_code, experiment_id)
LEFT JOIN `{project_id}.{workflows_dataset}.current_impact_funnel_status_materialized` funnel_status
    USING (state_code, opportunity_type)
WHERE funnel_status.person_external_id IS NULL
AND assignments.variant_date <= CURRENT_DATE('US/Eastern')
"""

CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_VIEW_NAME,
    view_query_template=CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_QUERY_TEMPLATE,
    description=CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_DESCRIPTION,
    reference_views_dataset=state_dataset_config.REFERENCE_VIEWS_DATASET,
    workflows_dataset=state_dataset_config.WORKFLOWS_VIEWS_DATASET,
    experiments_dataset=experiments_dataset_config.EXPERIMENTS_METADATA_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_IMPACT_FUNNEL_STATUS_NO_OPPORTUNITY_RECORDS_VIEW_BUILDER.build_and_print()
