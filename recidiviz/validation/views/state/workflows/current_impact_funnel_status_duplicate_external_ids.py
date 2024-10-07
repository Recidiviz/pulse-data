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

"""A view revealing duplicate rows in `current_impact_funnel_status`."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_VIEW_NAME = (
    "current_impact_funnel_status_duplicate_external_ids"
)

CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_DESCRIPTION = """Duplicate person_external_ids in `current_impact_funnel_status` across one opportunity"""

CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_QUERY_TEMPLATE = """
SELECT
  state_code, state_code AS region_code, person_external_id, opportunity_type, COUNT(*) AS total_rows
FROM `{project_id}.{workflows_dataset}.current_impact_funnel_status_materialized`
GROUP BY region_code, person_external_id, opportunity_type
HAVING total_rows > 1
"""

CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_VIEW_NAME,
    view_query_template=CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_QUERY_TEMPLATE,
    description=CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_DESCRIPTION,
    workflows_dataset=state_dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_IMPACT_FUNNEL_STATUS_DUPLICATE_EXTERNAL_IDS_VIEW_BUILDER.build_and_print()
