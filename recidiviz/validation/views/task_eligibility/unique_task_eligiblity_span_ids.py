# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A view revealing when a task_eligibility_span_id is reused within the same task/person.

To build, run:
    python -m recidiviz.validation.views.task_eligibility.unique_task_eligibility_span_ids
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.task_eligibility import dataset_config as tes_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_VIEW_NAME = "unique_task_eligibility_span_ids"

UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_DESCRIPTION = """
Identifies task_eligibility_span_ids which are used for multiple spans for the same
task and person.
"""

UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_QUERY_TEMPLATE = """
SELECT
  state_code AS region_code,
  task_name,
  person_id,
  task_eligibility_span_id,
  COUNT(*) AS num_instances_span_id
FROM `{project_id}.{task_eligibility_dataset}.all_tasks_materialized`
GROUP BY 1, 2, 3, 4
HAVING num_instances_span_id > 1;
"""

UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_VIEW_NAME,
    view_query_template=UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_QUERY_TEMPLATE,
    description=UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_DESCRIPTION,
    task_eligibility_dataset=tes_dataset_config.TASK_ELIGIBILITY_DATASET_ID,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_VIEW_BUILDER.build_and_print()
