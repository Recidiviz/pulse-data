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
"""A view revealing when task eligiblity spans are marked as eligible and almost eligible simultaneously.

To build, run:
    python -m recidiviz.validation.views.task_eligibility.eligible_and_almost_eligible_spans
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.task_eligibility import dataset_config as tes_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_VIEW_NAME = "eligible_and_almost_eligible_spans"

ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_DESCRIPTION = """
Identifies task_eligiblity spans that are marked as eligible and almost eligible within the same span. 
"""

ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_QUERY_TEMPLATE = """
SELECT
  state_code,
  state_code AS region_code,
  task_name,
  person_id,
  start_date,
  end_date
FROM `{project_id}.{task_eligibility_dataset}.all_tasks_materialized`
WHERE is_eligible AND is_almost_eligible;
"""

ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_VIEW_NAME,
    view_query_template=ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_QUERY_TEMPLATE,
    description=ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_DESCRIPTION,
    task_eligibility_dataset=tes_dataset_config.TASK_ELIGIBILITY_DATASET_ID,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ELIGIBLE_AND_ALMOST_ELIGIBLE_SPANS_VIEW_BUILDER.build_and_print()
