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
"""A view revealing when criteria spans have the same start and end date.

To build, run:
    python -m recidiviz.validation.views.state.zero_day_tes_criteria_spans
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.task_eligibility import dataset_config as tes_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

ZERO_DAY_TES_CRITERIA_SPANS_VIEW_NAME = "zero_day_tes_criteria_spans"

ZERO_DAY_TES_CRITERIA_SPANS_DESCRIPTION = """
Identifies zero-day criteria spans, i.e. spans with start_date equal to end_date.
All zero-day spans should be filtered out of criteria views.
"""

ZERO_DAY_TES_CRITERIA_SPANS_QUERY_TEMPLATE = """
SELECT
  state_code AS region_code,
  criteria_name,
  person_id,
  start_date,
  end_date
FROM `{project_id}.{task_eligibility_dataset}.all_criteria_materialized`
WHERE start_date = end_date;
"""

ZERO_DAY_TES_CRITERIA_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=ZERO_DAY_TES_CRITERIA_SPANS_VIEW_NAME,
    view_query_template=ZERO_DAY_TES_CRITERIA_SPANS_QUERY_TEMPLATE,
    description=ZERO_DAY_TES_CRITERIA_SPANS_DESCRIPTION,
    task_eligibility_dataset=tes_dataset_config.TASK_ELIGIBILITY_DATASET_ID,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ZERO_DAY_TES_CRITERIA_SPANS_VIEW_BUILDER.build_and_print()
