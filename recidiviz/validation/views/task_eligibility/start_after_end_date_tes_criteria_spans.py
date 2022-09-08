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
"""A view revealing when criteria spans have a start date that comes after the end date.

To build, run:
    python -m recidiviz.validation.views.state.start_after_end_date_tes_criteria_spans
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.task_eligibility import dataset_config as tes_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

START_AFTER_END_DATE_TES_CRITERIA_SPANS_VIEW_NAME = (
    "start_after_end_date_tes_criteria_spans"
)

START_AFTER_END_DATE_TES_CRITERIA_SPANS_DESCRIPTION = """
Identifies criteria spans with a start date that comes after the end date. These spans
will not be collapsed into eligibility spans properly.
"""

START_AFTER_END_DATE_TES_CRITERIA_SPANS_QUERY_TEMPLATE = """
SELECT
  state_code AS region_code,
  criteria_name,
  person_id,
  start_date,
  end_date
FROM `{project_id}.{task_eligibility_dataset}.all_criteria_materialized`
WHERE start_date IS NOT NULL AND end_date IS NOT NULL AND start_date > end_date;
"""

START_AFTER_END_DATE_TES_CRITERIA_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=START_AFTER_END_DATE_TES_CRITERIA_SPANS_VIEW_NAME,
    view_query_template=START_AFTER_END_DATE_TES_CRITERIA_SPANS_QUERY_TEMPLATE,
    description=START_AFTER_END_DATE_TES_CRITERIA_SPANS_DESCRIPTION,
    task_eligibility_dataset=tes_dataset_config.TASK_ELIGIBILITY_DATASET_ID,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        START_AFTER_END_DATE_TES_CRITERIA_SPANS_VIEW_BUILDER.build_and_print()
