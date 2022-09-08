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
"""A view revealing when candidate population spans that overlap for the same person.

To build, run:
    python -m recidiviz.validation.views.state.overlapping_tes_candidate_population_spans
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.task_eligibility import dataset_config as tes_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_VIEW_NAME = (
    "overlapping_tes_candidate_population_spans"
)

OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_DESCRIPTION = """
Identifies overlapping candidate population spans, i.e. spans where the next start_date
for a person comes before the previous population span end date.
"""

OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_QUERY_TEMPLATE = """
SELECT
  population_name,
  state_code AS region_code,
  person_id,
  start_date,
  end_date,
  LEAD(start_date) OVER (
    PARTITION BY population_name, state_code, person_id ORDER BY start_date
  ) AS next_start_date
FROM `{project_id}.{task_eligibility_dataset}.all_candidate_populations_materialized`
QUALIFY (end_date IS NULL AND next_start_date IS NOT NULL) OR next_start_date < end_date;
"""

OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_VIEW_NAME,
    view_query_template=OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_QUERY_TEMPLATE,
    description=OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_DESCRIPTION,
    task_eligibility_dataset=tes_dataset_config.TASK_ELIGIBILITY_DATASET_ID,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER.build_and_print()
