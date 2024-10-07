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
"""A view revealing when there are zero currently eligible clients for a given
opportunity in task eligibility spans
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.task_eligibility import dataset_config as tes_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_VIEW_NAME = (
    "zero_eligible_current_task_eligibility_spans"
)

ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_DESCRIPTION = """
Identifies when no one is currently eligible for a given opportunity 
"""

ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_QUERY_TEMPLATE = f"""
WITH zero_current_spans AS(
SELECT
    state_code,
    state_code AS region_code, 
    task_name,
    COUNTIF(is_eligible) AS total_eligible
FROM `{{project_id}}.{{task_eligibility_dataset}}.all_tasks_materialized`
WHERE CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}
    AND state_code != 'US_ID'
GROUP BY 1,2,3
)
SELECT
    *
FROM zero_current_spans
WHERE total_eligible = 0
"""

ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_VIEW_NAME,
    view_query_template=ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_QUERY_TEMPLATE,
    description=ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_DESCRIPTION,
    task_eligibility_dataset=tes_dataset_config.TASK_ELIGIBILITY_DATASET_ID,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ZERO_ELIGIBLE_CURRENT_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER.build_and_print()
