# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Logic for calculating statistics about the time to resolution for hard failing 
validations.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_spans import (
    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_ID = (
    "validation_hard_failure_time_to_resolution"
)
VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_DESCRIPTION = (
    "Computes the total time to resolution for hard failures by month, as well as the "
    "quintiles of failure resolution for each month. Failures that span "
    "multiple months will be attributed to the month that they started failing in, not "
    "when they were resolved. If a hard failure has not yet been resolved, it will not "
    "be included (as it has not yet been resolved)."
)

VIEW_QUERY = """
WITH 
-- calculate how long each hard failure took to resolve. if a failure spanned multiple
-- months, attribute that failure to the month that the failure took place
hard_failure_resolution_time AS (
    SELECT 
        state_code,
        CAST(DATE_TRUNC(failure_datetime, MONTH) AS DATE) AS failure_month,
        DATETIME_DIFF(resolution_datetime, failure_datetime, HOUR) AS resolution_time
    FROM `{project_id}.{platform_kpis_dataset}.{validation_hard_failure_spans_table_id}`
    -- if we haven't resolved the failure, we don't know what the time to resolution was
    WHERE validation_resolution_status != 'ONGOING'
)
-- calculate some stats for the time to resolution for each month
SELECT 
  state_code,
  failure_month,
  SUM(resolution_time) as total_time_to_resolution,
  APPROX_QUANTILES(resolution_time, 4)[OFFSET(4)] as percentile_100,
  APPROX_QUANTILES(resolution_time, 4)[OFFSET(3)] as percentile_75,
  APPROX_QUANTILES(resolution_time, 4)[OFFSET(2)] as percentile_50,
  APPROX_QUANTILES(resolution_time, 4)[OFFSET(1)] as percentile_25,
  APPROX_QUANTILES(resolution_time, 4)[OFFSET(0)] as percentile_0
FROM hard_failure_resolution_time
GROUP BY state_code, failure_month
"""


VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_ID,
    description=VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_DESCRIPTION,
    platform_kpis_dataset=PLATFORM_KPIS_DATASET,
    validation_hard_failure_spans_table_id=VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.table_for_query.table_id,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_BUILDER.build_and_print()
