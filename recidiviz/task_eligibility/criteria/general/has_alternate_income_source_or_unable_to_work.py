# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Identifies when someone a) has an alternate income source that allows them to remain
unemployed or b) is unable to work.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HAS_ALTERNATE_INCOME_SOURCE_OR_UNABLE_TO_WORK"

_QUERY_TEMPLATE = f"""
WITH employment_periods AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        employment_status,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_employment_period`
    -- drop zero-day periods
    WHERE start_date < {nonnull_end_date_clause("end_date")}
        AND employment_status IN ('ALTERNATE_INCOME_SOURCE', 'UNABLE_TO_WORK')
),
{create_sub_sessions_with_attributes("employment_periods")}
SELECT
    state_code,
    person_id, 
    start_date,
    end_date,
    TRUE AS meets_criteria,
    ARRAY_AGG(DISTINCT employment_status ORDER BY employment_status) AS employment_statuses,
    TO_JSON(STRUCT(
        ARRAY_AGG(DISTINCT employment_status ORDER BY employment_status) AS employment_statuses
    )) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        reasons_fields=[
            ReasonsField(
                name="employment_statuses",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Current employment status(es)",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
