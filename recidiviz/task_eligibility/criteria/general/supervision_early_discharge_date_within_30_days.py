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
"""Defines a criteria span view that shows spans of time during which someone is within
30 days of their early discharge date or has passed their early discharge
date.
"""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    task_deadline_critical_date_update_datetimes_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_EARLY_DISCHARGE_DATE_WITHIN_30_DAYS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is within 30 days of their supervision early discharge eligible date, or has
passed their early discharge eligible date."""

_DAYS_BEFORE_ELIGIBLE_DATE = 30
_QUERY_TEMPLATE = f"""
/*{{description}}*/
WITH
{task_deadline_critical_date_update_datetimes_cte(
    task_type=StateTaskType.DISCHARGE_EARLY_FROM_SUPERVISION,
    critical_date_column='eligible_date')
},
{critical_date_has_passed_spans_cte(
    meets_criteria_leading_window_days=_DAYS_BEFORE_ELIGIBLE_DATE
)}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS eligible_date)) AS reason,
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
