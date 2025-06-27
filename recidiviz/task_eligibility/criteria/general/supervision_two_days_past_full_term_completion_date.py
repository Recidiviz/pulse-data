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
"""Defines a criteria span view that shows spans of time during which
someone is two days past their supervision full term completion date (projected max completion
date)
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_TWO_DAYS_PAST_FULL_TERM_COMPLETION_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is two days past their supervision full term completion date"""

_QUERY_TEMPLATE = f"""
    #TODO(#28370) revert once normalization fix is in
    WITH critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            IF(EXTRACT(YEAR FROM projected_completion_date_max)=9999, NULL, projected_completion_date_max) AS critical_date
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized`
    ),
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time = -2)}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(critical_date AS eligible_date)) AS reason,
        critical_date AS eligible_date,
    FROM critical_date_has_passed_spans
    """

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of client's projected supervision full term completion",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
