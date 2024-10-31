# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
# ============================================================================
"""Describes spans of time when someone is within 6 months of their Recidiviz-projected TPR Date"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_WITHIN_6_MONTHS_OF_RECIDIVIZ_TPR_DATE"

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
SELECT
    state_code,
    person_id,
    start_date AS start_datetime,
    end_date AS end_datetime,
    projected_tpr_date AS critical_date
FROM `{{project_id}}.{{analyst_views_dataset}}.us_az_projected_dates_materialized`
WHERE start_date != {nonnull_end_date_clause('end_date')}
),
{critical_date_has_passed_spans_cte(
    meets_criteria_leading_window_time=6,
    date_part="MONTH",
)}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS recidiviz_tpr_date
    )) AS reason,
    cd.critical_date AS recidiviz_tpr_date,
FROM critical_date_has_passed_spans cd
"""


_REASONS_FIELDS = [
    ReasonsField(
        name="recidiviz_tpr_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Projected Recidiviz TPR eligibility date",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        analyst_views_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
