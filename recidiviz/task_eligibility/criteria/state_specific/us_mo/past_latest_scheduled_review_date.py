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
# ============================================================================
"""Spans when someone is past their latest scheduled review date.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_PAST_LATEST_SCHEDULED_REVIEW_DATE"

_DESCRIPTION = """Spans when someone is past their latest scheduled review date.
"""

_QUERY_TEMPLATE = f"""
    WITH next_review_date_spans AS (
        SELECT
            state_code,
            person_id,
            hearing_date,
            LEAD(hearing_date) OVER w as next_hearing_date,
            COALESCE(next_review_date, DATE_ADD(hearing_date, INTERVAL 30 DAY)) AS next_review_date,
            next_review_date IS NULL AS due_date_inferred,
        FROM `{{project_id}}.analyst_data.us_mo_classification_hearings_preprocessed_materialized`
        WINDOW w AS (
            PARTITION BY state_code, person_id
            ORDER BY hearing_date ASC
        )
    )
    SELECT
        state_code,
        person_id,
        next_review_date AS start_date,
        next_hearing_date AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(
            next_review_date,
            due_date_inferred
        )) AS reason,
        next_review_date,
        due_date_inferred,
    FROM next_review_date_spans
    WHERE next_review_date < {nonnull_end_date_clause('next_hearing_date')}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="next_review_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="due_date_inferred",
                type=bigquery.enums.SqlTypeNames.BOOL,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
