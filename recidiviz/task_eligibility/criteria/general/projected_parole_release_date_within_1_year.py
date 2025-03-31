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
"""
Defines a criteria span view that shows spans of time during which
someone is within 1 year of their projected parole release date.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_V2_ALL_DATASET,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "PROJECTED_PAROLE_RELEASE_DATE_WITHIN_1_YEAR"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is within 1 year of their projected parole release date.
"""
# TODO(#38345) replace with criteria builder once `is_past_completion_date_criteria_builder` is updated
# note the end_date here is cropped at the critical_date
_QUERY_TEMPLATE = f"""
    WITH critical_date_spans AS (
      SELECT
        state_code,
        person_id,
        start_date as start_datetime,
        end_date_exclusive as end_datetime,
        group_projected_parole_release_date AS critical_date
      FROM
        `{{project_id}}.{{sentence_sessions_v2_dataset}}.person_projected_date_sessions_materialized`
      WHERE state_code = "US_IX"
        AND group_projected_parole_release_date IS NOT NULL 
    ),
      {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time = 1,
                                          date_part = 'YEAR')}
    SELECT
        state_code,
        person_id,
        start_date,
        --crop this span to end at the TPD if it does not end before so that TRUE spans end at PHD 
        --this way when we use the inverse criteria, we aren't referencing TPDs in the past
        LEAST({nonnull_end_date_clause('end_date')}, critical_date) AS end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(critical_date AS group_projected_parole_release_date)) AS reason,
        critical_date AS group_projected_parole_release_date
    FROM critical_date_has_passed_spans
    WHERE start_date < {nonnull_end_date_clause('end_date')}
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    sentence_sessions_v2_dataset=SENTENCE_SESSIONS_V2_ALL_DATASET,
    reasons_fields=[
        ReasonsField(
            name="group_projected_parole_release_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date on which the individual is projected to be released on parole.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
