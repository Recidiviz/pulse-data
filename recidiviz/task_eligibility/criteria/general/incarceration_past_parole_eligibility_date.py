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
"""Describes the spans of time when a client has past their parole eligibility date."""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "INCARCERATION_PAST_PAROLE_ELIGIBILITY_DATE"

_REASON_QUERY = f"""
WITH critical_date_spans AS
(
SELECT
    state_code,
    person_id,
    start_date AS start_datetime,
    end_date_exclusive AS end_datetime,
    group_parole_eligibility_date as critical_date
FROM `{{project_id}}.{{sentence_sessions_dataset}}.person_projected_date_sessions_materialized`
WHERE group_parole_eligibility_date IS NOT NULL
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(cd.critical_date AS eligible_date)) AS reason,
    cd.critical_date AS parole_eligibility_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_REASON_QUERY,
        sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="parole_eligibility_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Client's latest parole eligibility date",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
