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

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
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

_CRITERIA_NAME = "INCARCERATION_PAST_PAROLE_ELIGIBILITY_DATE"

_DESCRIPTION = """Describes the spans of time when a client has past their parole eligibility date."""

_REASON_QUERY = f"""
WITH critical_date_spans AS (
    SELECT
        span.state_code,
        span.person_id,
        span.start_date AS start_datetime,
        span.end_date AS end_datetime,
        MAX(sent.parole_eligibility_date) AS critical_date,
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap='INCARCERATION')}
    WHERE
        sent.effective_date < {nonnull_end_date_clause('sent.projected_completion_date_max')}
        AND sent.sentence_type = 'INCARCERATION'
        AND sent.parole_eligibility_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
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
        description=_DESCRIPTION,
        criteria_spans_query_template=_REASON_QUERY,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="parole_eligibility_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
