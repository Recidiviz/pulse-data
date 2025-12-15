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
"""Describes the spans of time when someone is serving a life sentence.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_serving_periods_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SERVING_A_LIFE_SENTENCE"

_QUERY_TEMPLATE = f"""
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute ORDER BY statute) AS eligible_offenses, LOGICAL_OR(sent.is_life) AS life_sentence)) AS reason,
        ARRAY_AGG(DISTINCT statute ORDER BY statute) AS eligible_offenses, LOGICAL_OR(sent.is_life) AS life_sentence
    {join_sentence_serving_periods_to_compartment_sessions(compartment_level_1_to_overlap='INCARCERATION')}
    WHERE sent.is_life
    GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        sessions_dataset=SESSIONS_DATASET,
        sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="eligible_offenses",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="List of offenses that make this person eligible",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
