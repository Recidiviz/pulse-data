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
"""Defines a criteria span view that shows spans of time during which someone is serving
a supervision or supervision out of state parole term of one year or more.
This query is only relevant for states who have parole sentencing data stored separately in supervision sentences.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE"
)

_QUERY_TEMPLATE = f"""
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        CAST(DATE_DIFF(MAX(sent.projected_completion_date_max),MAX(sent.effective_date),DAY) AS INT64) >= 365 AS meets_criteria,
        TO_JSON(STRUCT(MAX(sent.projected_completion_date_max) AS projected_completion_date_max)) AS reason,
        MAX(sent.projected_completion_date_max) AS projected_completion_date_max,
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap=["SUPERVISION", "SUPERVISION_OUT_OF_STATE"])}
    WHERE sent.sentence_sub_type = "PAROLE" 
    GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="projected_completion_date_max",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Maximum projected completion date out of all sentences that a client is serving during their supervision period",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
