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

"""Defines a criteria view that shows spans of time for which supervision clients
have an assessment level of 3 or lower.

Levels meeting the criteria: HIGH DRUG (3), MODERATE (2), LOW (1)
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_LEVELS_TO_EXCLUDE = ", ".join(["'HIGH VIOLENT'", "'HIGH PROPERTY'"])

_CRITERIA_NAME = "US_CA_ASSESSMENT_LEVEL_3_OR_LOWER"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
have an assessment level of 3 or lower.

Levels meeting the criteria: HIGH DRUG (3), MODERATE (2), LOW (1)
"""

_QUERY_TEMPLATE = f"""
SELECT 
  state_code,
  person_id, 
  assessment_date AS start_date,
  score_end_date_exclusive AS end_date,
  (assessment_level_raw_text NOT IN ({_LEVELS_TO_EXCLUDE})) AS meets_criteria,
  TO_JSON(STRUCT(assessment_level_raw_text AS assessment_level, assessment_date AS assessment_date)) AS reason,
  assessment_level_raw_text AS assessment_level,
  assessment_date,
FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` 
WHERE state_code = 'US_CA'
  AND assessment_type = 'CSRA'
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_CA,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="assessment_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="Date of the assessment.",
            ),
            ReasonsField(
                name="assessment_level",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="Assessment level of the client.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
