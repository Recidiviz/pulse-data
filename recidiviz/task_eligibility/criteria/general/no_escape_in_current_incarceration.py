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
# =============================================================================
"""
Shows the spans of time during which someone is ineligible for a task because they have
an escape sentence in their current incarceration.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_V2_ALL_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_ESCAPE_IN_CURRENT_INCARCERATION"

_QUERY_TEMPLATE = """
WITH escape_related_sentences AS (
  SELECT 
    sis.state_code,
    sis.person_id,
    sis.imposed_date,
    sis.description,
  FROM `{project_id}.{sentence_sessions_dataset}.sentences_and_charges_materialized` sis
  WHERE REGEXP_CONTAINS(UPPER(sis.description), r'ESCAPE')
      AND imposed_date IS NOT NULL
)

SELECT 
  cs.state_code,
  cs.person_id,
  cs.start_date,
  cs.end_date,
  False AS meets_criteria,
  TO_JSON(STRUCT(
      ARRAY_AGG(e.description ORDER BY e.description) AS ineligible_offenses_descriptions,
      MAX(e.imposed_date) AS most_recent_escape_date
  )) AS reason,
  ARRAY_AGG(e.description ORDER BY e.description) AS ineligible_offenses_descriptions,
  MAX(e.imposed_date) AS most_recent_escape_date
FROM `{project_id}.{sessions_dataset}.incarceration_super_sessions_materialized` cs
INNER JOIN escape_related_sentences e
  ON cs.person_id = e.person_id
    AND cs.state_code = e.state_code
    AND e.imposed_date BETWEEN cs.start_date AND IFNULL(cs.end_date, '9999-12-31')
WHERE cs.start_date != cs.end_date
GROUP BY 1,2,3,4,5
ORDER BY start_date
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    sessions_dataset=SESSIONS_DATASET,
    sentence_sessions_dataset=SENTENCE_SESSIONS_V2_ALL_DATASET,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses_descriptions",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Ineligible offenses that make the person ineligible for the task",
        ),
        ReasonsField(
            name="most_recent_escape_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The most recent escape date",
        ),
    ],
    meets_criteria_default=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
