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
# ============================================================================
"""This criteria view builder defines spans of time where clients do not have a supervision level of
 RESIDENTIAL_PROGRAM. NOTE - this criteria excludes everyone in a residential program, EVEN IF they
 have a simultaneous supervision period with a different supervision level. This is why we use
 state_supervision_period instead of supervision_level_sessions, since supervision_level_sessions
 would prioritize the non-residential supervision level.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_LEVEL_IS_NOT_RESIDENTIAL_PROGRAM"

_DESCRIPTION = __doc__

_QUERY_TEMPLATE = f"""
WITH residential_spans AS (
/* pull spans of time where a client has a residential program level */
    SELECT
        state_code,
        person_id,
        start_date,
        termination_date AS end_date,
        supervision_level_raw_text,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
    WHERE supervision_level = 'RESIDENTIAL_PROGRAM' 
), 
/* sub-sessionize and aggregate in case a client has multiple residential case types at once */
{create_sub_sessions_with_attributes('residential_spans')}
SELECT state_code,
  person_id, 
  start_date,
  end_date,
  False AS meets_criteria,
  MAX(supervision_level_raw_text) AS supervision_level_raw_text,
  TO_JSON(STRUCT(MAX(supervision_level_raw_text) AS supervision_level_raw_text)) AS reason,
FROM sub_sessions_with_attributes
WHERE start_date IS DISTINCT FROM end_date -- remove 0-day spans 
GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="supervision_level_raw_text",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Raw supervision level for the RESIDENTIAL_PROGRAM supervision span",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
