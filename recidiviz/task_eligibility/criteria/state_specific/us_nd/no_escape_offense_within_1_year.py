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
"""Describes the spans of time when someone is not within a year of being
convicted of an ESCAPE offense"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NO_ESCAPE_OFFENSE_WITHIN_1_YEAR"

RELEVANT_OFFENSE_DESCRIPTIONS = tuple(
    [
        "AIDING PRISONER ESCAPE (IDENTIFY INSTITUTION)",
        "CONSPIRACY TO COMMIT ESCAPE",
        "ESCAPE",
        "ESCAPE - (IDENTIFY INSTITUTION)",
        "FLIGHT-ESCAPE",
        "HARBORING ESCAPE/FUGITIVE",
        "POSSESSION OF ESCAPE PARAPHERNALIA",
    ]
)

_QUERY_TEMPLATE = f"""
WITH escape_offenses AS (
    SELECT 
        state_code,
        person_id, 
        offense_date AS start_date,
        DATE_ADD(offense_date, INTERVAL 1 YEAR) AS end_date,
        description AS offense_description, 
        offense_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge`
    WHERE description IN {RELEVANT_OFFENSE_DESCRIPTIONS}
    AND status = 'CONVICTED'
),
{create_sub_sessions_with_attributes(table_name='escape_offenses')}

SELECT 
    state_code, 
    person_id, 
    start_date, 
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(ARRAY_AGG(DISTINCT offense_description ORDER BY offense_description) AS offense_description,
                   ARRAY_AGG(DISTINCT offense_date ORDER BY offense_date) AS offense_dates)) AS reason,
    ARRAY_AGG(DISTINCT offense_description ORDER BY offense_description) AS offense_description,
    ARRAY_AGG(DISTINCT offense_date ORDER BY offense_date) AS offense_dates,
FROM sub_sessions_with_attributes
GROUP BY state_code, person_id, start_date, end_date"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        state_code=StateCode.US_ND,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="offense_description",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Description of the escape offenses",
            ),
            ReasonsField(
                name="offense_dates",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Dates of the escape offenses",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
