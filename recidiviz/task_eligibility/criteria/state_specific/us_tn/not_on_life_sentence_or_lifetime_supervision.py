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
# ============================================================================
"""Describes the spans of time during which someone in TN
is not on lifetime supervision or serving a life sentence.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION"

_DESCRIPTION = """Describes the spans of time during which someone in TN
is not on lifetime supervision or serving a life sentence.
"""

_QUERY_TEMPLATE = f"""
    WITH sentences AS (
      SELECT 
        state_code, 
        person_id,
        date_imposed AS start_date,
        DATE_ADD(projected_completion_date_max, INTERVAL 1 DAY) AS end_date,
        life_sentence AS lifetime_flag,
      FROM `{{project_id}}.{{sessions_dataset}}.us_tn_sentences_preprocessed_materialized`
      WHERE {nonnull_end_date_exclusive_clause('projected_completion_date_max')} > date_imposed
    ),
    {create_sub_sessions_with_attributes('sentences')}
    , 
    collapse_sub_sessions AS (
    -- This CTE prioritizes any life sentences or lifetime supervision on any sentence during a given sub-session
        SELECT  
                person_id, 
                state_code, 
                start_date, 
                end_date,
                LOGICAL_OR(COALESCE(lifetime_flag, FALSE)) AS lifetime_flag,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    ),
    get_earliest_start_with_lifetime AS (
    -- If someone has a life sentence or lifetime supervision on a sentence, we want to treat all following 
    -- spans of time as failing to meet the criteria, even if future sentences accrued don't have that condition
        SELECT 
            state_code,
            person_id,
            MIN(start_date) AS min_start_with_lifetime,
        FROM collapse_sub_sessions
        WHERE lifetime_flag
        GROUP BY 1,2
    )
    SELECT
        state_code,
        person_id,
        start_date,
        IF(
            MAX({nonnull_end_date_exclusive_clause('end_date')}) OVER(PARTITION BY person_id) = {nonnull_end_date_exclusive_clause('end_date')},
            NULL, 
            end_date
        ) AS end_date,
        -- if a span starts after the earliest span with a life sentence, then criteria is not met. Else, if lifetime_flag
        -- is true than criteria is not met, otherwise it is
        CASE WHEN start_date >= min_start_with_lifetime THEN FALSE ELSE NOT COALESCE(lifetime_flag, FALSE) END AS meets_criteria,
        TO_JSON(STRUCT(lifetime_flag)) AS reason,
        lifetime_flag,
    FROM collapse_sub_sessions
    LEFT JOIN
        get_earliest_start_with_lifetime
    USING(person_id, state_code)
    WHERE start_date != {nonnull_end_date_exclusive_clause('end_date')}    
    
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="lifetime_flag",
                type=bigquery.enums.SqlTypeNames.BOOL,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
