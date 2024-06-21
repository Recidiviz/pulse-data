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
"""Describes spans of time when the latest CAF assessment for a resident did not have an override."""

from google.cloud import bigquery

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

_CRITERIA_NAME = "US_TN_LATEST_CAF_ASSESSMENT_NOT_OVERRIDE"

_DESCRIPTION = """Describes spans of time when the latest CAF assessment for a resident did not have an override."""

_QUERY_TEMPLATE = f"""
    WITH critical_dates AS (
        SELECT
            asmt.state_code,
            asmt.person_id,
            asmt.assessment_date AS start_date,
            asmt.score_end_date_exclusive AS end_date,
            NULLIF(JSON_EXTRACT_SCALAR(asmt.assessment_metadata,"$.OVERRIDEREASON"),"") AS override_reason,
        FROM
            `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` asmt
        WHERE
            assessment_type = 'CAF'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_date ORDER BY assessment_score DESC) = 1
    ),
    {create_sub_sessions_with_attributes('critical_dates')}
    , 
    dedup_cte AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date,
            -- Take non-null values if there are any
            MAX(override_reason) AS override_reason,
        FROM
            sub_sessions_with_attributes
        GROUP BY
            1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        override_reason IS NULL AS meets_criteria,
        TO_JSON(STRUCT(
            override_reason AS override_reason
        )) AS reason,
        override_reason,
    FROM dedup_cte
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        state_code=StateCode.US_TN,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="override_reason",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
