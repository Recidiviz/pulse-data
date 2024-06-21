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
"""Describes the spans of time when a TN client is serving sentences in counties that do not allow compliant reporting"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_IN_JUDICIAL_DISTRICT_17_WHILE_ON_PROBATION"

_DESCRIPTION = """Describes the spans of time when a TN client is serving sentences in counties that do not allow
compliant reporting. These means not being on probation in Judicial District 17.
"""

_QUERY_TEMPLATE = f"""
    WITH cte AS 
    (
    /*
    Get all spans where a person is serving any sentence that is from judicial district 17.
    */
    SELECT DISTINCT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive AS end_date,
        sent.judicial_district,
        sent.sentence_sub_type,
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
      WHERE span.state_code = 'US_TN'
        AND sent.judicial_district = '17'
        AND sent.sentence_sub_type IN ('PROBATION','DIVERSION')
    )
    ,
    sessionized_cte AS 
    (
    {aggregate_adjacent_spans(table_name='cte',
                       attribute=['judicial_district','sentence_sub_type'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE meets_criteria,
        TO_JSON(STRUCT(
            judicial_district AS judicial_district,
            sentence_sub_type AS sentence_type
        )) AS reason,
        judicial_district,
        sentence_sub_type AS sentence_type,
    FROM sessionized_cte
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
                name="judicial_district",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="sentence_type",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
