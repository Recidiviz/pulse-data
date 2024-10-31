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
"""Defines a criteria span view that shows spans of time during which someone has not
received an ACIS TPR date yet.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_az_query_fragments import (
    us_az_sentences_preprocessed_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_ACIS_TPR_DATE_NOT_SET"

_DESCRIPTION = __doc__

_REASONS_FIELDS = [
    ReasonsField(
        name="statutes",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Relevant statutes associated with the transition release",
    ),
    ReasonsField(
        name="descriptions",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Descriptions of relevant statutes associated with the transition release",
    ),
    ReasonsField(
        name="latest_acis_update_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Most recent date ACIS date was set",
    ),
]

_QUERY_TEMPLATE = f"""
WITH acis_set_date AS (
    SELECT
        state_code,
        person_id,
        JSON_EXTRACT_SCALAR(task_metadata, '$.sentence_group_external_id') AS sentence_group_external_id,
        SAFE_CAST(MIN(update_datetime) AS DATE) AS acis_set_date,
    FROM `{{project_id}}.normalized_state.state_task_deadline`
    WHERE task_type = 'DISCHARGE_FROM_INCARCERATION' 
        AND task_subtype = 'STANDARD TRANSITION RELEASE' 
        AND state_code = 'US_AZ' 
        AND eligible_date IS NOT NULL 
        AND eligible_date > '1900-01-01'
    GROUP BY state_code, person_id, task_metadata, sentence_group_external_id
),

sentences_preprocessed AS (
    {us_az_sentences_preprocessed_query_template()}
),

sentences_with_an_acis_date AS (
    -- This identifies all sentences who have already had a TPR date set.
    SELECT 
        sent.person_id,
        sent.state_code,
        asd.acis_set_date AS start_date,
        sent.end_date,
        sent.statute,
        sent.description,
        asd.acis_set_date,
    FROM sentences_preprocessed sent
    INNER JOIN acis_set_date asd
        ON sent.person_id = asd.person_id
            AND sent.state_code = asd.state_code
            and sent.sentence_group_external_id = asd.sentence_group_external_id
    -- We don't pull sentences where their end_date is the same date as the acis_set_date
    WHERE asd.acis_set_date != {nonnull_end_date_clause('sent.end_date')}
),

{create_sub_sessions_with_attributes('sentences_with_an_acis_date')}
 
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(statute, ', ' ORDER BY statute) AS statutes,
        STRING_AGG(description, ', ' ORDER BY description) AS descriptions,
        MAX(acis_set_date) AS latest_acis_update_date
    )) AS reason,
    STRING_AGG(statute, ', ' ORDER BY statute) AS statutes,
    STRING_AGG(description, ', ' ORDER BY description) AS descriptions,
    MAX(acis_set_date) AS latest_acis_update_date
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_AZ,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=_REASONS_FIELDS,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
