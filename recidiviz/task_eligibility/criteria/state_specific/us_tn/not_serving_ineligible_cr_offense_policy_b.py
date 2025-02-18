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
"""Describes the spans of time when a TN client is serving sentences for an offense that is ineligible for CR
Policy B"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B"

_QUERY_TEMPLATE = """
    SELECT 
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive AS end_date,
        FALSE as meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(description ORDER BY COALESCE(projected_completion_date_max,'9999-01-01')) AS ineligible_offenses,
            ARRAY_AGG(projected_completion_date_max ORDER BY COALESCE(projected_completion_date_max,'9999-01-01')) AS ineligible_sentences_expiration_date
            )) AS reason,
        ARRAY_TO_STRING(ARRAY_AGG(description ORDER BY COALESCE(projected_completion_date_max,'9999-01-01')),"") AS ineligible_offenses,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(projected_completion_date_max AS STRING) ORDER BY COALESCE(projected_completion_date_max,'9999-01-01')),"") AS ineligible_sentences_expiration_date,
    FROM `{project_id}.{sessions_dataset}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    JOIN `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
    WHERE span.state_code = 'US_TN'
        -- This line restricts additionally to sentences that have not yet passed their projected completion date, filtering out 
        -- sentences who have passed that date but may have a null completion date
        AND (sentences_preprocessed_id in UNNEST(sentences_preprocessed_id_array_projected_completion)
            -- ~2% of sentences in TN have an Active status even when the projected_completion_date_max is in the past
            -- This inclusion also considers those sentences, as long as there's no completion date
            OR status_raw_text = "AC"
            )
        AND (sent.is_violent_domestic OR sent.is_sex_offense)
    GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="The offenses someone is currently serving that make them ineligible for CR",
        ),
        ReasonsField(
            name="ineligible_sentences_expiration_date",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Expiration date for the ineligible offenses",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
