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
"""Describes the spans of time when a TN client is serving sentences for an offense that is ineligible for CR"""
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE"

_DESCRIPTION = """Describes the spans of time when a TN client is serving sentences for an offense that is ineligible for CR.
"""

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
    FROM `{project_id}.{sessions_dataset}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    JOIN `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
    WHERE span.state_code = 'US_TN'
        -- This line restricts additionally to sentences that have not yet passed their projected completion date, filtering out 
        -- sentences who have passed that date but may have a null completion date
        AND sentences_preprocessed_id in UNNEST(sentences_preprocessed_id_array_projected_completion)
        AND (sent.is_violent_domestic OR sent.is_sex_offense OR sent.is_dui OR sent.is_violent OR sent.is_victim_under_18)
    GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
