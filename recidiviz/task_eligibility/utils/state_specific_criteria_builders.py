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
# =============================================================================
"""Helper methods that return state-specific criteria view builders with similar logic
that can be parameterized.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def state_specific_supervision_type_raw_text_is_not(
    *,
    criteria_name: str,
    criteria_description: str,
    ineligible_raw_text_supervision_type_condition: str,
    state_code: StateCode,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Create state-specific TES criterion view builder that checks whether a client's
    raw-text supervision type is not (one of) the specified type(s).

    Args:
        criteria_name (str): Name of the criterion.
        criteria_description (str): Description of the criterion.
        ineligible_raw_text_supervision_type_condition (str): Identifies the supervision
            types that should be considered ineligible. Must be TRUE for ineligible
            supervision types. For example: "= 'CD'" or "LIKE '%-ISC'".
        state_code (StateCode): Specifies the state for which data will be
            included.

    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: Criterion view builder.
    """

    _query_template = f"""
    WITH ineligible_supervision_type_spans AS (
        /* Pull spans of time during which someone has an ineligible supervision type
        and is therefore ineligible. */
        SELECT
            state_code,
            person_id,
            start_date,
            termination_date AS end_date,
            supervision_type_raw_text,
            FALSE AS meets_criteria,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
        WHERE supervision_type_raw_text {ineligible_raw_text_supervision_type_condition}
            AND state_code='{state_code.value}'
            -- drop zero-day periods
            AND start_date<{nonnull_end_date_clause('termination_date')}   
    ),
    -- sub-sessionize in case there are overlapping supervision periods
    {create_sub_sessions_with_attributes("ineligible_supervision_type_spans")}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(DISTINCT supervision_type_raw_text ORDER BY supervision_type_raw_text) AS supervision_type_raw_text
        )) AS reason,
        ARRAY_AGG(DISTINCT supervision_type_raw_text ORDER BY supervision_type_raw_text) AS supervision_type_raw_text,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=state_code,
        criteria_name=criteria_name,
        description=criteria_description,
        # Set default to True because we only created spans of *ineligibility* in
        # the query above, and we want to assume that folks are eligible by default
        # otherwise.
        meets_criteria_default=True,
        criteria_spans_query_template=_query_template,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        reasons_fields=[
            ReasonsField(
                name="supervision_type_raw_text",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Raw-text supervision type(s)",
            ),
        ],
    )
