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
"""Excludes individuals under ineligible supervision types from eligibility for OR earned discharge"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_or_query_fragments import (
    OR_EARNED_DISCHARGE_INELIGIBLE_SUPERVISION_TYPES,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_OR_SUPERVISION_TYPE_ELIGIBLE"

_DESCRIPTION = """Excludes individuals under ineligible supervision types from eligibility for OR earned discharge"""

# TODO(#27325): Try to break this criterion up into multiple reusable criteria (rather than a
# single, highly-specific-to-EDIS criterion).
_QUERY_TEMPLATE = f"""
    WITH supervision_type_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            termination_date AS end_date,
            supervision_type_raw_text,
            (supervision_type_raw_text NOT IN ({list_to_query_string(
                OR_EARNED_DISCHARGE_INELIGIBLE_SUPERVISION_TYPES, quoted=True
                )})) AS is_eligible
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
        WHERE state_code='US_OR'
        -- drop zero-day periods (which have start_date=termination_date)
        AND start_date<{nonnull_end_date_clause('termination_date')}
    ),
    -- sub-sessionize in case there are overlapping supervision periods
    {create_sub_sessions_with_attributes("supervision_type_spans")}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(is_eligible) AS meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT supervision_type_raw_text ORDER BY supervision_type_raw_text) AS supervision_type)) AS reason,
        ARRAY_AGG(DISTINCT supervision_type_raw_text ORDER BY supervision_type_raw_text) AS supervision_type,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_OR,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        reasons_fields=[
            ReasonsField(
                name="supervision_type",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Raw-text supervision type(s)",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
