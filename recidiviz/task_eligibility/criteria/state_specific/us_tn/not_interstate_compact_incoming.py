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
"""Spans of time when someone being supervised in TN is not an incoming
interstate-compact client."""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
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

_CRITERIA_NAME = "US_TN_NOT_INTERSTATE_COMPACT_INCOMING"

_DESCRIPTION = """Spans of time when someone being supervised in TN is not an incoming
interstate-compact client."""

# TODO(#34728): If/when possible, replace this with a state-agnostic criterion that
# identifies ISC-in cases using generalized logic.
_QUERY_TEMPLATE = f"""
    WITH supervision_type_spans AS (
        SELECT DISTINCT
            state_code,
            person_id,
            start_date,
            termination_date AS end_date,
            supervision_type_raw_text,
            /* We exclude cases with raw-text supervision types ending in 'ISC', which
            indicates that a person is an incoming interstate-compact transfer. */
            (supervision_type_raw_text NOT LIKE '%-ISC') AS is_eligible,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
        WHERE state_code='US_TN'
            -- drop zero-day periods (which have start_date=termination_date)
            AND start_date<{nonnull_end_date_clause('termination_date')}
    ),
    supervision_type_spans_aggregated AS (
        SELECT
            * EXCEPT (session_id, date_gap_id)
        FROM (
            -- aggregate adjacent spans to reduce number of rows
            {aggregate_adjacent_spans(
                "supervision_type_spans",
                attribute=['supervision_type_raw_text', 'is_eligible']
            )}
        )
    ),
    -- sub-sessionize in case there are overlapping spans
    {create_sub_sessions_with_attributes("supervision_type_spans_aggregated")}
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
        state_code=StateCode.US_TN,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        reasons_fields=[
            ReasonsField(
                name="supervision_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Raw-text supervision type(s)",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
