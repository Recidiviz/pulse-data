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
"""Describes the spans of time when a TN client has a supervision session that overlaps with a sentence span.
Those who are on supervision without an overlapping span almost certainly have erroneous expiration dates
and can be surfaced as a separate "data quality" issue rather than eligible for discharge.
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HAS_ACTIVE_SENTENCE"

_DESCRIPTION = """Describes the spans of time when a TN client has a supervision session that overlaps with a sentence span.
Those who are on supervision without an overlapping span almost certainly have erroneous expiration dates
and can be surfaced as a separate "data quality" issue rather than eligible for discharge.
"""

_QUERY_TEMPLATE = f"""
    WITH sessions_and_sentence_spans AS (
       SELECT
            DISTINCT
            sess.state_code,
            sess.person_id,
            sess.start_date,
            sess.end_date_exclusive,
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
            ON span.state_code = sess.state_code
            AND span.person_id = sess.person_id
            -- Restrict to spans that overlap with supervision sessions
            AND sess.compartment_level_1 = "SUPERVISION"
            -- Use strictly less than for exclusive end_dates
            AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
            AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(
            TRUE AS has_active_sentence
        )) AS reason,
    FROM sessions_and_sentence_spans
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
