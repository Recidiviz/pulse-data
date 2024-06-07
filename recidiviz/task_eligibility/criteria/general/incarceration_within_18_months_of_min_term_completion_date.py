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
# =============================================================================
"""
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 18 months of their minimum term completion date.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "INCARCERATION_WITHIN_18_MONTHS_OF_MIN_TERM_COMPLETION_DATE"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 18 months of their minimum term completion date.
"""
# TODO(#22785) add projected_completion_date_min to [compertment_level_1]_projected_completion_date

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
    SELECT 
        * EXCEPT (projected_completion_date_min),
        {revert_nonnull_end_date_clause('projected_completion_date_min')} AS critical_date
    FROM (
        SELECT
            span.state_code,
            span.person_id,
            span.start_date AS start_datetime,
            span.end_date_exclusive AS end_datetime,
            MAX(sent.projected_completion_date_min) AS projected_completion_date_min,
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
        UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
            ON span.state_code = sess.state_code
            AND span.person_id = sess.person_id
            -- Restrict to spans that overlap with particular compartment levels
            AND compartment_level_1 = 'INCARCERATION'
            -- Use strictly less than for exclusive end_dates
            AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
            AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
        WHERE span.state_code = 'US_IX'
        GROUP BY 1, 2, 3, 4
        )
    ),
    {critical_date_has_passed_spans_cte(548)}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS min_term_completion_date)) AS reason,
    critical_date AS min_term_completion_date,
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="min_term_completion_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
