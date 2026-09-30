# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Describes spans of time when someone is serving an incarceration sentence
of 2 years or less. Length is the calendar duration of the longest sentence the
person is serving or has queued to serve consecutively, not the time remaining
until release, so someone serving a 5 year sentence with 18 months left does not
meet this criterion. Only prison sentences count: a probation term imposed alongside
the prison sentence, to be served after release, does not measure the incarceration."""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SERVING_INCARCERATION_SENTENCE_OF_2_YEARS_OR_LESS"

_LENGTH_COLUMN_NAME = "length_of_longest_sentence_in_days"

_DAYS_THRESHOLD = 365 * 2

_QUERY_TEMPLATE = f"""
SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date_exclusive AS end_date,
    MAX(sentence.sentence_length_days_max) AS {_LENGTH_COLUMN_NAME},
    MAX(sentence.sentence_length_days_max) <= {_DAYS_THRESHOLD} AS meets_criteria,
    TO_JSON(STRUCT(MAX(sentence.sentence_length_days_max) AS {_LENGTH_COLUMN_NAME})) AS reason,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.person_projected_date_sessions_materialized` span,
UNNEST(span.sentence_array) sentence
INNER JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized` sent
    ON span.state_code = sent.state_code
    AND span.person_id = sent.person_id
    AND sentence.sentence_id = sent.sentence_id
INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
    ON span.state_code = sess.state_code
    AND span.person_id = sess.person_id
    AND sess.compartment_level_1 = 'INCARCERATION'
    AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
    AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
WHERE sent.sentence_type = 'STATE_PRISON'
GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name=_LENGTH_COLUMN_NAME,
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Length of the longest active or queued prison sentence in days.",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
