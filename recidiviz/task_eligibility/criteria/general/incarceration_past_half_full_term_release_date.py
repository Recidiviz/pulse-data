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
"""Defines a criteria span view that shows spans of time during which someone
has completed half their maximum term incarceration sentence.
"""
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
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE"

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
    SELECT
        span.state_code,
        span.person_id,
        span.start_date AS start_datetime,
        span.end_date_exclusive AS end_datetime,
        (DATE_ADD(MAX(serving.effective_date),INTERVAL
            CAST(CEILING(DATE_DIFF(MAX(sentence_projected_full_term_release_date_max),MAX(serving.effective_date),DAY))/2 AS INT64) DAY)) AS critical_date
    FROM `{{project_id}}.{{sentence_sessions_dataset}}.person_projected_date_sessions_materialized` span,
    UNNEST(sentence_array)
    INNER JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized` sent
        USING (state_code, person_id, sentence_id)
    INNER JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_start_date_materialized` serving
        USING (state_code, person_id, sentence_id)
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
        ON span.state_code = sess.state_code
        AND span.person_id = sess.person_id
        AND sess.compartment_level_1 = 'INCARCERATION'
        AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
        AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
    WHERE sent.sentence_type = 'STATE_PRISON'
    GROUP BY 1, 2, 3, 4
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS eligible_date
    )) AS reason,
    cd.critical_date AS half_max_term_release_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="half_max_term_release_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date where a client will have completed half their maximum term incarceration sentence",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
