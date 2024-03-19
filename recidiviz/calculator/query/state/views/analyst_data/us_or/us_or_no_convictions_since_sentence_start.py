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
"""Identifies individuals' supervision sentences with no convictions since the start of the sentence"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    sentence_attributes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_NAME = (
    "us_or_no_convictions_since_sentence_start"
)

US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_DESCRIPTION = """Identifies individuals' supervision sentences with no convictions since the start of the sentence"""

US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_QUERY_TEMPLATE = f"""
    WITH sentences AS (
        /* NB: this query pulls from sentences_preprocessed (not sentence_spans, even
        though we'll ultimately end up creating spans for eligibility). This has been
        done because if we start from sentences_preprocessed, we start with a single
        span and end up with at most two spans per sentence for each subcriterion;
        however, if we started from sentence_spans, we might start with multiple spans
        per sentence that we'd then have to work with. Also, we treat each sentence
        separately when evaluating eligibility for OR earned discharge. If we decide to
        change this in the future, we can refactor this subcriterion query to rely upon
        sentence_spans. */
        SELECT * 
        FROM ({sentence_attributes()})
        WHERE state_code='US_OR' AND sentence_type='SUPERVISION'
    ),
    sentences_with_later_convictions AS (
        SELECT
            s1.person_id,
            s1.sentence_id,
            MIN(s2.offense_date) AS next_offense_date,
        FROM sentences s1
        LEFT JOIN sentences s2
            USING (state_code, person_id)
        WHERE (s1.sentence_id != s2.sentence_id)
        AND (s2.offense_date >= s1.start_date)
        GROUP BY s1.person_id, s1.sentence_id
    ),
    critical_date_spans AS (
        SELECT
            sentences.state_code,
            sentences.person_id,
            sentences.sentence_id,
            sentences.start_date AS start_datetime,
            sentences.end_date AS end_datetime,
            next_offense_date AS critical_date,
        FROM sentences
        LEFT JOIN sentences_with_later_convictions
        USING (person_id, sentence_id)
    ),
    {critical_date_has_passed_spans_cte(attributes=['sentence_id'])}
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date,
            end_date,
            IF(critical_date_has_passed, FALSE, TRUE) AS meets_criteria,
    FROM critical_date_has_passed_spans
"""

US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_NAME,
    description=US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_DESCRIPTION,
    view_query_template=US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER.build_and_print()
