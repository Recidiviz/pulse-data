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
"""Identifies individuals' supervision sentences that are not from convictions in other states"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    sentence_attributes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_IN_STATE_SENTENCE_VIEW_NAME = "us_or_in_state_sentence"

US_OR_IN_STATE_SENTENCE_VIEW_DESCRIPTION = """Identifies individuals' supervision sentences that are not from convictions in other states"""

US_OR_IN_STATE_SENTENCE_QUERY_TEMPLATE = f"""
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
    )
    SELECT
        state_code,
        person_id,
        sentence_id,
        start_date,
        end_date,
        /* Here, we check that it's a sentence from an in-state conviction (i.e., a
        sentence with a four-character county code). This excludes all sentences with
        two-character state codes and the three-character 'DOC' code, which according to
        ODOC should be excluded from EDIS. Note that there are some sentences with
        two-character codes that don't appear to be state codes; these appear to be data
        errors / are from very old data, and so we just include those in the exclusion
        here. */
        (CHAR_LENGTH(county_code)=4) AS meets_criteria,
    FROM sentences
"""

US_OR_IN_STATE_SENTENCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_IN_STATE_SENTENCE_VIEW_NAME,
    description=US_OR_IN_STATE_SENTENCE_VIEW_DESCRIPTION,
    view_query_template=US_OR_IN_STATE_SENTENCE_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_IN_STATE_SENTENCE_VIEW_BUILDER.build_and_print()
