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
"""Identify individuals' supervision sentences in OR eligible for earned discharge based
on sentence imposition dates.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    sentence_attributes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_VIEW_NAME = (
    "us_or_sentence_imposition_date_eligible"
)

US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_QUERY_TEMPLATE = f"""
    WITH sentences AS (
        /* NB: this query pulls from `sentences_preprocessed` (not `sentence_spans`,
        even though we'll ultimately end up creating spans for eligibility). This has
        been done because if we start from `sentences_preprocessed`, we start with a
        single span and end up with at most two spans per sentence for each
        subcriterion; however, if we started from `sentence_spans`, we might start with
        multiple spans per sentence that we'd then have to work with. Also, we treat
        each sentence separately when evaluating eligibility for OR earned discharge. If
        we decide to change this in the future, we can refactor this subcriterion query
        to rely upon `sentence_spans`. */
        SELECT * 
        FROM ({sentence_attributes()})
        WHERE state_code='US_OR'
            AND sentence_type='SUPERVISION'
    ),
    sentence_supervision_types AS (
        SELECT
            state_code,
            person_id,
            supervision_sentence_id AS sentence_id,
            supervision_type,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_sentence`
        WHERE state_code='US_OR'
    ),
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            CASE
                /* The bill (House Bill 2172 [2021]) that expanded EDIS to post-prison
                sentences was effective 2022-01-01. This bill was only applicable for
                post-prison sentences imposed on or after the act's effective date, so
                post-prison sentences imposed on or after 2022-01-01 were eligible for
                EDIS starting on 2022-01-01. */
                /* In `state_supervision_sentence` (which is where `supervision_type`
                comes from in this case), all parole sentences are post-prison
                sentences, so when looking for post-prison sentences we can filter to
                sentences having 'PAROLE' as their supervision type. */
                WHEN ((supervision_type='PAROLE') AND (date_imposed>='2022-01-01')) THEN DATE('2022-01-01')
                /* A later bill (Senate Bill 581 [2023], which was effective 2024-01-01)
                expanded EDIS to include post-prison sentences imposed on or after
                2013-08-01. This means that post-prison sentences imposed from
                2013-08-01 through 2021-12-31 became eligible for EDIS starting on
                2024-01-01. */
                WHEN ((supervision_type='PAROLE') AND (date_imposed BETWEEN '2013-08-01' AND '2021-12-31')) THEN DATE('2024-01-01')
                /* The original bill (House Bill 3194 [2013]) that established EDIS was
                effective 2013-07-25 and made eligible any probation sentences imposed
                on or after 2013-08-01. */
                WHEN (date_imposed>='2013-08-01') THEN DATE('2013-07-25')
                /* Any other sentences (which would be those sentences imposed before
                2013-08-01) have never been eligible. */
                ELSE DATE('9999-12-31')
            END AS critical_date,
        FROM sentences
        LEFT JOIN sentence_supervision_types
            USING (state_code, person_id, sentence_id)
    ),
    {critical_date_has_passed_spans_cte(attributes=['sentence_id'])}
    SELECT
        state_code,
        person_id,
        sentence_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
    FROM critical_date_has_passed_spans
"""

US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_VIEW_NAME,
    description=__doc__,
    view_query_template=US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_VIEW_BUILDER.build_and_print()
