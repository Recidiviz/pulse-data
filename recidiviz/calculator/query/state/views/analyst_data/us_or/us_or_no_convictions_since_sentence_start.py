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
"""Identify individuals' supervision sentences in OR with no convictions since the start
of the sentence.
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

US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_NAME = (
    "us_or_no_convictions_since_sentence_start"
)

# TODO(#35320): Once we've adjusted how we're ingesting charges in OR (such that not
# every charge is ingested as 'CONVICTED'), can we rework this criterion to check a
# table of charges to find new convictions since the start of a sentence, rather than
# having to join sentences to sentences?
# TODO(#35465): Fix historical-accuracy issues in this criterion. Because the
# `CONVICTION_DATE` (which becomes the `date_imposed`) in OR doesn't always represent
# the date of actual conviction in conditional discharge or diversion cases, sentences
# for those offenses may appear to have become ineligible earlier than they actually
# did.
US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_QUERY_TEMPLATE = f"""
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
        SELECT
            sa.*,
            /* Truncate `external_id` so that we can use it to identify sentences that
            have the same underlying charge. The `external_id` in OR is composed of five
            numbers separated by hyphens (see OR ingest mappings for more info re: how
            `external_id` is constructed); for sentences for the same underlying charge,
            the first four numbers of the `external_id` will be the same across
            sentences. The `REGEXP_EXTRACT` statement below pulls out the first four
            numbers from the `external_id` (preserving the hyphenation between them). */
            REGEXP_EXTRACT(sa.external_id, '^[0-9]*-[0-9]*-[0-9]*-[0-9]*') AS external_id_truncated,
            sss.supervision_type_raw_text,
        FROM ({sentence_attributes()}) sa
        /* Join in `state_supervision_sentence` so that we can get the raw-text
        supervision type for each supervision sentence. */
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_sentence` sss
            ON sa.state_code=sss.state_code
            AND sa.person_id=sss.person_id
            AND sa.sentence_id=sss.supervision_sentence_id
        WHERE sa.state_code='US_OR'
    ),
    critical_date_spans AS (
        /* For each supervision sentence, identify the date of the earliest conviction
        for any offense(s) committed since the start of the sentence. From that date
        onwards, the sentence in question would be ineligible for EDIS due to the new
        conviction. */
        SELECT
            s1.state_code,
            s1.person_id,
            s1.sentence_id,
            s1.start_date AS start_datetime,
            s1.end_date AS end_datetime,
            MIN(s2.date_imposed) AS critical_date,
        FROM sentences s1
        /* For each sentence in `s1`, we join it to any sentence(s) for any new
        offense(s) committed since the start of the `s1` sentence. We only consider
        new offenses for which the individual has already been convicted. */
        LEFT JOIN sentences s2
            ON s1.state_code=s2.state_code
            AND s1.person_id=s2.person_id
            -- don't join sentences to themselves
            AND s1.sentence_id!=s2.sentence_id
            /* Don't join sentences with the same underlying charge, because these
            sentences are all related to the same offense, and other sentences for the
            same offense should not be considered to be new convictions. */
            AND s1.external_id_truncated!=s2.external_id_truncated
            /* We only want `s2` sentences for offenses we know occurred on or after the
            start of the `s1` sentence. */
            AND s1.start_date<=s2.offense_date
            /* Sentences with raw-text supervision types of 'C' (conditional discharge)
            or 'D' (diversion) are for charges for which an individual has not yet been
            convicted. We therefore don't want to join these in and count them as
            sentences for new convictions. (If the client ends up being convicted, we
            should see a new, subsequent, post-conviction sentence in the data for that
            charge, and we'll pick up the conviction still from that subsequent
            sentence.) */
            AND s2.supervision_type_raw_text NOT IN ('C', 'D')
        /* Filter just to supervision sentences from `s1`, since we're only interested
        in supervision sentences for EDIS. (Note that we have to do this because the
        `sentences` CTE up above returns both supervision and incarceration sentences in
        this subcriterion query.) */
        WHERE s1.sentence_type='SUPERVISION'
        GROUP BY 1, 2, 3, 4, 5
    ),
    {critical_date_has_passed_spans_cte(attributes=['sentence_id'])}
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date,
            end_date,
            /* Because `critical_date_has_passed` indicates whether a sentence has
            become *ineligible* due to a conviction for a later offense, we use NOT here
            to flip it around and create an indicator of eligibility. */
            NOT critical_date_has_passed AS meets_criteria,
    FROM critical_date_has_passed_spans
"""

US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_NAME,
    description=__doc__,
    view_query_template=US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER.build_and_print()
