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
"""Identifies individuals' supervision sentences in OR for which at least half the
sentence has been served."""

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

US_OR_SERVED_HALF_SENTENCE_VIEW_NAME = "us_or_served_half_sentence"

US_OR_SERVED_HALF_SENTENCE_VIEW_DESCRIPTION = """Identifies individuals' supervision
sentences in OR for which at least half the sentence has been served."""

US_OR_SERVED_HALF_SENTENCE_QUERY_TEMPLATE = f"""
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
            *,
            /* Truncate `external_id`. (We'll use this to match incarceration sentences
            to their respective PPS sentences.) We have to truncate it so that we can
            link sentences that have the same underlying charge. (See OR ingest mappings
            to see how `external_id` is constructed.) */
            REGEXP_EXTRACT(external_id, '^[0-9]*-[0-9]*-[0-9]*-[0-9]*') AS external_id_truncated,
        FROM ({sentence_attributes()})
        WHERE state_code='US_OR'
            AND sentence_type='SUPERVISION'
    ),
    /* In cases where a client's incarceration sentence is commuted, the remaining time
    they would have spent in incarceration is spent on supervision but doesn't count as
    time accrued for EDIS. (OR made an agency decision to only count time served after
    the date on which they would have originally been released from incarceration.)
    Consequently, we can't use `max_sentence_length_days_calculated` to determine
    time-served requirements for the purposes of EDIS when someone with a commuted
    incarceration sentence is now serving their post-prison sentence, because
    `max_sentence_length_days_calculated` reflects the total time that person will spend
    on supervision and not the originally imposed PPS sentence length. In this CTE and
    those that follow, we get the original PPS sentence length for those PPS sentences
    associated with commuted incarceration sentences. */
    commuted_incarceration_sentences AS (
        SELECT
            state_code,
            person_id,
            /* Truncate `external_id`. (We'll use this to match incarceration sentences
            to their respective PPS sentences.) We have to truncate it so that we can
            link sentences that have the same underlying charge. (See OR ingest mappings
            to see how `external_id` is constructed.) */
            REGEXP_EXTRACT(external_id, '^[0-9]*-[0-9]*-[0-9]*-[0-9]*') AS external_id_truncated,
            CAST(JSON_VALUE(sentence_metadata, '$.PPS_SENTENCE_DAYS') AS INT64) AS pps_sentence_days,
            CAST(JSON_VALUE(sentence_metadata, '$.PPS_SENTENCE_MONTHS') AS INT64) AS pps_sentence_months,
            CAST(JSON_VALUE(sentence_metadata, '$.PPS_SENTENCE_YEARS') AS INT64) AS pps_sentence_years,
        FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
        WHERE state_code='US_OR'
            AND sentence_type='INCARCERATION'
            AND status='COMMUTED'
    ),
    supervision_sentences_with_commuted_incarceration_sentences AS (
        /* Here, we link PPS sentences that were preceded by commuted incarceration
        sentences with those incarceration sentences. */
        SELECT
            state_code,
            person_id,
            sentence_id,
            /* Calculate the total number of days for PPS sentences associated with
            commuted incarceration sentences. */
            DATE_DIFF(end_date,
                      DATE_SUB(DATE_SUB(DATE_SUB(end_date, INTERVAL pps_sentence_years YEAR), INTERVAL pps_sentence_months MONTH), INTERVAL pps_sentence_days DAY),
                      DAY) AS pps_sentence_length_days_calculated,
            TRUE AS preceded_by_commuted_incarceration_sentence,
        FROM sentences
        INNER JOIN commuted_incarceration_sentences
            USING (state_code, person_id, external_id_truncated)
    ),
    sentences_with_corrected_lengths AS (
        /* Here, we get the correct (for the purpose of EDIS) sentence length (in days)
        for each supervision sentence. We use `max_sentence_length_days_calculated`
        except for when supervision sentences are preceded by commuted incarceration
        sentences, in which case we use the original PPS sentence length associated with
        the commuted incarceration sentence. */
        SELECT
            sentences.* EXCEPT (max_sentence_length_days_calculated),
            IF(sswcis.preceded_by_commuted_incarceration_sentence,
               sswcis.pps_sentence_length_days_calculated,
               sentences.max_sentence_length_days_calculated
            ) AS corrected_sentence_length_days_calculated,
        FROM sentences
        LEFT JOIN supervision_sentences_with_commuted_incarceration_sentences sswcis
            USING (state_code, person_id, sentence_id)
    ),
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            /* OR accounts for time in absconsion when recording the maximum completion
            date for a sentence. We therefore don't need to account for absconsions
            ourselves. We can instead just use the sentence length and sentence end date
            to find the point in time at which someone has half their sentence
            remaining. */
            DATE_SUB(end_date,
                     INTERVAL CAST(FLOOR(corrected_sentence_length_days_calculated / 2) AS INT64) DAY
            ) AS critical_date,
        FROM sentences_with_corrected_lengths
    ),
    {critical_date_has_passed_spans_cte(attributes=['sentence_id'])}
    SELECT
        state_code,
        person_id,
        sentence_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        critical_date AS sentence_critical_date,
    FROM critical_date_has_passed_spans
"""

US_OR_SERVED_HALF_SENTENCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_SERVED_HALF_SENTENCE_VIEW_NAME,
    description=US_OR_SERVED_HALF_SENTENCE_VIEW_DESCRIPTION,
    view_query_template=US_OR_SERVED_HALF_SENTENCE_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_SERVED_HALF_SENTENCE_VIEW_BUILDER.build_and_print()
