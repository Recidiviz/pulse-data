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
"""Identifies individuals' supervision sentences that fall under eligible statutes"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
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
from recidiviz.task_eligibility.utils.us_or_query_fragments import (
    OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES,
    OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES_POST_PRISON,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_STATUTE_ELIGIBLE_VIEW_NAME = "us_or_statute_eligible"

US_OR_STATUTE_ELIGIBLE_VIEW_DESCRIPTION = """Identifies individuals' supervision sentences that fall under eligible statutes"""

US_OR_STATUTE_ELIGIBLE_QUERY_TEMPLATE = f"""
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
    sentence_supervision_types AS (
        SELECT
            state_code,
            person_id,
            supervision_sentence_id AS sentence_id,
            supervision_type_raw_text,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_sentence`
        WHERE state_code='US_OR'
    ),
    sentence_enhancements AS (
        SELECT
            state_code,
            person_id,
            sentence_id,
            -- the statement below will return FALSE if the 137.635 flag is missing
            JSON_VALUE(sentence_metadata, '$.FLAG_137635')='Y' AS sentenced_under_137635,
        FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
        WHERE state_code='US_OR' AND sentence_type='SUPERVISION'
    ),
    sentence_statute_eligibility AS (
        /* Here, we determine whether sentences fall under eligible statutes (according
        to the policy outlined in ORS 137.633, as of early 2024). We explicitly handle
        cases where the statute is missing, allowing these cases to pass through as
        eligible. */
        SELECT DISTINCT
            state_code,
            person_id,
            sentence_id,
            (
            -- check that statute isn't universally ineligible
            IFNULL((statute NOT IN ({list_to_query_string(OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES, quoted=True)})), TRUE)
            -- exclude sentences imposed under ORS 137.635, which is a sentencing enhancement
            AND (NOT sentenced_under_137635)
            -- check that if post-prison, statute isn't ineligible for post-prison cases
            AND NOT ((supervision_type_raw_text='O') AND IFNULL(statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES_POST_PRISON, quoted=True)}), FALSE))
            ) AS sentenced_under_eligible_statute,
        FROM sentences
        LEFT JOIN sentence_supervision_types
            USING (state_code, person_id, sentence_id)
        LEFT JOIN sentence_enhancements
            USING (state_code, person_id, sentence_id)
    ),
    critical_date_spans AS (
        /* While the statute exclusions list has been constant (as far as we know) since
        its introduction, this introduction of these exclusions did happen partway
        through the existence of the EDIS program. Here, we account for these historical
        changes. NB: here, we're creating spans of INELIGIBILITY (and will flip these to
        spans of eligibility later). */
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            CASE
                /* The original bill (House Bill 3194 [2013]) that established EDIS was
                effective 2013-07-25 and does not appear to have specified any statute
                exclusions.
                This means that if someone was sentenced under an eligible statute, that
                sentence has been eligible for EDIS since the introduction of the
                program (and so has/will never become ineligible). */
                WHEN (sentenced_under_eligible_statute) THEN DATE('9999-12-31')
                /* The bill (House Bill 2172 [2021]) that expanded EDIS to post-prison
                sentences was effective 2022-01-01, applied to sentences imposed on or
                after 2022-01-01, and appears to have introduced the
                statute exclusions.
                This means that if someone was sentenced under an ineligible statute
                (and that sentence was imposed on or after 2022-01-01), they became
                ineligible starting on 2022-01-01. (In effect, they will never be
                eligible.) */
                WHEN ((NOT sentenced_under_eligible_statute) AND (date_imposed>='2022-01-01')) THEN DATE('2022-01-01')
                /* A later bill (Senate Bill 581 [2023], which was effective 2024-01-01)
                applied the changes from the 2021 bill to all sentences imposed before
                2022-01-01 as well.
                This means that if someone was sentenced under an ineligible statute
                (and that sentence was imposed before 2022-01-01), they became
                ineligible starting on 2024-01-01. */
                WHEN ((NOT sentenced_under_eligible_statute) AND (date_imposed<'2022-01-01')) THEN DATE('2024-01-01')
            END AS critical_date,
        FROM sentences
        LEFT JOIN sentence_statute_eligibility
            USING (state_code, person_id, sentence_id)
    ),
    {critical_date_has_passed_spans_cte(attributes=['sentence_id'])}
    SELECT
        state_code,
        person_id,
        sentence_id,
        start_date,
        end_date,
        /* Recall that we created spans of INELIGIBILITY so far, so we need to use the
        NOT here to flip those around and get spans of eligibility. */
        NOT critical_date_has_passed AS meets_criteria,
    FROM critical_date_has_passed_spans
"""

US_OR_STATUTE_ELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_STATUTE_ELIGIBLE_VIEW_NAME,
    description=US_OR_STATUTE_ELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_OR_STATUTE_ELIGIBLE_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_STATUTE_ELIGIBLE_VIEW_BUILDER.build_and_print()
