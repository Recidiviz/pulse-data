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
"""Identifies individuals' supervision sentences for which they have served at least 6 months of the sentence"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
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

US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_NAME = "us_or_served_6_months_supervision"

US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_DESCRIPTION = """Identifies individuals' supervision sentences for which they have served at least 6 months of the sentence"""

US_OR_SERVED_6_MONTHS_SUPERVISION_QUERY_TEMPLATE = f"""
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
        SELECT
            * EXCEPT (start_date, end_date),
            start_date AS sentence_start_date,
            end_date as sentence_end_date,
        FROM ({sentence_attributes()})
        WHERE state_code='US_OR' AND sentence_type='SUPERVISION'
    ),
    absconsion_sessions AS (
        -- pull all sessions of absconsion in OR
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
        WHERE state_code='US_OR' AND compartment_level_2='ABSCONSION'
    ),
    absconsions_during_sentence AS (
        /* Get all spans of time when someone has absconded during a supervision
        sentence in OR. Spans are at the person-sentence level (such that each session
        of absconsion is tied to a specific sentence ID). */
        {create_intersection_spans(table_1_name='sentences',
                                   table_2_name='absconsion_sessions',
                                   index_columns=['state_code', 'person_id'],
                                   table_1_columns=['sentence_id'],
                                   table_1_start_date_field_name='sentence_start_date',
                                   table_1_end_date_field_name='sentence_end_date')}
    ),
    sentence_and_absconsion_spans AS (
        /* Combine all sentence and absconsion spans in one table, in preparation for
        creating sub-sessions. */
        SELECT
            state_code,
            person_id,
            sentence_id,
            sentence_start_date AS start_date,
            sentence_end_date AS end_date,
            CAST(NULL AS BOOL) AS absconded,
            0 AS days_absconded,
        FROM sentences
        UNION ALL
        SELECT
            * EXCEPT (end_date_exclusive),
            end_date_exclusive AS end_date,
            TRUE AS absconded,
            DATE_DIFF(end_date_exclusive, start_date, DAY) AS days_absconded,
        FROM absconsions_during_sentence
    ),
    {create_sub_sessions_with_attributes("sentence_and_absconsion_spans", index_columns=["state_code", "person_id", "sentence_id"])},
    sub_sessions_with_attributes_condensed AS (
        /* Aggregate across sub-sessions to get attributes for each span of time for
        each person-sentence. */
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date,
            end_date,
            COALESCE(LOGICAL_OR(absconded), FALSE) AS absconded,
            SUM(days_absconded) AS days_absconded,
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4, 5
    ),
    sub_sessions_with_attributes_condensed_with_cumulative_days AS (
        SELECT
            *,
            /* Calculate the running cumulative total number of days in absconsion for a
            given person-sentence. */
            SUM(days_absconded) OVER(PARTITION BY state_code, person_id, sentence_id ORDER BY start_date) AS cumulative_days_absconded,
        FROM sub_sessions_with_attributes_condensed
    ),
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            /* If someone has absconded, set the critical date to '9999-12-31' (because
            someone can't reach their critical date while they're in absconsion, which 
            effectively "stops the clock" on their time served). For spans of time
            preceded by days in absconsion, push out the critical date by the cumulative
            number of days in absconsion, such that days in absconsion don't count
            toward the 6-month minimum. */
            IF(absconded,
               '9999-12-31',
               DATE_ADD(DATE_ADD(sentence_start_date, INTERVAL 6 MONTH), INTERVAL cumulative_days_absconded DAY)
            ) AS critical_date,
        FROM sub_sessions_with_attributes_condensed_with_cumulative_days
        LEFT JOIN sentences
            USING (state_code, person_id, sentence_id)
    ),
    {critical_date_has_passed_spans_cte(attributes=['sentence_id'])},
    critical_date_has_passed_spans_aggregated AS (
        -- aggregate adjacent spans to reduce number of rows
        {aggregate_adjacent_spans(
            "critical_date_has_passed_spans",
            index_columns=['state_code', 'person_id', 'sentence_id'],
            attribute=['critical_date_has_passed', 'critical_date']
        )}
    )
    SELECT
        state_code,
        person_id,
        sentence_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        critical_date AS sentence_critical_date,
    FROM critical_date_has_passed_spans_aggregated
"""

US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_NAME,
    description=US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_DESCRIPTION,
    view_query_template=US_OR_SERVED_6_MONTHS_SUPERVISION_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_BUILDER.build_and_print()
