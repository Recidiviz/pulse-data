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
# ============================================================================
"""Combines sentence-level eligibility determinations for OR earned discharge"""

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    sentence_attributes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_OR_SENTENCE_ELIGIBLE"

_DESCRIPTION = (
    """Combines sentence-level eligibility determinations for OR earned discharge"""
)

_QUERY_TEMPLATE = f"""
    WITH sentences AS (
        -- NB: this query pulls from sentences_preprocessed (not sentence_spans)
        SELECT
            * EXCEPT (start_date, end_date),
            start_date AS sentence_start_date,
            end_date as sentence_end_date,
        FROM ({sentence_attributes()})
        WHERE state_code='US_OR'
    ),
    sentence_eligibility_spans AS (
        SELECT *
        FROM `{{project_id}}.{{analyst_dataset}}.us_or_earned_discharge_sentence_eligibility_spans_materialized`
    ),
    /* We sub-sessionize here because the spans coming from
    `us_or_earned_discharge_sentence_eligibility_spans_materialized` are at the
    person-sentence level, but we need to aggregate up to the person level for this
    criterion. */
    {create_sub_sessions_with_attributes("sentence_eligibility_spans")},
    most_recent_offenses AS (
        /* Here, for each person-sentence span of time, we pull the most recent offense
        from before the span started. */
        SELECT
            state_code,
            person_id,
            ssa.sentence_id,
            ssa.start_date,
            MAX(sentences.offense_date) AS most_recent_offense_date,
        FROM sub_sessions_with_attributes ssa
        LEFT JOIN sentences
            USING (state_code, person_id)
        WHERE (sentences.offense_date <= ssa.start_date)
        GROUP BY 1, 2, 3, 4
    ),
    absconsions_during_sentence AS (
        /* Here, for each sentence, we pull sessions of absconsion occurring during that
        sentence. */
        /* TODO(#26623): Once we know how we're accounting for absconsions in OR earned
        discharge, then (if possible/necessary) use `create_intersection_spans` to
        create this CTE, rather than creating it manually here. */
        SELECT
            state_code,
            person_id,
            sentences.sentence_id,
            IF(sess.start_date<sentences.sentence_start_date, sentences.sentence_start_date, sess.start_date) AS absconsion_start_date,
            IF((sess.end_date_exclusive>sentences.sentence_end_date) OR (sess.end_date_exclusive IS NULL), sentences.sentence_end_date, sess.end_date_exclusive) AS absconsion_end_date,
        FROM sentences
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
        USING (state_code, person_id)
        WHERE sess.compartment_level_2='ABSCONSION'
            AND sess.start_date<=sentences.sentence_end_date
            AND sess.end_date>=sentences.sentence_start_date
    ),
    days_absconded_by_span AS (
        /* Here, for each person-sentence span of time, we pull the total number of days
        of absconsion prior to the span. */
        SELECT
            state_code,
            person_id,
            sentence_id,
            ssa.start_date,
            /* We're only counting the number of days of absconsion prior to the start
            of the span, so if the absconsion goes into the span, we count only the
            days of absconsion up to the span start date. */
            SUM(DATE_DIFF(IF(absconsion_end_date>ssa.start_date, ssa.start_date, absconsion_end_date), absconsion_start_date, DAY)) AS days_absconded
        FROM sub_sessions_with_attributes ssa
        LEFT JOIN absconsions_during_sentence
            USING (state_code, person_id, sentence_id)
        WHERE absconsion_start_date <= ssa.end_date
        GROUP BY 1, 2, 3, 4
    ),
    sub_sessions_with_attributes_with_reasons AS (
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date,
            end_date,
            is_eligible,
            TO_JSON(STRUCT(
                sentences.date_imposed AS sentence_imposed_date,
                sentences.sentence_start_date AS supervision_sentence_start_date,
                /* This is the date of the most recent OFFENSE prior to the start of the
                given span, not the most recent new CONVICTION (which aligns with OR
                eligibility criteria, which care about offense dates).
                NB: new offenses during a sentence render that sentence permanently
                ineligible for earned discharge. This means that while the first new
                offense triggers an eligibility change (and therefore a span break),
                subsequent offenses do not change overall eligibility for a given
                sentence. This means that the "latest_conviction_date" field might be
                out of date later in a span, because it may not be updated to include
                subsequent offenses if a sentence is already ineligible because of the 
                first new offense. */
                most_recent_offenses.most_recent_offense_date AS latest_conviction_date,
                /* This is the total number of days of absconsion prior to the start of
                the given span. This field might be out of date later in a span if
                an individual is actively in absconsion during that span. */
                /* TODO(#26623): Ensure that we're calculating num_days_absconsion
                in a manner consistent with how OR tracks & considers days in 
                absconsion when determining ED eligibility. */
                COALESCE(days_absconded_by_span.days_absconded, 0) AS num_days_absconsion,
                sentences.statute AS sentence_statute,
                sentence_id AS sentence_id
            )) AS reason,
        FROM sub_sessions_with_attributes
        LEFT JOIN sentences
            USING (state_code, person_id, sentence_id)
        LEFT JOIN most_recent_offenses
            USING (state_code, person_id, sentence_id, start_date)
        LEFT JOIN days_absconded_by_span
            USING (state_code, person_id, sentence_id, start_date)
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        -- if any sentence is eligible, we consider the person to be eligible
        LOGICAL_OR(is_eligible) AS meets_criteria,
        -- split out eligible and ineligible sentences
        TO_JSON(STRUCT(ARRAY_AGG(IF(is_eligible, reason, NULL) IGNORE NULLS ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS eligible_sentences,
                       ARRAY_AGG(IF(is_eligible, NULL, reason) IGNORE NULLS ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS ineligible_sentences)) AS reason,
    FROM sub_sessions_with_attributes_with_reasons
    GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_OR,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        sessions_dataset=SESSIONS_DATASET,
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
