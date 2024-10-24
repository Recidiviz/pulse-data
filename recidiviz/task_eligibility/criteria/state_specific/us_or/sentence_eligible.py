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
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
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
        -- NB: this query pulls from `sentences_preprocessed` (not `sentence_spans`)
        SELECT
            * EXCEPT (start_date, end_date),
            start_date AS sentence_start_date,
            end_date as sentence_end_date,
        FROM ({sentence_attributes()})
        WHERE state_code='US_OR' AND sentence_type='SUPERVISION'
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
        WHERE sentences.offense_date<=ssa.start_date
        GROUP BY 1, 2, 3, 4
    ),
    absconsion_sessions AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
        WHERE state_code='US_OR' AND compartment_level_2='ABSCONSION'
    ),
    absconsions_during_sentence AS (
        /* Here, for each sentence, we pull sessions of absconsion occurring during that
        sentence. */
        SELECT
            * EXCEPT (start_date, end_date_exclusive),
            start_date AS absconsion_start_date,
            end_date_exclusive AS absconsion_end_date,
        FROM ({create_intersection_spans(table_1_name='sentences',
                                         table_2_name='absconsion_sessions',
                                         index_columns=['state_code', 'person_id'],
                                         table_1_columns=['sentence_id'],
                                         table_1_start_date_field_name='sentence_start_date',
                                         table_1_end_date_field_name='sentence_end_date')})
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
            SUM(DATE_DIFF(IF(absconsion_end_date>ssa.start_date, ssa.start_date, absconsion_end_date),
                          absconsion_start_date,
                          DAY)
            ) AS days_absconded,
        FROM sub_sessions_with_attributes ssa
        LEFT JOIN absconsions_during_sentence
            USING (state_code, person_id, sentence_id)
        WHERE absconsion_start_date<ssa.start_date
        GROUP BY 1, 2, 3, 4
    ),
    sub_sessions_with_attributes_with_reasons AS (
        SELECT
            state_code,
            person_id,
            sentence_id,
            start_date,
            ssa.end_date,
            ssa.is_eligible,
            ssa.sentence_eligibility_date,
            TO_JSON(STRUCT(
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
                first new offense. This field is mainly used for internal/debugging
                purposes and is not surfaced in the tool currently. */
                most_recent_offenses.most_recent_offense_date AS latest_conviction_date,
                /* This is the total number of days of absconsion prior to the start of
                the given span. This field might be out of date later in a span if
                an individual is actively in absconsion during that span. This field is
                mainly used for internal/debugging purposes and is not surfaced in the
                tool currently. */
                COALESCE(days_absconded_by_span.days_absconded, 0) AS num_days_absconsion,
                sentence_id,
                ssa.sentence_eligibility_date,
                ssa.is_eligible,
                ssa.is_almost_eligible,
                ssa.meets_criteria_served_6_months,
                ssa.meets_criteria_served_half_of_sentence
            )) AS reason,
        FROM sub_sessions_with_attributes ssa
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
        TO_JSON(STRUCT(ARRAY_AGG(reason ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS active_sentences,
                       MIN(sentence_eligibility_date) AS earliest_sentence_eligibility_date,
                       # TODO(#33043): drop these 2 fields after the Polaris migration
                       ARRAY_AGG(IF(is_eligible, reason, NULL) IGNORE NULLS ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS eligible_sentences,
                       ARRAY_AGG(IF(is_eligible, NULL, reason) IGNORE NULLS ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS ineligible_sentences
        )) AS reason,
        ARRAY_AGG(reason ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS active_sentences,
        MIN(sentence_eligibility_date) AS earliest_sentence_eligibility_date,
        # TODO(#33043): drop these 2 fields after the Polaris migration
        ARRAY_AGG(IF(is_eligible, reason, NULL) IGNORE NULLS ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS eligible_sentences,
        ARRAY_AGG(IF(is_eligible, NULL, reason) IGNORE NULLS ORDER BY JSON_VALUE(reason.supervision_sentence_start_date) ASC) AS ineligible_sentences,
    FROM sub_sessions_with_attributes_with_reasons
    GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_OR,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="active_sentences",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Supervision sentences considered active during the criteria span",
        ),
        ReasonsField(
            name="earliest_sentence_eligibility_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The earliest earned discharge eligibility date over all active sentences",
        ),
        # TODO(#33043): drop these 2 fields after the Polaris migration to us_or_earned_discharge_sentence_record.py
        ReasonsField(
            name="eligible_sentences",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Sentences eligible for earned discharge",
        ),
        ReasonsField(
            name="ineligible_sentences",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Sentences ineligible for earned discharge",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
