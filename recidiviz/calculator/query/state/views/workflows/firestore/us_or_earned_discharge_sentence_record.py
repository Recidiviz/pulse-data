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
"""Query for relevant sentence metadata needed to support Earned Discharge in Oregon
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.criteria.state_specific.us_or import (
    no_supervision_sanctions_within_6_months,
    sentence_eligible,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_VIEW_NAME = (
    "us_or_earned_discharge_sentence_record"
)

US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_DESCRIPTION = """
    Query for relevant sentence metadata needed to support Earned Discharge in Oregon
    """

US_OR_EARNED_DISCHARGE_SANCTIONS_CRITERIA = (
    no_supervision_sanctions_within_6_months.VIEW_BUILDER.criteria_name
)
US_OR_EARNED_DISCHARGE_SENTENCE_CRITERIA = sentence_eligible.VIEW_BUILDER.criteria_name

US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_QUERY_TEMPLATE = f"""
    WITH eligible_and_almost_eligible_population AS ({join_current_task_eligibility_spans_with_external_id(
        state_code='"US_OR"',
        tes_task_query_view="earned_discharge_materialized",
        id_type='"US_OR_ID"',
        eligible_and_almost_eligible_only=True,
    )})
    ,
    -- Create 1 row for each active sentence parsed from the eligibility spans reasons blob
    active_sentences AS (
        SELECT
            state_code,
            person_id,
            external_id,
            reasons,
            ineligible_criteria,
            {extract_object_from_json(
                json_column="sentence",
                object_column="sentence_id",
                object_type="INT64",
            )} AS sentence_id,
            {extract_object_from_json(
                json_column="sentence",
                object_column="is_eligible",
                object_type="BOOL",
            )} AS sentence_is_eligible,
            {extract_object_from_json(
                json_column="sentence",
                object_column="is_almost_eligible",
                object_type="BOOL",
            )} AS sentence_is_almost_eligible,
            {extract_object_from_json(
                json_column="sentence",
                object_column="sentence_eligibility_date",
                object_type="DATE",
            )} AS sentence_eligibility_date,
        FROM eligible_and_almost_eligible_population,
            UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason,
            UNNEST(JSON_QUERY_ARRAY(criteria_reason, "$.reason.active_sentences")) AS sentence
        WHERE {extract_object_from_json(
                 json_column="criteria_reason",
                 object_column="criteria_name",
                 object_type="STRING",
              )} = "{US_OR_EARNED_DISCHARGE_SENTENCE_CRITERIA}"
    )
    ,
    -- TODO(#24479): Query sentence conditions from preprocessed view
    current_conditions AS (
        SELECT
            COURT_CASE_NUMBER as court_case_number,
            ARRAY_AGG(STRUCT(
                COUNTY as county,
                CONDITION_CODE as condition_code,
                CONDITION_DESC as condition_description
            ) ORDER BY COUNTY, CONDITION_CODE, CONDITION_DESC) AS conditions
        FROM `{{project_id}}.us_or_raw_data_up_to_date_views.RCDVZ_CISPRDDTA_OPCOND_latest` op
        LEFT JOIN `{{project_id}}.us_or_raw_data_up_to_date_views.RCDVZ_DOCDTA_TBCOND_latest`
        USING (CONDITION_CODE, CONDITION_TYPE)
        -- Filter to "trackable" conditions, of which a subset are relevant to Earned Discharge.
        -- TODO(#25574): Refine filter once we better understand which conditions are relevant,
        -- and what exactly "trackable" means.
        WHERE op.CONDITION_TRACKABLE = "Y"
        GROUP BY 1
    )
    ,
    -- Store all relevant sentence-level information in a metadata JSON column
    sentences_with_metadata AS (
        SELECT
            active_sentences.external_id,
            active_sentences.person_id,
            active_sentences.state_code,
            active_sentences.sentence_id,
            sp.external_id AS sentence_external_id,
            active_sentences.reasons,
            active_sentences.ineligible_criteria,
            active_sentences.sentence_is_almost_eligible,
            TO_JSON(STRUCT(
                active_sentences.person_id,
                active_sentences.sentence_id,
                sp.external_id AS sentence_external_id,
                JSON_EXTRACT_SCALAR(sp.sentence_metadata, "$.COURT_CASE_NUMBER") AS court_case_number,
                sp.statute AS sentence_statute,
                sp.sentence_sub_type,
                sp.date_imposed AS sentence_imposed_date,
                sp.effective_date AS sentence_start_date,
                DATE_ADD(
                    sp.effective_date,
                    INTERVAL sp.max_sentence_length_days_calculated DAY
                ) AS sentence_end_date,
                sp.county_code AS sentence_county,
                sc.county_code AS charge_county,
                JSON_EXTRACT_SCALAR(sc.judge_full_name, "$.full_name") AS judge_full_name,
                cc.conditions,
                active_sentences.sentence_eligibility_date
            )) AS metadata_sentence,
            -- Override the sentence eligible flag to FALSE if the sanction criteria is not met so that cases where
            -- the sentence criteria are met but the sanction criterion is almost met are considered almost eligible
            -- (is_eligible=False)
            CASE
                WHEN "{US_OR_EARNED_DISCHARGE_SANCTIONS_CRITERIA}" IN UNNEST(active_sentences.ineligible_criteria)
                    THEN FALSE
                ELSE sentence_is_eligible
            END AS is_eligible,
        FROM active_sentences
        INNER JOIN `{{project_id}}.sessions.sentences_preprocessed_materialized` sp
            USING (state_code, person_id, sentence_id)
        LEFT JOIN `{{project_id}}.normalized_state.state_charge` sc
            USING(charge_id)
        LEFT JOIN current_conditions cc
        ON
            JSON_EXTRACT_SCALAR(sp.sentence_metadata, "$.COURT_CASE_NUMBER") = cc.court_case_number
        -- Only keep active sentences that are eligible or almost eligible for earned discharge
        WHERE (sentence_is_eligible OR sentence_is_almost_eligible)
    )
    ,
    programming AS (
        SELECT
            state_code,
            person_id,
            ARRAY_AGG(STRUCT(
                DATE(ENTRY_DATE) as entry_date,
                DATE(EXIT_DATE) as exit_date,
                TREAT_ID AS treatment_id,
                EXIT_CODE as exit_code
            ) ORDER BY ENTRY_DATE DESC) AS programs
        FROM `{{project_id}}.us_or_raw_data_up_to_date_views.RCDVZ_CISPRDDTA_CMOFFT_latest` prog
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            ON prog.RECORD_KEY = pei.external_id
            AND pei.id_type = "US_OR_RECORD_KEY"
        GROUP BY 1, 2
    )
    ,
    current_supervision_type AS (
        SELECT
            state_code,
            person_id,
            /* In case someone has multiple open supervision periods, pull just distinct
            supervision types. */
            ARRAY_AGG(DISTINCT supervision_type_raw_text ORDER BY supervision_type_raw_text) AS supervision_type,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
        WHERE state_code="US_OR"
        AND termination_date IS NULL
        GROUP BY 1, 2
    )
    SELECT
        sentences_with_metadata.external_id,
        opp_ids.opportunity_id,
        opp_ids.opportunity_pseudonymized_id,
        sentences_with_metadata.state_code,
        sentences_with_metadata.reasons,
        sentences_with_metadata.is_eligible,
        -- Convert the `ineligible_criteria` from person-level to sentence-level
        CASE
            -- If this span is fully eligible then `ineligible_criteria` is NULL
            WHEN sentences_with_metadata.is_eligible THEN NULL
            -- If the sentence is almost eligible then append the sentence criteria to the ineligible criteria
            WHEN sentences_with_metadata.sentence_is_almost_eligible
                AND "{US_OR_EARNED_DISCHARGE_SENTENCE_CRITERIA}" NOT IN UNNEST(sentences_with_metadata.ineligible_criteria)
                THEN ARRAY_CONCAT(sentences_with_metadata.ineligible_criteria, ["{US_OR_EARNED_DISCHARGE_SENTENCE_CRITERIA}"])
            ELSE sentences_with_metadata.ineligible_criteria
        END AS ineligible_criteria,
        TO_JSON(IFNULL(programming.programs, [])) AS metadata_programs,
        sentences_with_metadata.metadata_sentence,
        current_supervision_type.supervision_type AS metadata_supervision_type,
    FROM sentences_with_metadata
    INNER JOIN `{{project_id}}.workflows_views.sentence_id_to_opportunity_id` opp_ids
    USING (state_code, person_id, sentence_id)
    LEFT JOIN programming
    USING (state_code, person_id)
    LEFT JOIN current_supervision_type
    USING (state_code, person_id)
"""

US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_VIEW_NAME,
    view_query_template=US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_QUERY_TEMPLATE,
    description=US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_OR
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_EARNED_DISCHARGE_SENTENCE_RECORD_VIEW_BUILDER.build_and_print()
