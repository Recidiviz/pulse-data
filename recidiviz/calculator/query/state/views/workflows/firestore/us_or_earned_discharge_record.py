# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query for relevant metadata needed to support Earned Discharge in Oregon
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_EARNED_DISCHARGE_RECORD_VIEW_NAME = "us_or_earned_discharge_record"

US_OR_EARNED_DISCHARGE_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support Earned Discharge in Oregon
    """

US_OR_EARNED_DISCHARGE_RECORD_QUERY_TEMPLATE = f"""
    WITH base_query AS ({join_current_task_eligibility_spans_with_external_id(
        state_code='"US_OR"',
        tes_task_query_view="earned_discharge_materialized",
        id_type='"US_OR_RECORD_KEY"',
    )})
    ,
    -- TODO(#24479): Query from preprocessed view
    current_conditions AS (
        SELECT
            COURT_CASE_NUMBER as court_case_number,
            ARRAY_AGG(STRUCT(
                COUNTY as county,
                CONDITION_CODE as condition_code, 
                CONDITION_DESC as condition_description
            )) AS conditions
        FROM `{{project_id}}.us_or_raw_data_up_to_date_views.RCDVZ_CISPRDDTA_OPCOND_latest` op
        LEFT JOIN `{{project_id}}.us_or_raw_data_up_to_date_views.RCDVZ_DOCDTA_TBCOND_latest` 
        USING (CONDITION_CODE, CONDITION_TYPE)
        -- Filter to "trackable" conditions, of which a subset are relevant to Earned Discharge.
        -- TODO(#25574): Refine filter once we better understand which conditions are relevant,
        -- and what exactly "trackable" means.
        WHERE op.CONDITION_TRACKABLE = 'Y'
        GROUP BY 1
    )
    ,
    -- Collect all sentence-level information for the surfaced group of people
    eligible_and_ineligible_sentences AS (
        SELECT
            sp.person_id,
            sp.state_code,
            sp.sentence_id,
            sp.sentence_sub_type,
            sp.date_imposed AS sentence_imposed_date,
            sp.effective_date AS sentence_start_date,
            DATE_ADD(
                sp.effective_date, 
                INTERVAL sp.max_sentence_length_days_calculated DAY
            ) AS sentence_end_date,
            sp.statute AS sentence_statute,
            sp.county_code AS sentence_county,
            sc.county_code AS charge_county,
            JSON_EXTRACT_SCALAR(sp.sentence_metadata, '$.COURT_CASE_NUMBER') AS court_case_number,
            JSON_EXTRACT_SCALAR(sc.judge_full_name, '$.full_name') AS judge_full_name,
            cc.conditions,
            COALESCE(sentence_eligibility_spans.is_eligible, FALSE) AS is_eligible,
        FROM `{{project_id}}.sessions.sentences_preprocessed_materialized` sp
        LEFT JOIN `{{project_id}}.normalized_state.state_charge` sc
        USING(charge_id)
        LEFT JOIN current_conditions cc 
        ON
            JSON_EXTRACT_SCALAR(sp.sentence_metadata, '$.COURT_CASE_NUMBER') = cc.court_case_number
        LEFT JOIN `{{project_id}}.analyst_data.us_or_earned_discharge_sentence_eligibility_spans_materialized` sentence_eligibility_spans
        ON
            sp.person_id = sentence_eligibility_spans.person_id
            AND sp.sentence_id = sentence_eligibility_spans.sentence_id
            AND sp.state_code = sentence_eligibility_spans.state_code
            AND CURRENT_DATE('US/Pacific') BETWEEN sentence_eligibility_spans.start_date AND {nonnull_end_date_clause('sentence_eligibility_spans.end_date')}
        WHERE 
            sp.person_id IN (SELECT DISTINCT person_id FROM base_query)
            AND sp.state_code = 'US_OR'
            AND sp.status = 'SERVING'
            AND sp.sentence_type = 'SUPERVISION'
    )
    ,
    -- Aggregate sentences by person, to surface all eligible sentences for each eligible person
    eligible_sentences_by_person AS (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(
                sentence_id,
                court_case_number,
                sentence_statute,
                sentence_sub_type,
                sentence_imposed_date,
                sentence_start_date,
                sentence_end_date,
                sentence_county,
                charge_county,
                judge_full_name,
                conditions
            ) ORDER BY sentence_start_date DESC, sentence_id ASC) AS eligible_sentences,
        FROM eligible_and_ineligible_sentences sentences
        WHERE is_eligible
        GROUP BY 1
    )
    ,
    -- Aggregate sentences by person, to surface all ineligible sentences for each eligible person
    ineligible_sentences_by_person AS (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(
                sentence_id,
                court_case_number,
                sentence_statute,
                sentence_sub_type,
                sentence_imposed_date,
                sentence_start_date,
                sentence_end_date,
                sentence_county,
                charge_county,
                judge_full_name,
                conditions
            ) ORDER BY sentence_start_date DESC, sentence_id ASC) AS ineligible_sentences,
        FROM eligible_and_ineligible_sentences sentences
        WHERE NOT is_eligible
        GROUP BY 1
    )
    ,
    programming AS (
        SELECT
            RECORD_KEY as external_id,
            ARRAY_AGG(STRUCT(
                DATE(ENTRY_DATE) as entry_date,
                DATE(EXIT_DATE) as exit_date,
                TREAT_ID AS treatment_id,
                EXIT_CODE as exit_code
            ) ORDER BY ENTRY_DATE DESC) AS programs
        FROM `{{project_id}}.us_or_raw_data_up_to_date_views.RCDVZ_CISPRDDTA_CMOFFT_latest`
        GROUP BY 1
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
        WHERE state_code='US_OR'
        AND termination_date IS NULL
        GROUP BY 1, 2
    )
    ,
    form_birthdate AS (
        SELECT
            person_id,
            birthdate,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_person`
        WHERE state_code='US_OR'
    )
    ,
    -- Replace Record Key (linking ID in tables) with OR ID (used in OMS)
    base_query_with_or_id AS (
        SELECT
            pei.external_id AS external_id,
            base.* EXCEPT(external_id)
        FROM base_query base
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            base.person_id = pei.person_id
            AND base.state_code = pei.state_code
            AND pei.id_type = "US_OR_ID"
    )
    SELECT
        base.external_id,
        base.state_code,
        base.reasons,
        base.is_eligible,
        base.ineligible_criteria,
        TO_JSON(IFNULL(programming.programs, [])) AS metadata_programs,
        TO_JSON(eligible_sentences_by_person.eligible_sentences) AS metadata_eligible_sentences,
        TO_JSON(IFNULL(ineligible_sentences_by_person.ineligible_sentences, [])) AS metadata_ineligible_sentences,
        current_supervision_type.supervision_type AS metadata_supervision_type,
        form_birthdate.birthdate AS form_information_birthdate,
    FROM base_query_with_or_id base
    LEFT JOIN programming
    USING (external_id)
    LEFT JOIN eligible_sentences_by_person
    USING (person_id)
    LEFT JOIN ineligible_sentences_by_person
    USING (person_id)
    LEFT JOIN current_supervision_type
    USING (state_code, person_id)
    LEFT JOIN form_birthdate
    USING (person_id)
    WHERE base.is_eligible
"""

US_OR_EARNED_DISCHARGE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_OR_EARNED_DISCHARGE_RECORD_VIEW_NAME,
    view_query_template=US_OR_EARNED_DISCHARGE_RECORD_QUERY_TEMPLATE,
    description=US_OR_EARNED_DISCHARGE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_OR
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_EARNED_DISCHARGE_RECORD_VIEW_BUILDER.build_and_print()
