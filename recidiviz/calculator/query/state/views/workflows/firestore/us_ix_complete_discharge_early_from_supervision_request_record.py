# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query for information needed to autopopulate the early discharge form and
relevant case notes needed to determine eligibility
for early discharge from supervision in Idaho
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_ix.complete_discharge_early_from_parole_dual_supervision_request import (
    VIEW_BUILDER as US_IX_COMPLETE_DISCHARGE_EARLY_FROM_PAROLE_TES_VIEW_BUILDER,
)
from recidiviz.task_eligibility.eligibility_spans.us_ix.complete_discharge_early_from_probation_supervision_request import (
    VIEW_BUILDER as US_IX_COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_TES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_NAME = (
    "us_ix_complete_discharge_early_from_supervision_request_record"
)

_COLLAPSED_TES_SPANS_PROBATION_ADDRESS = (
    build_collapsed_tes_spans_view_materialized_address(
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_TES_VIEW_BUILDER
    )
)

_COLLAPSED_TES_SPANS_PAROLE_ADDRESS = (
    build_collapsed_tes_spans_view_materialized_address(
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_PAROLE_TES_VIEW_BUILDER
    )
)

US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_DESCRIPTION = """
    View of information needed to autopopulate the early discharge form as well as 
    relevant case notes for determining eligibility 
    for early discharge from supervision in Idaho 
    """
US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE = f"""
   WITH case_numbers AS(
      SELECT DISTINCT 
        charge.state_code,
        charge.person_id,
        charge.charge_id,
        ch.Docket, 
      FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_Sentence_latest` sent
      INNER JOIN  `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_SentenceLink_latest` sentlink 
        ON sent.SentenceId = sentlink.SentenceId
      INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_SentenceLinkOffense_latest` sentoff 
        ON sentlink.SentenceLinkId = sentoff.SentenceLinkId
      INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_Offense_latest` off 
        ON sentoff.OffenseId = off.OffenseId
      INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_SentenceOrder_latest` ord 
        ON off.SentenceOrderId = ord.SentenceOrderId
      INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_Charge_latest` ch 
        ON ch.ChargeId = ord.ChargeId
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_charge` charge
            ON sent.OffenderId = SPLIT((charge.external_id), '-')[SAFE_OFFSET(0)]
            AND sentoff.OffenseId = SPLIT((charge.external_id), '-')[SAFE_OFFSET(1)]
    ),
    open_consecutive_sentences AS (
    /* This CTE creates a list of open parent sentences that have consecutive sentences */ 
    SELECT sent.sentence_id
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized`,
        UNNEST (sentences_preprocessed_id_array_actual_completion) sentences_preprocessed_id
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.us_ix_consecutive_sentences_preprocessed_materialized` cs
        ON cs.consecutive_sentence_id = sent.sentence_id
      WHERE sent.state_code = "US_IX"
      AND CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}
      AND sent.projected_completion_date_max >= CURRENT_DATE('US/Pacific')
    ),
   sentence_charge_description AS (
      SELECT
        sent.state_code,
        sent.person_id,
        sent.sentence_type,
        sent.date_imposed,
        sentences_preprocessed_id,
        sent.projected_completion_date_max,
        sent.min_sentence_length_days_calculated,
        sent.max_sentence_length_days_calculated,
        charge.description, 
        loc.LocationName AS county_name,
        charge.judge_full_name,
        cn.Docket AS case_number,
      FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized`,
        UNNEST (sentences_preprocessed_id_array_actual_completion) sentences_preprocessed_id
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
      LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_charge` charge
        USING (state_code, person_id, charge_id)
      LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_Location_latest` loc
        ON charge.county_code = loc.LocationId
      LEFT JOIN case_numbers cn
        USING (state_code, person_id, charge_id)
      LEFT JOIN `{{project_id}}.{{sessions_dataset}}.us_ix_consecutive_sentences_preprocessed_materialized` cs
        USING(sentence_id)
      WHERE sent.state_code = "US_IX"
      AND CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
      --if this sentence is consecutive to a parent sentence that is open, than do not include this sentence 
      AND (cs.consecutive_sentence_id IS NULL OR cs.consecutive_sentence_id NOT IN (SELECT sentence_id FROM open_consecutive_sentences))
      AND sent.projected_completion_date_max >= CURRENT_DATE('US/Pacific')
      --Pick one record per person, sentence type and ChargeId, selecting the lowest sentence sequence number and
      --prioritizing amendments over initial sentence event type that have the same sequence number
      QUALIFY ROW_NUMBER() OVER(
        PARTITION BY sent.person_id, sent.sentence_type,
          SPLIT(JSON_VALUE(sent.sentence_metadata, '$.sentence_sequence'), '-')[SAFE_OFFSET(0)]
        ORDER BY SPLIT(JSON_VALUE(sentence_metadata, '$.sentence_sequence'), '-')[SAFE_OFFSET(1)],
          SPLIT(JSON_VALUE(sentence_metadata, '$.sentence_sequence'),'-')[SAFE_OFFSET(2)],
          -- If all else is the same pick the sentence with the latest effective date
          sent.effective_date DESC,
          -- If all else is the same pick the sentence with the latest date imposed
          sent.date_imposed DESC,
          -- Prioritizing amendments over initial sentences will be handled by state sentence length under the v2 schema
          JSON_VALUE(sentence_metadata, "$.sentence_event_type")
      ) = 1
    ),
    sentence_charge_description_aggregated AS (
      SELECT 
      charge.state_code,
      charge.person_id,
      charge.sentence_type,
      ARRAY_AGG(charge.projected_completion_date_max IGNORE NULLS ORDER BY 
                charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
                AS form_information_full_term_release_dates,
      ARRAY_AGG(charge.description IGNORE NULLS ORDER BY 
            charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
            AS form_information_charge_descriptions,
      ARRAY_AGG(charge.judge_full_name IGNORE NULLS ORDER BY 
            charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
            AS form_information_judge_names,
      ARRAY_AGG(charge.county_name IGNORE NULLS ORDER BY 
            charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
            AS form_information_county_names,
      ARRAY_AGG(charge.max_sentence_length_days_calculated IGNORE NULLS ORDER BY 
            charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
            AS form_information_sentence_max,
      ARRAY_AGG(charge.min_sentence_length_days_calculated IGNORE NULLS ORDER BY 
            charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
            AS form_information_sentence_min,
      ARRAY_AGG(charge.case_number IGNORE NULLS ORDER BY 
            charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
            AS form_information_case_numbers,
      ARRAY_AGG(charge.date_imposed IGNORE NULLS ORDER BY 
            charge.date_imposed, charge.projected_completion_date_max, charge.sentences_preprocessed_id) 
            AS form_information_date_imposed,
      FROM sentence_charge_description charge
      GROUP BY 1,2,3
    ),
    supervision_officer AS (
      SELECT 
        cses.person_id,
        cses.state_code,
        ss.full_name_json AS supervision_officer_name,
      FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`cses
      INNER JOIN `{{project_id}}.reference_views.state_staff_with_names` ss
          ON ss.legacy_supervising_officer_external_id = cses.supervising_officer_external_id_end
          AND cses.state_code  = ss.state_code
          AND cses.state_code = 'US_IX'
     --choose supervising officer for current supervision session 
      WHERE CURRENT_DATE('US/Pacific') BETWEEN cses.start_date AND {nonnull_end_date_clause('cses.end_date')}
      QUALIFY ROW_NUMBER() OVER(PARTITION BY cses.person_id, cses.start_date ORDER BY cses.start_date DESC)=1
    ),
    person_info AS (
      SELECT
        spi.person_id,
        spi.state_code,
        INITCAP(JSON_VALUE(PARSE_JSON(spi.full_name), '$.given_names'))
            || " " 
            || INITCAP(JSON_VALUE(PARSE_JSON(spi.full_name), '$.surname')) AS client_name,
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_person` spi
      WHERE state_code = 'US_IX'
    ),
    latest_within_session_assessment_score AS (
      SELECT
        ses.person_id,
        ses.state_code,
        assessment_date AS first_assessment_date,
        assessment_score AS first_assessment_score,
        FIRST_VALUE(assessment_date) OVER (PARTITION BY score.person_id ORDER BY score.assessment_date DESC) AS latest_assessment_date,
        FIRST_VALUE(assessment_score) OVER (PARTITION BY score.person_id ORDER BY score.assessment_date DESC) AS latest_assessment_score,
      FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` ses
      LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
        ON ses.state_code = score.state_code
        AND ses.person_id = score.person_id 
        AND score.assessment_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
        AND score.assessment_type = "LSIR" 
      WHERE ses.state_code = 'US_IX'
      AND CURRENT_DATE('US/Pacific') BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY assessment_date)=1
    ),
    latest_assessment_score AS(
    --if no within session LSI-R information is found, use the latest score
      SELECT 
        l.person_id,
        l.state_code,
        --only fill out first assessment information if it is different than the latest information
        IF(l.first_assessment_date = l.latest_assessment_date, NULL, l.first_assessment_date) AS first_assessment_date,
        IF(l.first_assessment_score = l.latest_assessment_score, NULL, l.first_assessment_score) AS first_assessment_score,
        COALESCE(l.latest_assessment_date, score.assessment_date) AS latest_assessment_date,
        COALESCE(l.latest_assessment_score, score.assessment_score) AS latest_assessment_score,
      FROM latest_within_session_assessment_score l
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
        USING(state_code, person_id)
      WHERE assessment_type = "LSIR"
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY assessment_date DESC)=1
        ),
    latest_ncic_ilets_check AS(
      SELECT 
        a.state_code,
        a.person_id,
        a.NoteDate AS review_date,
        REGEXP_EXTRACT(a.Details, {{note_title_regex}} ) as note_title,
        COALESCE(REGEXP_EXTRACT(a.Details, {{note_body_regex}}), a.Details) as note_body,
      FROM `{{project_id}}.{{supplemental_dataset}}.us_ix_case_note_matched_entities` a
      WHERE ncic_ilets_nco_check
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY review_date DESC)=1
    ),
    notes AS(
    --violations
        SELECT 
            v.state_code,
            v.person_id,
            v.violation_date AS event_date,
            CAST(v.supervision_violation_id AS STRING) AS note_id,
            vt.violation_type AS note_title,
             "--" AS note_body,
            "Violations" AS criteria,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` vt
            USING (supervision_violation_id, person_id, state_code)
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
             USING (supervision_violation_id, person_id, state_code)
        WHERE (violation_type NOT IN ('TECHNICAL') OR violation_type IS NULL)
        AND DATE_ADD(COALESCE(vr.response_date,v.violation_date), INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
    --ncic/ilets and new crime violations
        AND state_code = "US_IX"
        UNION ALL
        SELECT 
            a.state_code,
            a.person_id,
            a.NoteDate AS event_date,
            a.OffenderNoteId AS note_id,
            REGEXP_EXTRACT(a.Details, {{note_title_regex}} ) as note_title,
            COALESCE(REGEXP_EXTRACT(a.Details, {{note_body_regex}}), a.Details) as note_body,
            CASE 
                WHEN ncic_ilets_nco_check THEN "No new criminal activity check"
                WHEN new_crime THEN "Violations"
                ELSE NULL
            END AS criteria
        FROM `{{project_id}}.{{supplemental_dataset}}.us_ix_case_note_matched_entities` a
        WHERE (ncic_ilets_nco_check OR new_crime)
        --only select ncic checks and new crime within the past 90 days
        AND DATE_ADD(a.NoteDate, INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
    --Community service,Special conditions, Interlock, Treatment, Specialty court, Transfer chrono
        UNION ALL
        SELECT 
            a.state_code,
            a.person_id,
            a.NoteDate AS event_date,
            a.OffenderNoteId AS note_id,
            REGEXP_EXTRACT(a.Details, {{note_title_regex}} ) as note_title,
            COALESCE(REGEXP_EXTRACT(a.Details, {{note_body_regex}}), a.Details) as note_body,
            CASE 
                WHEN (community_service AND NOT not_cs AND NOT agents_warning) THEN "Community service"
                WHEN case_plan THEN "Special Conditions"
                WHEN interlock THEN "Interlock"
                WHEN any_treatment THEN "Treatment"
                WHEN (specialty_court AND court AND NOT psi) THEN "Specialty court"
                WHEN transfer_chrono THEN "Transfer chrono"
                ELSE NULL
            END AS criteria
          FROM `{{project_id}}.{{supplemental_dataset}}.us_ix_case_note_matched_entities` a
          WHERE (community_service AND NOT not_cs AND NOT agents_warning)
                OR case_plan
                OR interlock
                OR any_treatment
                OR (specialty_court AND court AND NOT psi)
                OR (transfer_chrono AND DATE_ADD(a.NoteDate, INTERVAL 1 YEAR) >= CURRENT_DATE('US/Pacific'))
    --DUI notes
        UNION ALL
        SELECT 
            a.state_code,
            a.person_id,
            a.NoteDate AS event_date,
            a.OffenderNoteId AS note_id,
            REGEXP_EXTRACT(a.Details, {{note_title_regex}} ) as note_title,
            COALESCE(REGEXP_EXTRACT(a.Details, {{note_body_regex}}), a.Details) as note_body,
            "DUI" AS criteria
        FROM `{{project_id}}.{{supplemental_dataset}}.us_ix_case_note_matched_entities` a
        WHERE dui 
            AND NOT not_m_dui
            --only include DUI notes within the past 12 months 
            AND DATE_ADD(a.NoteDate, INTERVAL 12 MONTH) >= CURRENT_DATE('US/Pacific')
    ),
    dedup_notes AS(
    SELECT
        n.state_code,
        n.person_id,
        n.event_date,
        n.note_id,
        n.note_title,
        n.note_body,
        n.criteria
     FROM notes n
    --only select notes during the current supervision session
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` ses
        ON n.person_id = ses.person_id
        AND CURRENT_DATE('US/Pacific') BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
        AND n.event_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
    -- only select each unique note_id once, even if it matches multiple headers (i.e. community service and case plan)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY n.person_id, n.note_id ORDER BY n.criteria)=1
    ),
     latest_notes AS(
    SELECT
        n.state_code,
        n.person_id,
        TO_JSON(
            ARRAY_AGG(
                IF(n.note_title IS NOT NULL, STRUCT(n.note_title, n.note_body, n.event_date, n.criteria),NULL)
                IGNORE NULLS
                ORDER BY n.criteria, n.event_date, n.note_title
            )
        ) AS case_notes,
    FROM dedup_notes n
    GROUP BY 1,2
    )
        SELECT
            tes.person_id,
            tes.is_eligible,
            tes.is_almost_eligible,
            pei.external_id AS external_id,
            tes.state_code,
            COALESCE(tes_probation_collapsed.start_date, tes_parole_collapsed.start_date) AS eligible_date,
            ses.start_date AS supervision_start_date,
            pi.client_name AS form_information_client_name,
            IF(tes.task_name = "COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_SUPERVISION_REQUEST", "Probation", "Parole") 
                AS form_information_supervision_type,
            agg_charge.form_information_full_term_release_dates,
            so.supervision_officer_name AS form_information_supervision_officer_name,
            ncic.review_date AS form_information_ncic_check_date,
            agg_charge.form_information_charge_descriptions,
            agg_charge.form_information_judge_names,
            agg_charge.form_information_county_names,
            agg_charge.form_information_sentence_max,
            agg_charge.form_information_sentence_min,
            agg_charge.form_information_case_numbers,
            agg_charge.form_information_date_imposed,
            score.latest_assessment_date AS form_information_latest_assessment_date,
            score.latest_assessment_score AS form_information_latest_assessment_score,
            score.first_assessment_date AS form_information_first_assessment_date,
            score.first_assessment_score AS form_information_first_assessment_score,
            DATE_DIFF(proj.projected_completion_date_max, CURRENT_DATE('US/Pacific'), DAY) AS days_remaining_on_supervision,
            tes.reasons AS reasons,
            n.case_notes AS case_notes,
            -- Almost eligible if there is only 1 ineligible_criteria present
            IF(ARRAY_LENGTH(tes.ineligible_criteria) = 1, tes.ineligible_criteria, []) AS ineligible_criteria,
        FROM (
            SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_discharge_early_from_probation_supervision_request_materialized` 
            UNION ALL
            SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_discharge_early_from_parole_dual_supervision_request_materialized` 
        ) tes
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` ses
            ON tes.state_code = ses.state_code
            AND tes.person_id = ses.person_id
            AND tes.start_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cses
            ON tes.state_code = cses.state_code
            AND tes.person_id = cses.person_id 
            AND CURRENT_DATE('US/Pacific') BETWEEN cses.start_date AND {nonnull_end_date_clause('cses.end_date')}
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` proj
            ON proj.state_code = ses.state_code
            AND proj.person_id = ses.person_id
            --use the projected completion date from the current span
            AND CURRENT_DATE('US/Pacific') BETWEEN proj.start_date AND {nonnull_end_date_exclusive_clause('proj.end_date')}
        LEFT JOIN latest_notes n
            ON ses.state_code = n.state_code
            AND ses.person_id = n.person_id
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON ses.state_code = pei.state_code 
            AND ses.person_id = pei.person_id
            AND pei.id_type = "US_IX_DOC"
            --only individuals that are currently eligible for early discharge
        LEFT JOIN sentence_charge_description_aggregated agg_charge
            ON tes.state_code = agg_charge.state_code
            AND tes.person_id = agg_charge.person_id
            --only list Probation sentences for ED from Probation and Parole sentencecs for ED from Parole 
            AND IF(cses.compartment_level_2 IN ("DUAL", "PAROLE"), "INCARCERATION", "SUPERVISION") = agg_charge.sentence_type
        LEFT JOIN supervision_officer so
            ON tes.state_code = so.state_code
            AND tes.person_id = so.person_id
        LEFT JOIN person_info pi
            ON tes.state_code = pi.state_code
            AND tes.person_id = pi.person_id
        LEFT JOIN latest_assessment_score score
            ON tes.state_code = score.state_code
            AND tes.person_id = score.person_id
        LEFT JOIN latest_ncic_ilets_check ncic
            ON tes.state_code = ncic.state_code
            AND tes.person_id = ncic.person_id
            AND DATE_ADD(review_date, INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
        LEFT JOIN `{{project_id}}.{_COLLAPSED_TES_SPANS_PROBATION_ADDRESS.to_str()}` tes_probation_collapsed
            ON tes_probation_collapsed.state_code = tes.state_code
            AND tes_probation_collapsed.person_id = tes.person_id
            AND CURRENT_DATE('US/Pacific') BETWEEN tes_probation_collapsed.start_date AND {nonnull_end_date_exclusive_clause('tes_probation_collapsed.end_date')}
        LEFT JOIN `{{project_id}}.{_COLLAPSED_TES_SPANS_PAROLE_ADDRESS.to_str()}` tes_parole_collapsed
            ON tes_parole_collapsed.state_code = tes.state_code
            AND tes_parole_collapsed.person_id = tes.person_id
            AND CURRENT_DATE('US/Pacific') BETWEEN tes_parole_collapsed.start_date AND {nonnull_end_date_exclusive_clause('tes_parole_collapsed.end_date')}
        WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
            AND tes.state_code = 'US_IX'
            AND (tes.is_eligible OR tes.is_almost_eligible)
"""

US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_IX
    ),
    should_materialize=True,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    note_title_regex="r'^{{note_title:(.*?)}}'",
    note_body_regex=" r'{{note:((?s:.*))}}'",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
