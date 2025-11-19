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
"""
Helper SQL queries for Michigan
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    TaskEligibilitySpansSubsetType,
    array_agg_case_notes_by_person_id,
    select_relevant_task_eligibility_spans_for_record,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.string_formatting import fix_indent


def generate_sccp_form_opportunity_record_view_builder(
    description: str,
    view_id: str,
    tes_view_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
    latest_scc_review_date_criteria_builder: TaskCriteriaBigQueryViewBuilder,
    latest_scc_review_date_reasons_field: str,
) -> SimpleBigQueryViewBuilder:
    """
    Creates a big query view builder that generates the opportunity record query needed for all
    security classification committee review opportunities.

    Args:
        description (str): The description for the returned opportunity record view
        view_id (str): The view_id / table_id for the opportunity record view
        tes_view_builder (SingleTaskEligibilitySpansBigQueryViewBuilder): The view
            builder object for the task eligibility spans that will be used for this
            opportunity record view.
        latest_scc_review_date_criteria_builder (TaskCriteriaBigQueryViewBuilder): The
            view builder for the criteria that will be used to hydrate the
            latest_scc_review_date column.
        latest_scc_review_date_reasons_field (str): The name of the field in the reasons
            blob for latest_scc_review_date_criteria_builder that we should use to pull
            the latest_scc_review_date value.

    Returns:
        SimpleBigQueryViewBuilder
    """
    filtered_tes_query = select_relevant_task_eligibility_spans_for_record(
        tes_view_builder=tes_view_builder,
        spans_subset_type=TaskEligibilitySpansSubsetType.ELIGIBLE_AND_ALMOST_ELIGIBLE_ONLY,
        include_eligible_date=False,
        include_reasons_v2=False,
    )

    query_template = f"""
WITH eligible_and_almost_eligible AS (
{fix_indent(filtered_tes_query, indent_level=4)}
),
bondable_codes AS (
/* Queries all bondable codes for misconduct reports in the last 6 months */
    SELECT
        person_id,
        incident_date,
        SPLIT(JSON_EXTRACT_SCALAR(incident_metadata, '$.BONDABLE_OFFENSES'), ',') AS bondable_offenses
    FROM `{{project_id}}.us_mi_normalized_state.state_incarceration_incident`
    WHERE 
        state_code = "US_MI"
        AND DATE_ADD(incident_date, INTERVAL 6 MONTH) >= CURRENT_DATE('US/Eastern')
        AND JSON_EXTRACT_SCALAR(incident_metadata, '$.BONDABLE_OFFENSES') != ''
    ),
nonbondable_codes AS (
/* Queries all nonbondable codes for misconduct reports in the last year */
    SELECT
        person_id,
        incident_date,
        SPLIT(JSON_EXTRACT_SCALAR(incident_metadata, '$.NONBONDABLE_OFFENSES'), ',') AS nonbondable_offenses
    FROM `{{project_id}}.us_mi_normalized_state.state_incarceration_incident`
    WHERE 
        state_code = "US_MI"
        AND DATE_ADD(incident_date, INTERVAL 1 YEAR) >= CURRENT_DATE('US/Eastern')
        AND JSON_EXTRACT_SCALAR(incident_metadata, '$.NONBONDABLE_OFFENSES') != ''
),
all_offenses_unnested AS (
/* Unnest all bondable and nonbondable offenses for reuse across queries */
    SELECT 
        b.person_id,
        b.incident_date,
        bondable_offense,
        NULL AS nonbondable_offense
    FROM bondable_codes b,
        UNNEST(b.bondable_offenses) AS bondable_offense
    
    UNION ALL
    
    SELECT 
        n.person_id,
        n.incident_date,
        NULL AS bondable_offense,
        nonbondable_offense
    FROM nonbondable_codes n,
        UNNEST(n.nonbondable_offenses) AS nonbondable_offense
),
form_misconduct_codes AS (
/* Queries bondable and nonbondable codes to include on the scc form. 
For bondable codes, the resident can remain in their current cell. 
For nonbondable code, the resident must be moved to segregation until ticket is heard. */
    SELECT
        person_id,
        CONCAT('(', 
            STRING_AGG(DISTINCT CONCAT(bondable_offense, ', ', STRING(incident_date)), '), (' ORDER BY CONCAT(bondable_offense, ', ', STRING(incident_date))),
        ')') AS bondable_offenses_within_6_months,
        CONCAT('(',
            STRING_AGG(DISTINCT CONCAT(nonbondable_offense, ', ', STRING(incident_date)), '), (' ORDER BY CONCAT(nonbondable_offense, ', ', STRING(incident_date))),
        ')') AS nonbondable_offenses_within_1_year
    FROM all_offenses_unnested
    GROUP BY person_id
),
recent_misconduct_distinct_offenses AS (
/* Distinct offenses within current housing sessions for metadata */
    SELECT DISTINCT
        h.person_id,
        o.bondable_offense,
        o.incident_date AS bondable_incident_date,
        o.nonbondable_offense,
        o.incident_date AS nonbondable_incident_date
    FROM  all_offenses_unnested o
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.housing_unit_type_collapsed_solitary_sessions_materialized` h
        ON o.person_id = h.person_id 
        AND o.incident_date >= h.start_date 
        AND CURRENT_DATE('US/Eastern') BETWEEN h.start_date AND {nonnull_end_date_exclusive_clause('h.end_date_exclusive')}
),
recent_misconduct_codes AS (
/* Queries bondable and nonbondable codes within the current housing_unit_session for metadata. */
    SELECT 
        person_id,
        ARRAY_AGG(
            IF(bondable_offense IS NOT NULL, 
               TO_JSON(STRUCT(bondable_offense AS bondable_offense,
                       bondable_incident_date AS bondable_incident_date)), 
               NULL)
            IGNORE NULLS ORDER BY bondable_offense, bondable_incident_date) 
            AS json_bondable_offenses_within_6_months,
        ARRAY_AGG(
            IF(nonbondable_offense IS NOT NULL, 
               TO_JSON(STRUCT(nonbondable_offense AS nonbondable_offense,
                       nonbondable_incident_date AS nonbondable_incident_date)), 
               NULL)
            IGNORE NULLS ORDER BY nonbondable_offense, nonbondable_incident_date) 
            AS json_nonbondable_offenses_within_1_year,
        CONCAT('(',
            STRING_AGG(CONCAT(bondable_offense, ', ', STRING(bondable_incident_date)), '), (' ORDER BY CONCAT(bondable_offense, ', ', STRING(bondable_incident_date))),
        ')') AS bondable_offenses_within_6_months,
        CONCAT('(',
            STRING_AGG(CONCAT(nonbondable_offense, ', ', STRING(nonbondable_incident_date)), '), (' ORDER BY CONCAT(nonbondable_offense, ', ', STRING(nonbondable_incident_date))),
        ')') AS nonbondable_offenses_within_1_year,
    FROM recent_misconduct_distinct_offenses
    GROUP BY 1
),
ad_seg_stays AS (
  SELECT 
    h.person_id,
    h.start_date,
    h.end_date_exclusive,
    --concatenate nonbondable and bondable offenses 
    COALESCE(IF(JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES') != "", 
        CONCAT(JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES'), ",", 
        JSON_EXTRACT_SCALAR(i.incident_metadata, '$.BONDABLE_OFFENSES')),
        JSON_EXTRACT_SCALAR(i.incident_metadata, '$.BONDABLE_OFFENSES')), "") AS offenses
  FROM (
    SELECT * 
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized`
    WHERE state_code = 'US_MI'
      AND housing_unit_type = 'ADMINISTRATIVE_SOLITARY_CONFINEMENT'
      --only join previous (closed) ad seg stays 
      AND end_date_exclusive IS NOT NULL
      --only select stays greater than one month 
      AND DATE_DIFF(end_date_exclusive, start_date, DAY) >= 30
      --only select stays within the last 3 years
      AND DATE_ADD(start_date, INTERVAL 3 YEAR) >= CURRENT_DATE('US/Eastern')
  ) h
  LEFT JOIN 
    `{{project_id}}.us_mi_normalized_state.state_incarceration_incident` i
  ON 
    h.state_code = i.state_code
    AND h.person_id = i.person_id 
    --only join incidents that happen within 2 months of the segregation stay 
    AND DATE_DIFF(start_date, incident_date, DAY) < 60
    AND i.incident_date <= h.start_date
  --choose the most recent incident report within the last 2 months and prioritize the incident with the bondable code
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY h.person_id, h.start_date 
    ORDER BY 
        JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES') = "", 
        incident_date DESC,
        -- In the case when there are multiple NONBONDABLE_OFFENSES on the same date,
        -- pick one arbitrarily
        JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES')
  ) = 1
),
previous_ad_seg_stays_for_form AS (
/* Used for form filling:
Queries all previous ad seg stays from the past three years, and the associates the closest incarceration incident.
30 days in ad seg is used a proxy to determine "true" ad seg stays, vs. time spent in an ad seg cell */
SELECT 
  person_id,
  ARRAY_AGG(
    CONCAT('(', STRING(start_date), ',', offenses, ')') IGNORE NULLS ORDER BY start_date, offenses
  ) AS form_ad_seg_stays_and_reasons_within_3_yrs
FROM ad_seg_stays
GROUP BY person_id
),
previous_ad_seg_stays AS (
/* Used for sidebar component: 
Queries all previous ad seg stays from the past three years, and the associates the closest incarceration incident.
30 days in ad seg is used a proxy to determine "true" ad seg stays, vs. time spent in an ad seg cell */
SELECT 
  person_id,
  ARRAY_AGG(TO_JSON(STRUCT(start_date AS stay_start_date,
                end_date_exclusive AS stay_end_date,
                offenses AS stay_offenses))
                IGNORE NULLS ORDER BY start_date, end_date_exclusive, offenses) AS json_ad_seg_stays_and_reasons_within_3_yrs
FROM ad_seg_stays
GROUP BY person_id
),
reasons_for_eligibility AS (
/* Queries the reasons for eligibility from the currently eligible or upcoming eligible span */
    SELECT 
        person_id,
        {extract_object_from_json(
            json_column="criteria_reason",
            object_column=f"reason.{latest_scc_review_date_reasons_field}",
            object_type="DATE",
        )} AS latest_scc_review_date,
        {extract_object_from_json(
            json_column="criteria_reason",
            object_column="reason.next_scc_date",
            object_type="DATE",
        )} AS next_scc_date,
    FROM eligible_and_almost_eligible,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    -- Pull out the reasons blob from the one criteria with review date metadata
    WHERE {extract_object_from_json(
            json_column="criteria_reason",
            object_column="criteria_name",
            object_type="STRING",
        )} = "{latest_scc_review_date_criteria_builder.criteria_name}"
),
SMI AS (
  SELECT person_id, MMD
  FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Mental_Health_Problems_latest`
  INNER JOIN `{{project_id}}.us_mi_normalized_state.state_person_external_id` pei
      ON OffenderCode = pei.external_id 
          AND id_type = 'US_MI_DOC'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY CAST(StatusDate AS DATETIME) DESC) = 1
),
OPT AS (
  SELECT person_id, OPTLevelOfCare
  FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Level_Of_Care_latest`
  INNER JOIN `{{project_id}}.us_mi_normalized_state.state_person_external_id` pei
      ON OffenderCode = pei.external_id 
          AND id_type = 'US_MI_DOC'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY CAST(AdmissionDate AS DATETIME) DESC) = 1
),
stg_information AS (
    SELECT 
        pei.person_id,
        STG_Level
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Security_Threat_Group_Involvement_latest`
    INNER JOIN `{{project_id}}.us_mi_normalized_state.state_person_external_id` pei
        ON Offender_Number = pei.external_id 
            AND id_type = 'US_MI_DOC'
    -- there are a small number of times where a single person appears wih two distinct STG levels in the table
    -- in those cases, let's pick the STG level that was most recently entered
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY CAST(Start_Date AS DATETIME) DESC, STG_LEVEL DESC) = 1
),
assessment_information AS (
    SELECT 
        pei.person_id,
        Management_Level_Assessment_Result,
        Confinement_Level_Assessment_Result,
        Actual_Placement_Level_Assessment_Result,
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Security_Classification_latest`
    INNER JOIN `{{project_id}}.us_mi_normalized_state.state_person_external_id` pei
        ON Offender_Number = pei.external_id 
            AND id_type = 'US_MI_DOC'
),
case_notes_cte AS (
    SELECT
        person_id,
        cn.Activity AS criteria,
        NULL AS note_title,
        cn.Note AS note_body,
        DATE(cn.Note_Date) AS event_date
    FROM (
        SELECT pei.person_id, Note, Note_Date, Activity
        FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Case_Notes_latest`
        INNER JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Supervision_Schedule_Activities_latest` 
            USING(Offender_Number, Case_Note_Id)
        INNER JOIN `{{project_id}}.us_mi_normalized_state.state_person_external_id` pei
            ON Offender_Number = pei.external_id 
                AND id_type = 'US_MI_DOC'
    ) cn
    LEFT JOIN (
        SELECT person_id, start_date, end_date_exclusive 
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized` s
        WHERE state_code = "US_MI" AND CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
    ) s
    USING (person_id)
    WHERE Activity in ('SCC - Security Classification Committee','SCC – ADD – 12 Month Review','SCC – Warden – 6 Month Review','In Person Contact')
        -- Filter to notes for the current super session
        AND DATE(cn.Note_Date) BETWEEN s.start_date AND {nonnull_end_date_exclusive_clause('s.end_date_exclusive')}
),
array_case_notes_cte AS (
{array_agg_case_notes_by_person_id()}
)
SELECT 
    person_id,
    state_code,
    stable_person_external_id AS external_id,
    tes.reasons,
    tes.is_eligible,
    tes.is_almost_eligible,
    tes.ineligible_criteria,
    IF(tes.is_eligible,DATE_DIFF(CURRENT_DATE("US/Eastern"), e.next_scc_date, DAY)>=7, False) AS is_overdue,
    --form information
    display_person_external_id AS form_information_prisoner_number,
    INITCAP(JSON_VALUE(PARSE_JSON(sp.full_name), '$.given_names'))
            || " " 
            || INITCAP(JSON_VALUE(PARSE_JSON(sp.full_name), '$.surname')) AS form_information_prisoner_name,
    DATE(sgt.person_projected_full_term_release_date_max) AS form_information_max_release_date,
    DATE(sgt.person_projected_full_term_release_date_min) AS form_information_min_release_date,
    si.facility AS form_information_facility,
    si.housing_unit AS form_information_lock,
    (COALESCE(SMI.MMD, "No") = "Yes") AS form_information_OPT,
    CASE WHEN stg.STG_Level = 'I' THEN "1"
         WHEN stg.STG_Level = 'II' THEN "2"
         ELSE stg.STG_Level 
      END AS form_information_STG,
    h.housing_unit_type AS form_information_segregation_type,
    h.start_date AS form_information_segregation_classification_date,
    m.bondable_offenses_within_6_months AS form_information_bondable_offenses_within_6_months,
    m.nonbondable_offenses_within_1_year AS form_information_nonbondable_offenses_within_1_year,
    pf.form_ad_seg_stays_and_reasons_within_3_yrs AS form_information_ad_seg_stays_and_reasons_within_3_yrs,
    --metadata 
    e.latest_scc_review_date AS metadata_latest_scc_review_date,
    DATE(sgt.person_projected_full_term_release_date_max) AS metadata_max_release_date,
    DATE(sgt.person_projected_full_term_release_date_min) AS metadata_min_release_date,
    DATE_DIFF(DATE(sgt.person_projected_full_term_release_date_min), CURRENT_DATE('US/Eastern'), MONTH) <24 AS metadata_less_than_24_months_from_erd,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), h.start_date, DAY) AS metadata_days_in_solitary_session,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), hc.start_date, DAY) AS metadata_days_in_collapsed_solitary_session,
    h.start_date AS metadata_solitary_session_start_date,
    CASE WHEN h.housing_unit_type = 'ADMINISTRATIVE_SOLITARY_CONFINEMENT' THEN 'Administrative Segregation'
            WHEN h.housing_unit_type = 'TEMPORARY_SOLITARY_CONFINEMENT' THEN 'Temporary Segregation'
            WHEN h.housing_unit_type = 'DISCIPLINARY_SOLITARY_CONFINEMENT' THEN 'Detention'
            WHEN h.housing_unit_type = 'MENTAL_HEALTH_SOLITARY_CONFINEMENT' THEN 'Observation'
            ELSE 'Other Segregation'
    END AS metadata_solitary_session_type,
    (OPT.OPTLevelOfCare = "Y") AS metadata_OPT,
    rm.json_bondable_offenses_within_6_months AS metadata_json_recent_bondable_offenses,
    rm.json_nonbondable_offenses_within_1_year AS metadata_json_recent_nonbondable_offenses,
    #TODO(#51218) remove once frontend is updated to json field
    rm.bondable_offenses_within_6_months AS metadata_recent_bondable_offenses,
    rm.nonbondable_offenses_within_1_year AS metadata_recent_nonbondable_offenses,
    #TODO(#51218) remove once frontend is updated to json field
    pf.form_ad_seg_stays_and_reasons_within_3_yrs AS metadata_ad_seg_stays_and_reasons_within_3_yrs,
    p.json_ad_seg_stays_and_reasons_within_3_yrs AS metadata_json_ad_seg_stays_and_reasons_within_3_yrs,
    #TODO(#28298) add in missing info when we receive data
    NULL AS metadata_needed_programming,
    NULL AS metadata_completed_programming,
    assessment.Management_Level_Assessment_Result AS metadata_management_security_level,
    assessment.Confinement_Level_Assessment_Result AS metadata_confinement_security_level,
    assessment.Actual_Placement_Level_Assessment_Result AS metadata_actual_security_level,
    a.case_notes 
FROM eligible_and_almost_eligible tes
LEFT JOIN `{{project_id}}.us_mi_normalized_state.state_person` sp
    USING (state_code, person_id)
# TODO(#45179): Once the frontend does not read from the external_id column anymore, 
# remove this join / the external_id column.
LEFT JOIN (
    SELECT state_code, person_id, stable_person_external_id
    FROM `{{project_id}}.reference_views.product_stable_person_external_ids_materialized`
    WHERE system_type = "INCARCERATION"
)
USING
    (state_code, person_id)
LEFT JOIN (
    SELECT state_code, person_id, display_person_external_id
    FROM `{{project_id}}.reference_views.product_display_person_external_ids_materialized`
    WHERE system_type = "INCARCERATION"
)
USING
    (state_code, person_id)
LEFT JOIN (
    SELECT state_code, person_id, facility, housing_unit
    FROM `{{project_id}}.us_mi_normalized_state.state_incarceration_period`
    WHERE CURRENT_DATE('US/Eastern') BETWEEN admission_date AND {nonnull_end_date_clause('release_date')}
) si
USING (state_code, person_id)
LEFT JOIN `{{project_id}}.{{sentence_sessions_dataset}}.current_person_prison_projected_dates_materialized` sgt
    USING (state_code, person_id)
LEFT JOIN form_misconduct_codes m
    USING (person_id)
LEFT JOIN recent_misconduct_codes rm
    USING (person_id)
LEFT JOIN previous_ad_seg_stays_for_form pf
    USING (person_id)
LEFT JOIN previous_ad_seg_stays p
    USING (person_id)
LEFT JOIN (
    SELECT state_code, person_id, housing_unit_type, start_date
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized`
    WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
) h
    USING (state_code, person_id)
LEFT JOIN (
    SELECT state_code, person_id, start_date
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_non_protective_custody_solitary_sessions_materialized`
    WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
) hc
    USING (state_code, person_id)
LEFT JOIN reasons_for_eligibility e
    USING (person_id)
LEFT JOIN assessment_information assessment
    USING (person_id)
LEFT JOIN OPT opt
    USING (person_id)
LEFT JOIN SMI smi
    USING (person_id)
LEFT JOIN stg_information stg
    USING (person_id)
LEFT JOIN array_case_notes_cte a 
    USING (person_id)

    """
    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
        view_id=view_id,
        view_query_template=query_template,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
        sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
        us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
        ),
        should_materialize=True,
    )


def secondary_officer_dockets_cte() -> str:
    return """
        agent_in_client_rec AS (
            SELECT 
                state_code,
                person_id,
                supervising_officer_external_id AS Case_Manager_Omnni_Employee_Id
            FROM `{project_id}.workflows_views.client_record_supervision_cases_materialized`
        ),
        secondary_officer_metadata AS (
            SELECT 
                person_id,
                ARRAY_AGG(
                    INITCAP(CONCAT(JSON_EXTRACT_SCALAR(s.full_name, "$.given_names"), 
                           " ", 
                           JSON_EXTRACT_SCALAR(s.full_name, "$.surname")))
                    ORDER BY Case_Manager_Omnni_Employee_Id
                ) AS metadata_officers
            FROM `{project_id}.{us_mi_raw_data_up_to_date_dataset}.COMS_Case_Managers_latest` cm
            LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` ON Offender_Number = external_id AND id_type = 'US_MI_DOC'
            LEFT JOIN `{project_id}.{us_mi_raw_data_up_to_date_dataset}.COMS_Supervision_Statuses_latest` ss USING(Supervision_Status_Id, Offender_Number)
            LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id` sei ON Case_Manager_Omnni_Employee_Id = sei.external_id AND sei.id_type = 'US_MI_OMNI_USER'
            LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff` s ON sei.staff_id = s.staff_id
            LEFT JOIN agent_in_client_rec USING(person_id, Case_Manager_Omnni_Employee_Id)
            WHERE 
                -- agent is currently supervising the client for an open supervision status
                cm.end_date is null AND ss.end_date is null
                -- only grab supervision-related agents
                AND Supervision_Status IN ('Parole', 'Probation', 'Delayed Sentence', 'Interstate Compact Parole', 'Interstate Compact Probation', 'Pending MDOC Custody')
                -- only grab agents that aren't already the primary agent in the client record
                AND agent_in_client_rec.Case_Manager_Omnni_Employee_Id IS NULL
            GROUP BY 1
        ),
        dockets_metadata AS (
            SELECT 
                person_id,
                ARRAY_AGG(
                TO_JSON(
                    STRUCT(docket_number_description AS docket_number,
                        ref1.description AS legal_order_type, 
                        DATE(effective_date) AS legal_order_effective_date, 
                        DATE(expiration_date) AS legal_order_expiration_date,
                        loc.name AS issue_location)
                )
                ORDER BY docket_number_description
                ) AS metadata_dockets
            FROM `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_LEGAL_ORDER_latest`
            LEFT JOIN `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_REFERENCE_CODE_latest` ref1 ON order_type_id = ref1.reference_code_id
            LEFT JOIN `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_REFERENCE_CODE_latest` ref2 ON order_status_id = ref2.reference_code_id
            LEFT JOIN `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_LOCATION_latest` loc ON issue_location_id = loc.location_id
            LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei ON external_id = offender_booking_id AND id_type = 'US_MI_DOC_BOOK'
            WHERE
                -- only grab open legal orders 
                ref2.description = 'Active'
                -- only include Parole/Probation dockets
                AND ref1.description IN ('Probation', 'Parole')
            GROUP BY 1 
        )
    """
