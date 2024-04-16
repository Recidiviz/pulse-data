# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Sessionized view of each individual. Session defined as continuous stay within a compartment"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "compartment_sub_sessions_preprocessed"
)

COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = """This is a view that creates an initial set of sub-sessions
from dataflow sessions. This view does the following:
1. Unnest attribute arrays in dataflow_sessions
2. Deduplicate overlapping compartments
3. Fill gaps between sessions
4. Join with dataflow event start/end reasons
5. Create inferred compartments based on start/end reasons
"""

MO_DATA_GAP_DAYS = "10"

COMPARTMENT_SUB_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = """
    WITH session_attributes_unnested AS
    (
    SELECT DISTINCT
        person_id,
        dataflow_session_id,
        state_code,
        start_date,
        end_date_exclusive,
        session_attributes.metric_source,
        session_attributes.compartment_level_1 AS compartment_level_1,
        session_attributes.compartment_level_2 AS compartment_level_2,
        session_attributes.supervising_officer_external_id AS supervising_officer_external_id,
        session_attributes.compartment_location AS compartment_location,
        session_attributes.facility,
        session_attributes.facility_name,
        session_attributes.supervision_office,
        session_attributes.supervision_office_name,
        session_attributes.supervision_district,
        session_attributes.supervision_district_name,
        session_attributes.supervision_region_name,
        session_attributes.correctional_level AS correctional_level,
        session_attributes.correctional_level_raw_text AS correctional_level_raw_text,
        session_attributes.housing_unit AS housing_unit,
        session_attributes.housing_unit_category AS housing_unit_category,
        session_attributes.housing_unit_type AS housing_unit_type,
        session_attributes.housing_unit_type_raw_text AS housing_unit_type_raw_text,
        session_attributes.case_type,
        COALESCE(session_attributes.prioritized_race_or_ethnicity,'EXTERNAL_UNKNOWN') AS prioritized_race_or_ethnicity,
        session_attributes.gender,
        last_day_of_data,
    FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    WHERE session_attributes.compartment_level_1 != 'INCARCERATION_NOT_INCLUDED_IN_STATE'
    )
    ,
    dual_recategorization_cte AS
    (
    SELECT DISTINCT
        person_id,
        dataflow_session_id,
        state_code,
        start_date,
        end_date_exclusive,
        compartment_level_1,
        CASE WHEN cnt > 1 AND compartment_level_1 = 'SUPERVISION' THEN 'DUAL' ELSE compartment_level_2 END AS compartment_level_2,
        supervising_officer_external_id,
        compartment_location,
        facility,
        facility_name,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        supervision_region_name,
        correctional_level,
        correctional_level_raw_text,
        housing_unit,
        housing_unit_category,
        housing_unit_type,
        housing_unit_type_raw_text,
        case_type,
        prioritized_race_or_ethnicity,
        gender,
        metric_source,
        last_day_of_data,
    FROM
        (
        SELECT
            *,
        COUNT(DISTINCT(CASE WHEN compartment_level_2 IN ('PAROLE', 'PROBATION')
            THEN compartment_level_2 END)) OVER(PARTITION BY person_id, state_code, dataflow_session_id, compartment_level_1) AS cnt,
        FROM session_attributes_unnested
        )
    )
    ,
    dedup_compartment_cte AS
    (
    SELECT
        cte.person_id,
        cte.dataflow_session_id,
        cte.state_code,
        cte.metric_source,
        cte.compartment_level_1,
        cte.compartment_level_2,
        cte.supervising_officer_external_id,
        cte.compartment_location,
        cte.facility,
        cte.facility_name,
        cte.supervision_office,
        cte.supervision_office_name,
        cte.supervision_district,
        cte.supervision_district_name,
        cte.supervision_region_name,
        cte.correctional_level,
        cte.correctional_level_raw_text,
        cte.housing_unit,
        cte.housing_unit_category,
        cte.housing_unit_type,
        cte.housing_unit_type_raw_text,
        cte.case_type,
        cte.prioritized_race_or_ethnicity,
        cte.gender,
        cte.start_date,
        cte.end_date_exclusive,
        cte.last_day_of_data,
    FROM dual_recategorization_cte cte
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_1_dedup_priority` cl1_dedup
        USING(compartment_level_1)
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_2_dedup_priority` cl2_dedup
        USING(compartment_level_1, compartment_level_2)
    LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_dedup_priority` sl_dedup
        ON cte.correctional_level = sl_dedup.correctional_level
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, dataflow_session_id
        ORDER BY COALESCE(cl1_dedup.priority, 999),
                COALESCE(cl2_dedup.priority, 999),
                COALESCE(correctional_level_priority, 999),
                NULLIF(supervising_officer_external_id, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(compartment_location, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(case_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(correctional_level_raw_text, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit_category, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit_type_raw_text, 'EXTERNAL_UNKNOWN') NULLS LAST


                ) = 1
    )
     ,
    session_gaps_cte AS
    /*
    Take dataflow_sessions output and fill gaps between sessions. At this point these are identified with a
    metric_source value of "INFERRED" but the compartment values are not specified yet. Once dataflow metrics are joined
    logic is implemented to determine what compartment the gap should represent. This gives full session coverage for
    each person from the start of their first session to the last day for which we have data. Session attribute values
    are also not specified but created as empty strings to allow for a later union with dataflow metrics.
    */
    (
    SELECT
        person_id,
        CAST(NULL AS INT64) AS dataflow_session_id,
        state_code,
        'INFERRED' AS metric_source,
        CAST(NULL AS STRING) AS compartment_level_1,
        CAST(NULL AS STRING) AS compartment_level_2,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        CAST(NULL AS STRING) AS facility_name,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_office_name,
        CAST(NULL AS STRING) AS supervision_district,
        CAST(NULL AS STRING) AS supervision_district_name,
        CAST(NULL AS STRING) AS supervision_region_name,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS housing_unit,
        CAST(NULL AS STRING) AS housing_unit_category,
        CAST(NULL AS STRING) AS housing_unit_type,
        CAST(NULL AS STRING) AS housing_unit_type_raw_text,
        CAST(NULL AS STRING) AS case_type,
        prioritized_race_or_ethnicity,
        gender,
        start_date,
        end_date_exclusive,
        MIN(last_day_of_data) OVER(PARTITION BY state_code) AS last_day_of_data
    FROM
        (
        SELECT
            person_id,
            dataflow_session_id,
            state_code,
            --new session starts the day of the current row's end date exclusive
            end_date_exclusive AS start_date,
            --new session ends the day the following row starts
            LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date ASC) AS end_date_exclusive,
            last_day_of_data,
            prioritized_race_or_ethnicity,
            gender,
        FROM dedup_compartment_cte
        )
    /*
    This where clause ensures that these new release records are only created when there is a gap in sessions.
    The release record start date will be greater than the release record end date when constructed from continuous
    sessions, and will therefore be excluded. In cases where there is a currently active session, no release record will
    be created because the release record start date will be null.
    */
    WHERE COALESCE(end_date_exclusive, '9999-01-01') > start_date
    )
    ,
    session_full_coverage_cte AS
    /*
    Union together the incarceration and supervision sessions with the newly created inferred sessions.
    */
    (
    SELECT *
    FROM dedup_compartment_cte
    UNION ALL
    SELECT * FROM session_gaps_cte
    )
    ,
    sessions_with_start_end_bool AS
    (
    SELECT
        *,
        COALESCE(LAG(CONCAT(compartment_level_1,compartment_level_2)) OVER(PARTITION BY person_id ORDER BY start_date),'')
            != COALESCE(CONCAT(compartment_level_1,compartment_level_2),'') AS first_sub_session,
        COALESCE(LEAD(CONCAT(compartment_level_1,compartment_level_2)) OVER(PARTITION BY person_id ORDER BY start_date),'')
            != COALESCE(CONCAT(compartment_level_1,compartment_level_2),'') AS last_sub_session,
    FROM session_full_coverage_cte
    )
    ,
    sessions_joined_with_dataflow AS
    /*
    Join the sessions_aggregated cte to dataflow metrics to get start and end reasons. Also calculate inflow and
    outflow compartments. This is all information needed to categorize gaps into compartments.
    */
    (
    SELECT
        sessions.*,
        starts.start_reason,
        starts.start_sub_reason,
        -- TODO(#27883): Investigate other cases of same-day supervision and incarceration end reasons
        ends.end_reason AS end_reason_original,
        IF(ends.end_reason IN ('RELEASED_FROM_TEMPORARY_CUSTODY','TEMPORARY_RELEASE')
            AND alternative_ends.end_reason IN ('ADMITTED_TO_INCARCERATION','REVOCATION'),
            ends.end_reason,  alternative_ends.end_reason) AS end_reason_alternative,        
        IF(ends.end_reason IN ('RELEASED_FROM_TEMPORARY_CUSTODY','TEMPORARY_RELEASE')
            AND alternative_ends.end_reason IN ('ADMITTED_TO_INCARCERATION','REVOCATION'),
            alternative_ends.end_reason, ends.end_reason) AS end_reason,
    FROM sessions_with_start_end_bool sessions
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_session_start_reasons_materialized` starts
        ON sessions.person_id =  starts.person_id
        AND sessions.start_date = starts.start_date
        AND sessions.first_sub_session
        AND (sessions.compartment_level_1 = starts.compartment_level_1
        OR (starts.compartment_level_1 = 'SUPERVISION' AND sessions.compartment_level_1 IN ('SUPERVISION_OUT_OF_STATE', 'INVESTIGATION'))
        OR (starts.compartment_level_1 = 'INCARCERATION' AND sessions.compartment_level_1 = 'INCARCERATION_OUT_OF_STATE'))
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_session_end_reasons_materialized` ends
        ON ends.release_termination_date = sessions.end_date_exclusive
        AND ends.person_id = sessions.person_id
        AND sessions.last_sub_session
        AND (sessions.compartment_level_1 = ends.compartment_level_1
        OR (ends.compartment_level_1 = 'SUPERVISION' AND sessions.compartment_level_1 IN ('SUPERVISION_OUT_OF_STATE', 'INVESTIGATION'))
        OR (ends.compartment_level_1 = 'INCARCERATION' AND sessions.compartment_level_1 = 'INCARCERATION_OUT_OF_STATE'))
    -- TODO(#27884): Consider cases of overlapping incarceration and supervision start reasons as well
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_session_end_reasons_materialized` alternative_ends
        ON alternative_ends.release_termination_date = sessions.end_date_exclusive
        AND alternative_ends.person_id = sessions.person_id
        AND sessions.last_sub_session
        AND sessions.compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
        AND alternative_ends.metric_source = 'SUPERVISION_TERMINATION'
    )
    ,
    sessions_with_inflows_outflows AS
    (
    SELECT
        *,
        MIN(start_date) OVER(PARTITION BY person_id) AS earliest_start_date,
        LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date ASC) AS inflow_from_level_1,
        LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date ASC) AS inflow_from_level_2,
        LEAD(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date ASC) AS outflow_to_level_1,
        LEAD(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date ASC) AS outflow_to_level_2,
        LAG(end_reason) OVER (PARTITION BY person_id ORDER BY start_date ASC) AS prev_end_reason,
        LAG(end_reason_alternative) OVER (PARTITION BY person_id ORDER BY start_date ASC) AS prev_end_reason_alternative,
        LEAD(start_reason) OVER (PARTITION BY person_id ORDER BY start_date ASC) AS next_start_reason,
        LEAD(start_sub_reason) OVER (PARTITION BY person_id ORDER BY start_date ASC) AS next_start_sub_reason,
        LAG(end_date_exclusive) OVER (PARTITION BY person_id ORDER BY start_date ASC) AS prev_end_date_exclusive,
        LEAD(start_date) OVER (PARTITION BY person_id ORDER BY start_date ASC) AS next_start_date,
        COALESCE(LAG(CONCAT(compartment_level_1, compartment_level_2)) OVER(PARTITION BY person_id ORDER BY start_date),'') !=
           COALESCE(CONCAT(compartment_level_1, compartment_level_2),'') AS new_compartment,
    FROM sessions_joined_with_dataflow
    )
    ,
    sessions_with_inferred_booleans AS
    (
    SELECT
        *,
        /*
        The following field is used in conjunction with the `is_inferred_compartment` field to enforce that
        recategorization of compartment values are applied to all adjacent sessions of the same original
        compartment type
        */
        SUM(IF(new_compartment,1,0)) OVER(PARTITION BY person_id, state_code ORDER BY start_date) AS session_id_prelim,
        metric_source = 'INFERRED'
            AND prev_end_reason IN (
                    'SENTENCE_SERVED','COMMUTED','DISCHARGE','EXPIRATION','PARDONED',
                    'RELEASED_FROM_ERRONEOUS_ADMISSION', 'RELEASED_FROM_TEMPORARY_CUSTODY',
                    'TEMPORARY_RELEASE', 'VACATED'
                )  AS inferred_release,
        metric_source = 'INFERRED'
            AND (prev_end_reason IN ('ABSCONSION','ESCAPE')
                 OR next_start_reason IN ('RETURN_FROM_ABSCONSION','RETURN_FROM_ESCAPE')
                 OR (next_start_reason = 'REVOCATION' AND next_start_sub_reason = 'ABSCONDED')
                ) AS inferred_escape,
        metric_source = 'INFERRED'
            AND prev_end_reason = 'DEATH' AS inferred_death,
        metric_source = 'INFERRED'
            AND (prev_end_reason = 'RELEASED_IN_ERROR' OR next_start_reason = 'RETURN_FROM_ERRONEOUS_RELEASE') AS inferred_erroneous,
        metric_source = 'INFERRED'
            -- TODO(#27881): Investigate implication of removing inflow_from condition in pending custody inference
            AND inflow_from_level_1 = 'SUPERVISION'
            AND (prev_end_reason in ('REVOCATION', 'ADMITTED_TO_INCARCERATION')
                OR (next_start_reason IN ('REVOCATION', 'SANCTION_ADMISSION'))) AS inferred_pending_custody,
        metric_source = 'INFERRED'
            AND inflow_from_level_1 = 'INCARCERATION'
            AND prev_end_reason IN ('CONDITIONAL_RELEASE', 'RELEASED_TO_SUPERVISION') AS inferred_pending_supervision,
        metric_source = 'INFERRED'
            AND prev_end_reason = 'TRANSFER_TO_OTHER_JURISDICTION' AS inferred_oos,
        metric_source = 'INFERRED'
            AND prev_end_reason = 'SUSPENSION' AS inferred_suspension,
        metric_source = 'INFERRED'
            AND inflow_from_level_1 = outflow_to_level_1 AND inflow_from_level_2 = outflow_to_level_2
            AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER', 'STATUS_CHANGE')
            AND COALESCE(next_start_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER', 'STATUS_CHANGE')
            AND (state_code != 'US_MO' OR DATE_DIFF(next_start_date, prev_end_date_exclusive, DAY) < {mo_data_gap_days}) AS inferred_missing_data,
        metric_source = 'INFERRED'
            AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
            AND COALESCE(next_start_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
            AND state_code = 'US_MO'
            AND DATE_DIFF(COALESCE(next_start_date, last_day_of_data), prev_end_date_exclusive, DAY) >= {mo_data_gap_days} AS inferred_mo_release,
        metric_source = 'INFERRED'
            AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
            AND next_start_reason = 'RETURN_FROM_SUSPENSION'
            AND state_code = 'US_MO' AS inferred_mo_suspension,
        metric_source = 'INFERRED'
            AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
            AND next_start_reason IN ('NEW_ADMISSION', 'TEMPORARY_CUSTODY')
            AND state_code = 'US_MO' AS inferred_mo_pending_custody,
        metric_source = 'INFERRED'
            AND prev_end_reason IN ('ADMITTED_TO_INCARCERATION','REVOCATION')
            AND prev_end_reason_alternative IN ('RELEASED_FROM_TEMPORARY_CUSTODY','TEMPORARY_RELEASE')
            AS inferred_unknown,
    FROM sessions_with_inflows_outflows
    )
    ,
    sessions_with_inferred_compartments AS
    /*
    The subquery uses start reasons, end reasons, inflows and outflows to categorize people into compartments.
    Additional compartments include "LIBERTY", "ABSCONSION", "DEATH", "ERRONEOUS_RELEASE", "PENDING_CUSTODY",
    "PENDING_SUPERVISION", "SUSPENSION", "INCARCERATION - OUT_OF_STATE", and "SUPERVISION - OUT_OF_STATE". The
    "ABSCONSION" compartment type is also applied to non-inferred sessions that have "ABSCONSION" as the start reason.
    Gaps where a person inflows and outflows to the same compartment_level_1 and compartment_level_2 AND has null start
    and end reasons are infilled with the same compartment values as the adjacent sessions. Ultimately the "DEATH"
    compartment gets dropped downstream as it will always (barring data issues) be an active compartment with no outflows.
    */
    (
    SELECT
        person_id,
        dataflow_session_id,
        state_code,
        COALESCE(
            CASE 
                WHEN inferred_unknown THEN 'INTERNAL_UNKNOWN'
                WHEN inferred_release OR inferred_mo_release THEN 'LIBERTY'
                WHEN inferred_escape
                    OR (start_reason = 'ABSCONSION' AND COALESCE(compartment_level_2, "INTERNAL_UNKNOWN") NOT IN ("BENCH_WARRANT", "ABSCONSION"))
                    THEN LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_death THEN 'DEATH'
                WHEN inferred_erroneous THEN 'ERRONEOUS_RELEASE'
                WHEN inferred_missing_data THEN LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_pending_custody OR inferred_mo_pending_custody THEN 'PENDING_CUSTODY'
                WHEN inferred_pending_supervision THEN 'PENDING_SUPERVISION'
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('INCARCERATION','SUPERVISION') THEN CONCAT(LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date), '_', 'OUT_OF_STATE')
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('SUPERVISION_OUT_OF_STATE') THEN 'SUPERVISION_OUT_OF_STATE'
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('INCARCERATION_OUT_OF_STATE') THEN 'INCARCERATION_OUT_OF_STATE'
                WHEN inferred_suspension OR inferred_mo_suspension THEN 'SUSPENSION'
                ELSE compartment_level_1 END, 'INTERNAL_UNKNOWN') AS compartment_level_1,
        COALESCE(
            CASE 
                WHEN inferred_unknown THEN 'INTERNAL_UNKNOWN'
                WHEN inferred_release OR inferred_mo_release THEN 'LIBERTY_REPEAT_IN_SYSTEM'
                WHEN inferred_escape
                    OR (start_reason = 'ABSCONSION' AND COALESCE(compartment_level_2, "INTERNAL_UNKNOWN") NOT IN ("BENCH_WARRANT", "ABSCONSION"))
                    THEN 'ABSCONSION'
                WHEN inferred_death THEN 'DEATH'
                WHEN inferred_erroneous THEN 'ERRONEOUS_RELEASE'
                WHEN inferred_missing_data THEN LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_pending_custody OR inferred_mo_pending_custody THEN 'PENDING_CUSTODY'
                WHEN inferred_pending_supervision THEN 'PENDING_SUPERVISION'
                WHEN inferred_oos THEN LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_suspension OR inferred_mo_suspension THEN 'SUSPENSION'
                ELSE compartment_level_2 END, 'INTERNAL_UNKNOWN') AS compartment_level_2,
        supervising_officer_external_id,
        compartment_location,
        facility,
        facility_name,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        supervision_region_name,
        correctional_level,
        correctional_level_raw_text,
        housing_unit,
        housing_unit_category,
        housing_unit_type,
        housing_unit_type_raw_text,
        case_type,
        prioritized_race_or_ethnicity,
        gender,
        start_date,
        end_date_exclusive,
        start_reason,
        start_sub_reason,
        end_reason_original,
        end_reason_alternative,
        end_reason,
        metric_source,
        last_day_of_data,
        earliest_start_date,
        first_sub_session,
        last_sub_session,
        session_id_prelim,

        /*
        The following field is calculated to tells us which sub-sessions are actually impacted by the inference. This is
        used so that we can enforce that any recategorization of a sub-session compartment is applied to all
        sub-sessions that are adjacent and with the same original compartment type
        */
        COALESCE(inferred_release OR inferred_escape
            OR start_reason = 'ABSCONSION' AND COALESCE(compartment_level_2, "INTERNAL_UNKNOWN") NOT IN ("BENCH_WARRANT", "ABSCONSION")
            OR inferred_death OR inferred_erroneous OR inferred_pending_custody OR inferred_pending_supervision
            OR inferred_oos OR inferred_suspension OR inferred_missing_data OR inferred_mo_release
            OR inferred_mo_suspension OR inferred_mo_pending_custody, FALSE) AS is_inferred_compartment,
    FROM sessions_with_inferred_booleans 
    )
    /*
    In situations where we overwrite an original compartment value due to inference, ensure that all adjacent
    sub-sessions with the same original compartment value also take on this recategorized value.
    */
    SELECT
        * EXCEPT(compartment_level_1, compartment_level_2, session_id_prelim, is_inferred_compartment),
        FIRST_VALUE(compartment_level_1)
            OVER(PARTITION BY person_id, session_id_prelim ORDER BY IF(is_inferred_compartment,0,1)) AS compartment_level_1,
        FIRST_VALUE(compartment_level_2)
            OVER(PARTITION BY person_id, session_id_prelim ORDER BY IF(is_inferred_compartment,0,1)) AS compartment_level_2
    FROM sessions_with_inferred_compartments
"""
COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_NAME,
    view_query_template=COMPARTMENT_SUB_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    description=COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    mo_data_gap_days=MO_DATA_GAP_DAYS,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
