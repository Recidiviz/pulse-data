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
"""Query for relevant metadata needed to support upcoming restrictive housing hearing opportunity in Missouri
TODO(#26722): Deprecate once new opportunities are live.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_start_date_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_mo_query_fragments import (
    classes_cte,
    current_bed_stay_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_NAME = (
    "us_mo_overdue_restrictive_housing_hearing_record"
)

US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support upcoming restrictive housing hearing opportunity in Missouri 
    """

US_MO_CDV_MINOR_NUM_MONTHS_TO_DISPLAY = "6"
US_MO_CDV_MAJOR_NUM_MONTHS_TO_DISPLAY = "12"
US_MO_UPCOMING_HEARING_NUM_DAYS = 7
US_MO_NUM_CLASSES_TO_DISPLAY = 10

US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_QUERY_TEMPLATE = f"""
    WITH base_query AS ({join_current_task_eligibility_spans_with_external_id(
        state_code='"US_MO"',
        tes_task_query_view="overdue_restrictive_housing_hearing_materialized",
        id_type='"US_MO_DOC"',
    )})
    ,
    eligible_and_almost_eligible AS (

        SELECT
            IF(
                is_eligible,
                "OVERDUE",
                IF(
                    is_almost_eligible,
                    "UPCOMING",
                    "OTHER"
                )
            ) AS eligibility_category,
            base_query.*
        FROM base_query
        WHERE 
            'US_MO_IN_RESTRICTIVE_HOUSING' NOT IN UNNEST(ineligible_criteria)            
    )
    ,
    current_confinement_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(start_date) OVER w as start_date,
        FROM `{{project_id}}.{{sessions_dataset}}.us_mo_confinement_type_sessions_materialized`
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY confinement_type_session_id DESC
        )
    )
    ,
    most_recent_hearings AS (
        SELECT DISTINCT
            h.state_code,
            h.person_id,
            FIRST_VALUE(h.hearing_id) OVER person_window AS most_recent_hearing_id,
            FIRST_VALUE(h.hearing_date) OVER person_window AS most_recent_hearing_date,
            FIRST_VALUE(h.hearing_type) OVER person_window AS most_recent_hearing_type,
            FIRST_VALUE(h.hearing_facility) OVER person_window AS most_recent_hearing_facility,
            FIRST_VALUE(h.hearing_comments) OVER person_window AS most_recent_hearing_comments,
        FROM `{{project_id}}.{{analyst_views_dataset}}.us_mo_classification_hearings_preprocessed_materialized` h 
        LEFT JOIN current_confinement_stay c
        USING(state_code, person_id)
        WHERE 
            -- Omit most recent hearing if it occurred before the current stay in solitary
            -- confinement, *and* no review was scheduled later than when the stay began.
            {nonnull_start_date_clause('h.next_review_date')} >= c.start_date
            OR h.hearing_date >= c.start_date
        WINDOW person_window AS (
            PARTITION by h.person_id, h.state_code
            ORDER BY h.hearing_date DESC
        )
    )
    ,
    current_housing_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(facility_code) OVER w as facility_code,
        FROM `{{project_id}}.{{sessions_dataset}}.us_mo_housing_stay_sessions_materialized`
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY housing_stay_session_id DESC
        )
    )
    ,
    {current_bed_stay_cte()}
    ,
    cdv_sanctions_by_person AS (
        SELECT
            person_id,
            ARRAY_AGG(
                STRUCT(
                    incarceration_incident_outcome_id as sanction_id,
                    date_effective as sanction_start_date,
                    projected_end_date as sanction_expiration_date,
                    outcome_type_raw_text as sanction_code
                ) ORDER BY date_effective DESC
            ) AS sanctions,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
        GROUP BY 1
    )
    ,
    cdv_all AS (
        SELECT 
            person_id,
            cdv.incident_date AS cdv_date,
            cdv.incident_type_raw_text as cdv_rule,
            -- Rules are formatted as [major_number].[minor_number] e.g. 11.5
            -- Major violations are rules 1-9
            -- Further info on CDVs here: 
            -- https://doc.mo.gov/sites/doc/files/media/pdf/2020/03/Offender_Rulebook_REVISED_2019.pdf
            CAST(RTRIM(REGEXP_EXTRACT(cdv.incident_type_raw_text, r'.+\\.'), r'\\.') AS INT64) < 10 AS is_major_violation,
            -- Excludes violations that occur the day of a hearing, which could in reality
            -- occur either before or after the hearing, since hearings are likely to be
            -- scheduled further in advance than on the same day as the violation.
            cdv.incident_date > h.most_recent_hearing_date AS occurred_since_last_hearing,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` cdv
        LEFT JOIN most_recent_hearings h
        USING(person_id)
    )
    ,
    cdv_majors_recent AS (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(cdv_date, cdv_rule) ORDER BY cdv_date DESC) AS cdvs
        FROM cdv_all
        WHERE
            cdv_date >= DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL {{us_mo_cdv_major_months_to_display}} MONTH)
            AND is_major_violation
        GROUP BY 1
    )
    ,
    cdv_minors_since_hearing AS (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(cdv_date, cdv_rule) ORDER BY cdv_date DESC) AS cdvs
        FROM cdv_all
        WHERE
            occurred_since_last_hearing
            AND NOT is_major_violation
        GROUP BY 1
    )
    ,
    cdv_minors_recent AS (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(cdv_date, cdv_rule) ORDER BY cdv_date DESC) AS cdvs
        FROM cdv_all
        WHERE
            cdv_date >= DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL {{us_mo_cdv_minor_months_to_display}} MONTH)
            AND NOT occurred_since_last_hearing
            AND NOT is_major_violation
        GROUP BY 1
    )
    ,
    -- TODO(#19220): Reference state_assessment once these screeners are ingested
    mental_health_latest AS (
        SELECT DISTINCT
            person_id,
            FIRST_VALUE(assessment_date) OVER person_scores,
            FIRST_VALUE(assessment_score) OVER person_scores as mental_health_assessment_score,
        FROM `{{project_id}}.{{analyst_dataset}}.us_mo_screeners_preprocessed_materialized` 
        WHERE assessment_type = 'mental_health'
        WINDOW person_scores AS (
            PARTITION BY person_id, state_code
            ORDER BY assessment_date DESC
        )
    )
    ,
    {classes_cte()}
    ,
    classes_with_date AS (
        SELECT 
            c.DOC_ID as external_id,
            IF(ACTUAL_START_DT = '7799-12-31', NULL, SAFE.PARSE_DATE('%Y-%m-%d', ACTUAL_START_DT)) as start_date,
            IF(ACTUAL_EXIT_DT = '7799-12-31', NULL, SAFE.PARSE_DATE('%Y-%m-%d', ACTUAL_EXIT_DT)) as end_date,
            CLASS_TITLE as class_title,
            CLASS_EXIT_REASON_DESC as class_exit_reason,
        FROM classes c
        /* EXIT_TYPE_CD and CLASS_EXIT_REASON_DESC together describe the reason someone stopped taking a class.
        Each description (CLASS_EXIT_REASON_DESC) is under the umbrella of a more general EXIT_TYPE_CD category:
        "UNS": Unsuccessful, "SFL": Successful, and "NOF": (Unsuccessful) No-fault exit
        Here we omit Unsuccessful classes so as only to surface classes that were either completed or were not
        completed, but through no fault of the person taking them. */
        WHERE COALESCE(EXIT_TYPE_CD, '') != 'UNS'
    )
    ,
    classes_by_person_recent AS (
        SELECT
            external_id,
            ARRAY_AGG(
                STRUCT(
                    start_date, 
                    end_date, 
                    class_title, 
                    class_exit_reason
                ) 
                ORDER BY start_date DESC
                LIMIT {US_MO_NUM_CLASSES_TO_DISPLAY}
            ) AS classes
        FROM classes_with_date
        WHERE start_date IS NOT NULL
        GROUP BY 1
    )
    ,
    aic_scores_latest AS (
        SELECT 
            GY_DOC AS external_id, 
            (
                CASE GY_AIS
                WHEN 'X' THEN 'Alpha'
                WHEN 'Y' THEN 'Kappa'
                WHEN 'Z' THEN 'Sigma'
                ELSE 'Unknown' END
            ) AS aic_score,
        FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK194_latest`
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY GY_DOC
            ORDER BY GY_BH DESC
        ) = 1
    )
    ,
    -- List of "unwaived enemies" of people in Restrictive Housing who are in the same facility.
    -- Indicates that those people should be separated from each other, and used in determining where
    -- people can be assigned.
    unwaived_enemies AS (
        SELECT
          enemy.CD_DOC as external_id, 
          enemy.CD_ENY as enemy_external_id,
          bed_enemy.bed_number as enemy_bed_number,
          bed_enemy.room_number as enemy_room_number,
          bed_enemy.complex_number as enemy_complex_number,
          bed_enemy.building_number as enemy_building_number,
          bed_enemy.housing_use_code as enemy_housing_use_code,
          bed_enemy.facility as enemy_facility,
        FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK033_latest` enemy
        -- Join on CD_ENY to get person in Restrictive Housing's enemies' bed stays
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei_enemy
        ON
            enemy.CD_ENY = pei_enemy.external_id
            AND pei_enemy.state_code = 'US_MO'
        -- Join on CD_DOC to get person in Restrictive Housing's facility
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei_self
        ON
            enemy.CD_DOC = pei_self.external_id
            AND pei_self.state_code = 'US_MO'
        LEFT JOIN current_bed_stay bed_enemy
        ON pei_enemy.person_id = bed_enemy.person_id
        LEFT JOIN current_bed_stay bed_self
        ON pei_self.person_id = bed_self.person_id
        WHERE 
            -- Enemy is not "waived"
            CD_EWA = 'N' 
            -- Unwaived enemy is in same facility
            AND bed_enemy.facility = bed_self.facility 
    )
    ,
    unwaived_enemies_by_person AS (
        SELECT
            external_id,
            ARRAY_AGG(
                STRUCT(
                    enemy_external_id,
                    enemy_bed_number,
                    enemy_room_number,
                    enemy_complex_number,
                    enemy_building_number,
                    enemy_housing_use_code
                ) ORDER BY enemy_external_id ASC
            ) AS unwaived_enemies,
        FROM unwaived_enemies
        GROUP BY 1
    )
    ,
    final AS (
        SELECT
            base.external_id,
            base.state_code,
            base.reasons,
            base.is_eligible,
            base.eligibility_category,
            base.ineligible_criteria,
            hearings.most_recent_hearing_date AS metadata_most_recent_hearing_date,
            hearings.most_recent_hearing_type AS metadata_most_recent_hearing_type,
            hearings.most_recent_hearing_facility AS metadata_most_recent_hearing_facility,
            hearings.most_recent_hearing_comments AS metadata_most_recent_hearing_comments,
            housing.facility_code AS metadata_current_facility,
            confinement.start_date AS metadata_restrictive_housing_start_date,
            bed.bed_number AS metadata_bed_number,
            bed.room_number AS metadata_room_number,
            bed.complex_number AS metadata_complex_number,
            bed.building_number AS metadata_building_number,
            bed.housing_use_code AS metadata_housing_use_code,
            TO_JSON(IFNULL(cdv_majors_recent.cdvs, [])) AS metadata_major_cdvs,
            TO_JSON(IFNULL(cdv_minors_since_hearing.cdvs, [])) AS metadata_cdvs_since_last_hearing,
            IFNULL(ARRAY_LENGTH(cdv_minors_recent.cdvs), 0) AS metadata_num_minor_cdvs_before_last_hearing,
            mental_health_latest.mental_health_assessment_score AS metadata_mental_health_assessment_score,
            TO_JSON(IFNULL(classes_by_person_recent.classes, [])) AS metadata_classes_recent,
            aic_scores_latest.aic_score as metadata_aic_score,
            TO_JSON(IFNULL(unwaived_enemies_by_person.unwaived_enemies, [])) AS metadata_unwaived_enemies,
            TO_JSON(IFNULL(cdv_sanctions_by_person.sanctions, [])) AS metadata_all_sanctions,
        FROM eligible_and_almost_eligible base
        LEFT JOIN most_recent_hearings hearings
        USING (person_id)
        LEFT JOIN current_housing_stay housing
        USING (person_id)
        LEFT JOIN current_confinement_stay confinement
        USING (person_id)
        LEFT JOIN current_bed_stay bed
        USING (person_id)
        LEFT JOIN cdv_majors_recent
        USING (person_id)
        LEFT JOIN cdv_minors_since_hearing
        USING (person_id)
        LEFT JOIN cdv_minors_recent
        USING (person_id)
        LEFT JOIN mental_health_latest
        USING (person_id)
        LEFT JOIN classes_by_person_recent
        USING (external_id)
        LEFT JOIN aic_scores_latest
        USING (external_id)
        LEFT JOIN unwaived_enemies_by_person
        USING (external_id)
        LEFT JOIN cdv_sanctions_by_person
        USING (person_id)
    )
    SELECT * FROM final
"""

US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_NAME,
    view_query_template=US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_QUERY_TEMPLATE,
    description=US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MO
    ),
    should_materialize=True,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
    ),
    us_mo_cdv_minor_months_to_display=US_MO_CDV_MINOR_NUM_MONTHS_TO_DISPLAY,
    us_mo_cdv_major_months_to_display=US_MO_CDV_MAJOR_NUM_MONTHS_TO_DISPLAY,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER.build_and_print()
