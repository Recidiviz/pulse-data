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
"""Generic opportunity record information for someone in Restrictive Housing"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_exclusive_clause,
    nonnull_start_date_clause,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.task_eligibility.utils.us_mo_query_fragments import (
    classes_cte,
    current_bed_stay_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_NAME = "us_mo_restrictive_housing_record"

US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_DESCRIPTION = (
    """Generic opportunity record information for someone in Restrictive Housing"""
)

US_MO_CDV_MINOR_NUM_MONTHS_TO_DISPLAY = "6"
US_MO_CDV_MAJOR_NUM_MONTHS_TO_DISPLAY = "12"
US_MO_NUM_CLASSES_TO_DISPLAY = 10

US_MO_RESTRICTIVE_HOUSING_RECORD_QUERY_TEMPLATE = f"""
    WITH base_query AS (
        SELECT
            pei.external_id,
            cs.person_id,
            cs.state_code,
        -- Populate for all people incarcerated in MO, since some of these fields may be
        -- used in the resident record.
        FROM `{{project_id}}.sessions.compartment_sessions_materialized` cs
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            ON
                cs.person_id = pei.person_id
                AND cs.state_code = pei.state_code
                AND pei.id_type = "US_MO_DOC"
        WHERE 
            {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="cs.start_date",
                end_date_column="cs.end_date_exclusive"
            )}
            AND cs.compartment_level_1 = 'INCARCERATION'
            AND cs.state_code = "US_MO"
    )
    ,
    current_confinement_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(start_date) OVER w AS start_date,
        FROM `{{project_id}}.sessions.us_mo_confinement_type_sessions_materialized`
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
        FROM `{{project_id}}.analyst_data.us_mo_classification_hearings_preprocessed_materialized` h 
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
            FIRST_VALUE(facility_code) OVER w AS facility_code,
        FROM `{{project_id}}.sessions.us_mo_housing_stay_sessions_materialized`
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY housing_stay_session_id DESC
        )
    )
    ,
    {current_bed_stay_cte()}
    ,
    solitary_assignments AS (
        SELECT
            person_id,
            start_date,
            end_date,
        FROM `{{project_id}}.sessions.us_mo_confinement_type_sessions_materialized`
        WHERE 
            confinement_type = 'SOLITARY_CONFINEMENT'
            AND start_date >= DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 1 YEAR)
    )
    ,
    solitary_assignments_by_person AS (
        SELECT
            person_id,
            COUNT(*) AS num_solitary_assignments_past_year,
            ARRAY_AGG(
                STRUCT(
                    start_date,
                    end_date
                ) ORDER BY start_date DESC
            ) AS solitary_assignment_dates,
        FROM solitary_assignments
        GROUP BY 1
    )
    ,
    d1_sanctions AS (
        SELECT
            person_id,
            incarceration_incident_outcome_id AS sanction_id,
            date_effective AS sanction_start_date,
            projected_end_date AS sanction_expiration_date,
            outcome_type_raw_text AS sanction_code
        FROM `{{project_id}}.normalized_state.state_incarceration_incident_outcome`
        WHERE 
            state_code = 'US_MO'
            AND outcome_type_raw_text = 'D1'
            AND date_effective >= DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 1 YEAR)
    )
    ,
    cdv_d1_sanctions_by_person AS (
        SELECT
            person_id,
            COUNT(*) AS num_d1_sanctions_past_year,
            ARRAY_AGG(
                STRUCT(
                    sanction_id,
                    sanction_start_date,
                    sanction_expiration_date,
                    sanction_code
                ) ORDER BY sanction_start_date DESC
            ) AS sanctions,
        FROM d1_sanctions
        GROUP BY 1
    )
    ,
    -- TODO(#25571): Confirm that someone can only have one D1 sanction at a time
    active_d1_sanctions AS (
        SELECT DISTINCT
            person_id,
            FIRST_VALUE(sanction_id) OVER w AS sanction_id,
            FIRST_VALUE(sanction_start_date) OVER w AS sanction_start_date,
            FIRST_VALUE(sanction_expiration_date) OVER w AS sanction_expiration_date,
            FIRST_VALUE(DATE_DIFF(sanction_expiration_date, sanction_start_date, DAY)) OVER w AS sanction_length,
        FROM d1_sanctions
        WHERE 
            CURRENT_DATE('US/Pacific') BETWEEN sanction_start_date AND {nonnull_end_date_exclusive_clause('sanction_expiration_date')}
        -- There are some duplicate sanctions, so this window selects an arbitrary sanction deterministically
        WINDOW w AS (
            PARTITION BY person_id
            ORDER BY sanction_id DESC
        )
    )
    ,
    cdv_all AS (
        SELECT 
            person_id,
            cdv.incident_date AS cdv_date,
            cdv.incident_type_raw_text AS cdv_rule,
            -- Rules are formatted AS [major_number].[minor_number] e.g. 11.5
            -- Major violations are rules 1-9
            -- Further info on CDVs here: 
            -- https://doc.mo.gov/sites/doc/files/media/pdf/2020/03/Offender_Rulebook_REVISED_2019.pdf
            CAST(RTRIM(REGEXP_EXTRACT(cdv.incident_type_raw_text, r'.+\\.'), r'\\.') AS INT64) < 10 AS is_major_violation,
            -- Excludes violations that occur the day of a hearing, which could in reality
            -- occur either before or after the hearing, since hearings are likely to be
            -- scheduled further in advance than on the same day AS the violation.
            cdv.incident_date > h.most_recent_hearing_date AS occurred_since_last_hearing,
        FROM `{{project_id}}.normalized_state.state_incarceration_incident` cdv
        LEFT JOIN most_recent_hearings h
        USING(person_id)
        WHERE cdv.state_code = 'US_MO'
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
            FIRST_VALUE(assessment_score) OVER person_scores AS mental_health_assessment_score,
        FROM `{{project_id}}.analyst_data.us_mo_screeners_preprocessed_materialized` 
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
            c.DOC_ID AS external_id,
            IF(ACTUAL_START_DT = '7799-12-31', NULL, SAFE.PARSE_DATE('%Y-%m-%d', ACTUAL_START_DT)) AS start_date,
            IF(ACTUAL_EXIT_DT = '7799-12-31', NULL, SAFE.PARSE_DATE('%Y-%m-%d', ACTUAL_EXIT_DT)) AS end_date,
            CLASS_TITLE AS class_title,
            CLASS_EXIT_REASON_DESC AS class_exit_reason,
        FROM classes c
        /* EXIT_TYPE_CD and CLASS_EXIT_REASON_DESC together describe the reason someone stopped taking a class.
        Each description (CLASS_EXIT_REASON_DESC) is under the umbrella of a more general EXIT_TYPE_CD category:
        "UNS": Unsuccessful, "SFL": Successful, and "NOF": (Unsuccessful) No-fault exit
        Here we omit Unsuccessful classes so AS only to surface classes that were either completed or were not
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
        FROM `{{project_id}}.us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK194_latest`
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
          enemy.CD_DOC AS external_id, 
          enemy.CD_ENY AS enemy_external_id,
          bed_enemy.bed_number AS enemy_bed_number,
          bed_enemy.room_number AS enemy_room_number,
          bed_enemy.complex_number AS enemy_complex_number,
          bed_enemy.building_number AS enemy_building_number,
          bed_enemy.housing_use_code AS enemy_housing_use_code,
          bed_enemy.facility AS enemy_facility,
        FROM `{{project_id}}.us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK033_latest` enemy
        -- Join on CD_ENY to get person in Restrictive Housing's enemies' bed stays
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei_enemy
        ON
            enemy.CD_ENY = pei_enemy.external_id
            AND pei_enemy.state_code = 'US_MO'
        -- Join on CD_DOC to get person in Restrictive Housing's facility
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei_self
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
    -- Most recent ITSC entry for each person. "Hearing date" (JV_BA) is the date of the entry,
    -- and "Next review date" (JV_AY) is the date of the scheduled "meaningful" hearing.
    itsc_entries AS (
        SELECT DISTINCT
            FIRST_VALUE(pei.person_id) OVER person_window AS person_id,
            FIRST_VALUE(
                IF (
                    tak295.JV_BA = "0", 
                    NULL,
                    SAFE.PARSE_DATE("%Y%m%d", tak295.JV_BA)
                ) 
            ) OVER person_window AS hearing_date,
            FIRST_VALUE(
                IF (
                    tak295.JV_AY = "0", 
                    NULL,
                    SAFE.PARSE_DATE("%Y%m%d", tak295.JV_AY)
                )
            ) OVER person_window AS next_review_date,
            FIRST_VALUE(tak295.JV_FTX) OVER person_window AS comments,
        FROM `{{project_id}}.us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK295_latest` tak295
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
        ON
            tak295.JV_DOC = pei.external_id
            AND pei.state_code = 'US_MO'
        WINDOW person_window AS (
            PARTITION by pei.person_id, pei.state_code
            ORDER BY tak295.JV_BA DESC
        )
    )
    ,
    itsc_entries_during_rh_placement AS (
        SELECT
            person_id,
            hearing_date,
            next_review_date,
            comments
        FROM itsc_entries i
        LEFT JOIN current_confinement_stay c
        USING(person_id)
        WHERE 
            -- Omit most recent hearing if it occurred before the current stay
            {nonnull_start_date_clause('i.hearing_date')} >= c.start_date
    )
    ,
    final AS (
        SELECT
            base.external_id,
            base.person_id,
            base.state_code,
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
            aic_scores_latest.aic_score AS metadata_aic_score,
            TO_JSON(IFNULL(unwaived_enemies_by_person.unwaived_enemies, [])) AS metadata_unwaived_enemies,
            TO_JSON(IFNULL(cdv_d1_sanctions_by_person.sanctions, [])) AS metadata_all_sanctions,
            IFNULL(cdv_d1_sanctions_by_person.num_d1_sanctions_past_year, 0) AS metadata_num_d1_sanctions_past_year,
            TO_JSON(IFNULL(solitary_assignments_by_person.solitary_assignment_dates, [])) AS metadata_solitary_assignment_info_past_year,
            IFNULL(solitary_assignments_by_person.num_solitary_assignments_past_year, 0) AS metadata_num_solitary_assignments_past_year,
            active_d1_sanctions.sanction_start_date AS metadata_active_d1_sanction_start_date,
            active_d1_sanctions.sanction_expiration_date AS metadata_active_d1_sanction_expiration_date,
            active_d1_sanctions.sanction_length AS metadata_active_d1_sanction_length,
            itsc_entries_during_rh_placement.hearing_date AS metadata_itsc_hearing_date,
            itsc_entries_during_rh_placement.next_review_date AS metadata_itsc_next_review_date,
            itsc_entries_during_rh_placement.comments AS metadata_itsc_comments,
        FROM base_query base
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
        LEFT JOIN cdv_d1_sanctions_by_person
        USING (person_id)
        LEFT JOIN solitary_assignments_by_person
        USING (person_id)
        LEFT JOIN active_d1_sanctions
        USING (person_id)
        LEFT JOIN itsc_entries_during_rh_placement
        USING (person_id)
    )
    SELECT * FROM final
"""

US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_NAME,
    view_query_template=US_MO_RESTRICTIVE_HOUSING_RECORD_QUERY_TEMPLATE,
    description=US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_DESCRIPTION,
    should_materialize=True,
    us_mo_cdv_minor_months_to_display=US_MO_CDV_MINOR_NUM_MONTHS_TO_DISPLAY,
    us_mo_cdv_major_months_to_display=US_MO_CDV_MAJOR_NUM_MONTHS_TO_DISPLAY,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_BUILDER.build_and_print()
