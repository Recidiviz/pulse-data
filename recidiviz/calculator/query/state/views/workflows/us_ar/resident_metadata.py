# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Arkansas resident metadata"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AR_RESIDENT_METADATA_VIEW_NAME = "us_ar_resident_metadata"

US_AR_RESIDENT_METADATA_VIEW_DESCRIPTION = """
Arkansas resident metadata
"""


US_AR_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = f"""
    WITH all_residents AS (
        SELECT
            pei.external_id,
            cs.*
        FROM `{{project_id}}.sessions.compartment_sessions_materialized` cs
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
        ON
            cs.person_id = pei.person_id
            AND pei.state_code = 'US_AR'
            AND pei.id_type = 'US_AR_OFFENDERID'
        WHERE cs.state_code = 'US_AR'
        AND cs.compartment_level_1 = 'INCARCERATION'
        AND cs.end_date_exclusive IS NULL
    )
    ,
    current_sentences AS (
        SELECT
            sent.person_id,
            ARRAY_AGG(
                STRUCT(
                    sent.person_id,
                    sent.external_id AS sentence_id,
                    sent.effective_date AS start_date,
                    sent.projected_completion_date_max AS end_date,
                    sent.initial_time_served_days
                ) ORDER BY sent.effective_date DESC
            ) AS current_sentences,
        {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap=["INCARCERATION"])}
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause('span.start_date', 'span.end_date')}
        AND span.state_code = 'US_AR'
        AND sent.sentence_type = 'INCARCERATION'
        GROUP BY 1
    )
    ,
    programs AS (
        SELECT
            OFFENDERID AS external_id,
            PGMACHIVTYPEOFCERT AS program_type,
            PGMACHIVAWARDLOC AS program_location,
            DATE(PGMACHIVCERTAWARDDT) AS program_achievement_date,
            PGMACHIVEVALSCORE AS program_evaluation_score,
        FROM `{{project_id}}.us_ar_raw_data_up_to_date_views.PROGRAMACHIEVEMENT_latest`
    )
    ,
    ged_completion AS (
        SELECT DISTINCT
            external_id,
            FIRST_VALUE(program_achievement_date) OVER person_window AS program_achievement_date  
        FROM programs
        WHERE program_type = 'GED'
        -- There are a handful of people with multiple GED completions, so we take the most recent one
        WINDOW person_window AS (
            PARTITION by external_id
            ORDER BY program_achievement_date DESC
        )
    )
    ,
    program_achievement AS (
        SELECT
            external_id,
            ARRAY_AGG(
                STRUCT(
                    program_type,
                    program_location,
                    program_achievement_date,
                    program_evaluation_score
                ) ORDER BY program_achievement_date DESC
            ) AS program_achievement
        FROM programs
        GROUP BY 1
    )
    ,
    violations_milestones_6_months AS (
        SELECT
            state_code,
            person_id,
            meets_criteria,
            start_date,
            end_date,
            reason,
        FROM `{{project_id}}.task_eligibility_criteria_general.no_incarceration_sanctions_within_6_months_materialized`
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="start_date",
            end_date_column="end_date"
        )}
    )
    ,
    violations_milestones_12_months AS (
        SELECT
            state_code,
            person_id,
            meets_criteria,
            start_date,
            end_date,
            reason,
        FROM `{{project_id}}.task_eligibility_criteria_general.no_incarceration_sanctions_within_12_months_materialized`
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="start_date",
            end_date_column="end_date"
        )}
    )
    select
        ar.person_id,
        ip.CURRENTGTEARNINGCLASS AS current_gt_earning_class,
        ip.CURRCUSTODYCLASSIFICATION AS current_custody_classification,
        css.facility_name AS current_location,
        TO_JSON(IFNULL(current_sentences.current_sentences, [])) AS current_sentences,
        DATE(ip.PAROLEELIGIBILITYDATE) AS parole_eligibility_date,
        DATE(ip.PROJRELEASEDT) AS projected_release_date,
        DATE(ip.MAXFLATRELEASEDATE) AS max_flat_release_date,
        TO_JSON(IFNULL(pa.program_achievement, [])) AS program_achievement,
        DATE(gc.program_achievement_date) AS ged_completion_date,
        COALESCE(vm6.meets_criteria, TRUE) AS no_incarceration_sanctions_within_6_months,
        COALESCE(vm12.meets_criteria, TRUE) AS no_incarceration_sanctions_within_12_months,
    FROM all_residents ar
    LEFT JOIN `{{project_id}}.us_ar_raw_data_up_to_date_views.INMATEPROFILE_latest` ip
    ON
        ar.external_id = ip.OFFENDERID
    LEFT JOIN program_achievement pa
    ON
        ar.external_id = pa.external_id
    LEFT JOIN ged_completion gc
    ON
        ar.external_id = gc.external_id
    LEFT JOIN current_sentences
    USING(person_id)
    LEFT JOIN violations_milestones_6_months vm6
    USING(person_id, state_code)
    LEFT JOIN violations_milestones_12_months vm12
    USING(person_id, state_code)
    LEFT JOIN `{{project_id}}.sessions.compartment_sub_sessions_materialized` css
    ON
        ar.person_id = css.person_id
        AND ar.state_code = css.state_code
        AND css.end_date_exclusive IS NULL
    ORDER BY ip.OFFENDERID

"""

US_AR_RESIDENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_AR_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_AR_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_AR_RESIDENT_METADATA_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_RESIDENT_METADATA_VIEW_BUILDER.build_and_print()
