#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View of cohorts with PROBATION, RIDER, and TERM sentences."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Only consider recent data, starting from this year
RECENT_DATA_START_YEAR = "2010"

SENTENCE_COHORT_VIEW_NAME = "sentence_cohort"

SENTENCE_COHORT_DESCRIPTION = """
This view combines cohorts of people who were sentenced to PROBATION, RIDER, or TERM.

This allows for recidivism and sentence disposition calculations for the purpose of PSI Case Insights.

The query starts with a transition from liberty to either incarceration or supervision. This transition can either be
directly from LIBERTY or INVESTIGATION, or immediately following a TEMPORARY_CUSTODY session which itself follows
LIBERTY or INVESTIGATION. The person's cohort depends on whether they transitioned to PROBATION (PROBATION cohort),
TREATMENT_IN_PRISON (RIDER cohort), or GENERAL (TERM cohort).

It then finds the subsequent session when they were released to either SUPERVISION or LIBERTY (this is simply
the next session for those sentenced directly to PROBATION), the date of which is the beginning of their recidivism
window.

Once it has all these cohort starts, it joins with sentence_imposed_group_summary_materialized to retrieve attributes of
the offense (description, category, violent/drug/sex offense) which are used for aggregations downstream.
"""

TRANSITION_COHORTS_CTE = """
SELECT
    sess.state_code,
    sess.person_id,
    sess.session_id,
    sess.gender,
    sess.assessment_score_end AS assessment_score_upon_admission,
    CASE
        WHEN sess.outflow_to_level_2 = "PROBATION" THEN "PROBATION"
        WHEN sess.outflow_to_level_2 = "TREATMENT_IN_PRISON" THEN "RIDER"
        WHEN sess.outflow_to_level_2 = "GENERAL" THEN "TERM"
    END AS cohort_group,
    release_sess.start_date AS cohort_start_date,
    sess.end_date_exclusive AS admission_start_date,
    sess.session_id + 1 AS first_session_of_sentence_id,
FROM `{project_id}.sessions.compartment_sessions_materialized` sess
INNER JOIN `{project_id}.sessions.compartment_sessions_materialized` release_sess
    ON release_sess.state_code = sess.state_code
    AND release_sess.person_id = sess.person_id
    AND release_sess.start_date >= sess.end_date_exclusive
    AND release_sess.compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE", "LIBERTY")
WHERE
    sess.outflow_to_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE",
                                "SUPERVISION", "SUPERVISION_OUT_OF_STATE")
    AND sess.outflow_to_level_2 IN ("GENERAL", "TREATMENT_IN_PRISON", "PROBATION")
    AND
    -- Include the following transitions:
    --   Liberty/Investigation -> Incarceration temporary custody -> Supervision/Incarceration (not temporary custody)
    --   Liberty/Investigation -> Supervision/Incarceration (not temporary custody)
        (
            (
                sess.inflow_from_level_1 IN ("LIBERTY", "INVESTIGATION")
                AND sess.compartment_level_2 = "TEMPORARY_CUSTODY"
            )
        OR
            (
                sess.compartment_level_1 IN ("LIBERTY", "INVESTIGATION")
            )
        )
-- Pick the first subsequent supervision/liberty session start for the cohort start date
QUALIFY ROW_NUMBER() OVER (PARTITION BY sess.state_code, sess.person_id, sess.end_date_exclusive ORDER BY release_sess.start_date ASC) = 1
"""

FIRST_TIMER_COHORTS_CTE = """
-- Handle cases where the first session for a person is supervision/incarceration without a prior
-- investigation/temporary custody period
SELECT
    sess.state_code,
    sess.person_id,
    sess.session_id,
    sess.gender,
    sess.assessment_score_start AS assessment_score_upon_admission,
    CASE
        WHEN sess.compartment_level_2 = "PROBATION" THEN "PROBATION"
        WHEN sess.compartment_level_2 = "TREATMENT_IN_PRISON" THEN "RIDER"
        WHEN sess.compartment_level_2 = "GENERAL" THEN "TERM"
    END AS cohort_group,
    release_sess.start_date AS cohort_start_date,
    sess.start_date AS admission_start_date,
    sess.session_id AS first_session_of_sentence_id,
FROM `{project_id}.sessions.compartment_sessions_materialized` sess
INNER JOIN `{project_id}.sessions.compartment_sessions_materialized` release_sess
    ON release_sess.state_code = sess.state_code
    AND release_sess.person_id = sess.person_id
    AND release_sess.start_date >= sess.end_date_exclusive
    AND release_sess.compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE", "LIBERTY")
WHERE
    sess.session_id = 1
    AND sess.compartment_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE",
                                     "SUPERVISION", "SUPERVISION_OUT_OF_STATE")
    AND sess.compartment_level_2 IN ("GENERAL", "TREATMENT_IN_PRISON", "PROBATION")
-- Pick the first subsequent supervision/liberty session start for the cohort start date
QUALIFY ROW_NUMBER() OVER (PARTITION BY sess.state_code, sess.person_id, sess.end_date_exclusive ORDER BY release_sess.start_date ASC) = 1
"""

ALL_COHORTS_CTE = f"""
SELECT * FROM ({TRANSITION_COHORTS_CTE})

UNION ALL

SELECT * FROM ({FIRST_TIMER_COHORTS_CTE})
"""

SENTENCE_COHORT_QUERY_TEMPLATE = f"""
SELECT
  all_cohorts.state_code,
  all_cohorts.person_id,
  all_cohorts.gender,
  all_cohorts.assessment_score_upon_admission as assessment_score,
  all_cohorts.cohort_group,
  all_cohorts.cohort_start_date,
  imposed_summary.most_severe_description,
  imposed_summary.most_severe_ncic_category_uniform,
  imposed_summary.any_is_drug_uniform,
  imposed_summary.any_is_violent_uniform,
  imposed_summary.any_is_sex_offense,
FROM ({ALL_COHORTS_CTE}) all_cohorts
JOIN `{{project_id}}.sessions.compartment_sessions_closest_sentence_imposed_group` closest_imposed_group
  ON all_cohorts.person_id = closest_imposed_group.person_id
  AND all_cohorts.first_session_of_sentence_id = closest_imposed_group.session_id
LEFT JOIN `{{project_id}}.sessions.sentence_imposed_group_summary_materialized` imposed_summary
  ON closest_imposed_group.person_id = imposed_summary.person_id
  AND closest_imposed_group.sentence_imposed_group_id = imposed_summary.sentence_imposed_group_id
WHERE EXTRACT(YEAR FROM cohort_start_date) >= {RECENT_DATA_START_YEAR}
"""

SENTENCE_COHORT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=SENTENCE_COHORT_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCE_COHORT_QUERY_TEMPLATE,
    description=SENTENCE_COHORT_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_COHORT_VIEW_BUILDER.build_and_print()
