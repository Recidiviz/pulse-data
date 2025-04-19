# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View of cohorts with various sentences.

This view combines cohorts of people who were sentenced to either Probation, or to Incarceration with different sentence
lengths.

This allows for recidivism and sentence disposition calculations for the purpose of PSI Case Insights.

The query starts by grouping all sentences tied to the same court ID (which is parsed out from an external id) and
keeping only the sentence for that court ID with the earliest imposed date. This makes sure that we're only considering
initial sentences, and not later amendments, revocations, etc.

After pulling information about individual charges and sentence lengths, it then uses state_sentence_imposed_group
(which is all sentences imposed on a particular date) as the primary source of possible cohort starts, and joins with
the above court ID groups. This means that a cohort start is tied to a sentence imposed group that includes the earliest
instance of a court case, which means it's the initial sentence. The most severe charge from that sentence imposed group
is then associated with the cohort start.

Finally, the sentence group is associated with sessions. The "admission session" is the earliest session whose end_date
is later than the imposed_date; for probation, this should be the session when probation starts and for incarceration
this should be the session when incarceration starts. The "release session" is the session when they are back in the
community; for probation, this is the same as the "admission session" and for incarceration, this is the next session
when they're out on supervision or at liberty.
"""


# pylint: disable=anomalous-backslash-in-string
def parse_court_case_id_part(
    sentence_group_external_id_column: str = "sentence_group_external_id",
    dash_offset: int = 0,
) -> str:
    """Extract part of the court case id from the sentence_group_external_id.

    The sentence_group_external_id typically looks like one of the following:
    * 09-2014-CR-03873|106408
    * 09-2014-CR-03873-1|106408
    * 09-2014-CR-03873 (CT 1,2)|106408
    * 09-2014-CR-03873 (CT 1/2)|106408
    The court case id we need to extract in this example is 03873, which is either the last or 2nd-to-last
    dash-delimited element of the string before the pipe, after removing parentheses, commas, and spaces.

    This method does the following:
    * Split by "|" and take the first element
    * Split by "-", reverse, and take the dash_offset'th element (dash_offset=0 means taking the last dash-delimited
    element; dash_offset=1 means taking the 2nd-to-last)
    * Remove parentheses, spaces, commas, and slashes.
    """
    return f"""
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REPLACE(
          REGEXP_REPLACE(
            IFNULL(
              -- court case id is the last element of the sentence group external id
              ARRAY_REVERSE(SPLIT(
                SPLIT({sentence_group_external_id_column}, "|")[OFFSET(0)],
                "-"
              ))[SAFE_OFFSET({dash_offset})],
              "UNKNOWN"
            ),
          "\\\([^()]*\\\)", ""), # remove parentheses
        " ", ""), # remove spaces
      ",.*", ""), # remove commas and everything after
    "/.*", "") # remove slashes and everything after
    """


def extract_court_case_id_part(
    sentence_group_external_id_column: str = "sentence_group_external_id",
) -> str:
    """If the last dash-separated field is a single digit, choose the second-to-last field instead."""
    return f"""
    IF(
      LENGTH({parse_court_case_id_part(sentence_group_external_id_column, 0)}) = 1,
      LTRIM({parse_court_case_id_part(sentence_group_external_id_column, 1)}, "0"),
      LTRIM({parse_court_case_id_part(sentence_group_external_id_column, 0)}, "0")
    )
    """


US_ND_SENTENCE_IMPOSED_GROUP_SUMMARY = f"""
WITH court_cases AS (
SELECT
  state_code,
  {extract_court_case_id_part()} AS court_case_id_part,
  sentence_imposed_group_id,
  person_id,
FROM `{{project_id}}.normalized_state.state_sentence`
WHERE state_code = 'US_ND'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id, court_case_id_part
    ORDER BY imposed_date ASC
    ) = 1
),

-- Unifies charge and offense information needed for this view's output
-- TODO(#36539) Update this view when normalized_state.state_charge_v2 has uniform fields
uniform_charges AS (
    SELECT
        charge.state_code,
        charge.charge_v2_id,
        charge.external_id,
        charge.classification_type,
        charge.classification_subtype,
        charge.description,
        charge.is_sex_offense,
        charge.is_violent,
        charge.is_drug,
        charge.ncic_category_external,
        clean_offense.ncic_category_uniform,
        clean_offense.is_violent_uniform,
        clean_offense.is_drug_uniform,
        clean_offense.ncic_code_uniform,
        SPLIT(charge.external_id,'-')[SAFE_OFFSET(0)] AS offender_book_id_type1,
        SPLIT(charge.external_id,'|')[SAFE_OFFSET(2)] AS offender_book_id_type2,
    FROM
        `{{project_id}}.normalized_state.state_charge_v2` AS charge
    LEFT JOIN
        `{{project_id}}.reference_views.cleaned_offense_description_to_labels` AS clean_offense
    ON
        charge.description = clean_offense.offense_description
    WHERE IFNULL(classification_type, "UNKNOWN") = "FELONY"
),
-- Gathers completion dates to be used in imposed group aggregation
initial_sentence_lengths AS (
    SELECT
        sentence_id,
        sentence_length_days_min,
        sentence_length_days_max,
        projected_completion_date_min_external,
        projected_completion_date_max_external,
    FROM
        `{{project_id}}.normalized_state.state_sentence_length`
    WHERE
        sequence_num = 1
),
-- Aggregates offense and sentence characteristics by imposed group
aggregated_sentence_charge_data AS (
    SELECT
        imposed_groups.person_id,
        imposed_groups.sentence_imposed_group_id,
        imposed_groups.most_severe_charge_v2_id,
        imposed_groups.sentencing_authority,
        imposed_groups.imposed_date,
        imposed_groups.serving_start_date,
        court_cases.court_case_id_part,
        LOGICAL_OR(all_charges.is_sex_offense) AS any_is_sex_offense,
        LOGICAL_OR(all_charges.is_violent_uniform) AS any_is_violent,
        LOGICAL_OR(all_charges.is_drug_uniform) AS any_is_drug,
        LOGICAL_OR(sentences.is_life) AS any_is_life,
        CASE
            WHEN LOGICAL_OR(sentences.sentence_type = "STATE_PRISON") THEN "INCARCERATION"
            WHEN LOGICAL_OR(sentences.sentence_type = "PROBATION") THEN "PROBATION"
            ELSE "OTHER_SENTENCE_TYPE"
        END AS sentence_type,
        MAX(initial_sentence_lengths.sentence_length_days_min) AS sentence_length_days_min,
        MAX(initial_sentence_lengths.sentence_length_days_max) AS sentence_length_days_max,
        MAX(initial_sentence_lengths.projected_completion_date_min_external) AS projected_completion_date_min,
        MAX(initial_sentence_lengths.projected_completion_date_max_external) AS projected_completion_date_max,
    FROM
        `{{project_id}}.normalized_state.state_sentence_imposed_group` AS imposed_groups
    JOIN
        `{{project_id}}.normalized_state.state_sentence` AS sentences
    USING
        (sentence_imposed_group_id)
    JOIN
        `{{project_id}}.normalized_state.state_charge_v2_state_sentence_association` AS charge_to_sentence
    USING
        (sentence_id)
    JOIN
        uniform_charges AS all_charges
    USING
        (charge_v2_id)
    INNER JOIN
        court_cases
    ON
        court_cases.state_code = imposed_groups.state_code
        AND court_cases.person_id = imposed_groups.person_id
        AND court_cases.sentence_imposed_group_id = imposed_groups.sentence_imposed_group_id
        AND court_cases.court_case_id_part = {extract_court_case_id_part("sentences.sentence_group_external_id")}
    LEFT JOIN
        initial_sentence_lengths
    ON
        sentences.sentence_id = initial_sentence_lengths.sentence_id
    WHERE EXTRACT(YEAR FROM imposed_groups.imposed_date) >= 2010
      AND sentences.sentence_type in ("STATE_PRISON", "PROBATION")
      AND court_cases.court_case_id_part != "UNKNOWN"
    GROUP BY
        person_id, sentence_imposed_group_id, most_severe_charge_v2_id, sentencing_authority, serving_start_date, imposed_date,
        court_case_id_part
)
SELECT
    most_severe_charge.state_code,
    person.gender,
    imposed_group.person_id,
    imposed_group.sentence_imposed_group_id,
    imposed_group.most_severe_charge_v2_id,
    imposed_group.sentencing_authority,
    imposed_group.imposed_date,
    imposed_group.serving_start_date,
    imposed_group.sentence_type,
    imposed_group.any_is_sex_offense,
    imposed_group.any_is_violent,
    imposed_group.any_is_drug,
    imposed_group.any_is_life,
    imposed_group.projected_completion_date_min,
    imposed_group.projected_completion_date_max,
    imposed_group.sentence_length_days_min,
    imposed_group.sentence_length_days_max,
    DATE_DIFF(
        imposed_group.projected_completion_date_min,
        imposed_group.serving_start_date,
        DAY
    ) AS sentence_length_days_min_from_projected_date,
    DATE_DIFF(
        imposed_group.projected_completion_date_max,
        imposed_group.serving_start_date,
        DAY
    ) AS sentence_length_days_max_from_projected_date,
    most_severe_charge.external_id AS most_severe_charge_external_id,
    most_severe_charge.classification_type AS most_severe_charge_classification_type,
    most_severe_charge.classification_subtype AS most_severe_charge_classification_subtype,
    most_severe_charge.description AS most_severe_charge_description,
    most_severe_charge.is_sex_offense AS most_severe_charge_is_sex_offense,
    most_severe_charge.is_violent AS most_severe_charge_is_violent,
    most_severe_charge.is_drug AS most_severe_charge_is_drug,
    most_severe_charge.ncic_category_external AS most_severe_charge_ncic_category_external,
    most_severe_charge.ncic_category_uniform AS most_severe_charge_ncic_category_uniform,
    most_severe_charge.is_violent_uniform AS most_severe_charge_is_violent_uniform,
    most_severe_charge.is_drug_uniform AS most_severe_charge_is_drug_uniform,
    most_severe_charge.ncic_code_uniform AS most_severe_charge_ncic_code_uniform,
    most_severe_charge.offender_book_id_type1,
    most_severe_charge.offender_book_id_type2,
    most_severe_charge.description AS most_severe_description,
FROM
    aggregated_sentence_charge_data AS imposed_group
INNER JOIN uniform_charges AS most_severe_charge
  ON imposed_group.most_severe_charge_v2_id = most_severe_charge.charge_v2_id
INNER JOIN `{{project_id}}.us_nd_normalized_state.state_person` person
   ON person.person_id = imposed_group.person_id
"""

ALL_COHORTS_CTE = f"""
WITH admission_session AS (
  SELECT
    sigs.*,
    sess.session_id,
    compartment_level_1,
    compartment_level_2,
    sess.start_date,
    sess.assessment_score_end,
    sess.start_reason,
    sess.start_sub_reason,
  FROM ({US_ND_SENTENCE_IMPOSED_GROUP_SUMMARY}) sigs
  INNER JOIN `{{project_id}}.sessions.compartment_sessions_materialized` sess
    ON sess.person_id = sigs.person_id
    AND (
        CASE
            WHEN sentence_type = "PROBATION" THEN sess.compartment_level_1 = "SUPERVISION" AND sigs.imposed_date < IFNULL(sess.end_date, "9999-12-31")
            WHEN sentence_type = "INCARCERATION" THEN sess.compartment_level_1 = "INCARCERATION" AND sigs.imposed_date < IFNULL(sess.end_date, "9999-12-31")
            ELSE sigs.imposed_date BETWEEN sess.start_date AND IFNULL(sess.end_date, "9999-12-31")
        END
    )
    # Take the earliest session whose end_date is later than the imposed_date
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sess.state_code, sess.person_id, sigs.sentence_imposed_group_id
    ORDER BY
        sess.start_date ASC
    ) = 1
),
sentence_admission_release AS (
SELECT
    admission_sess.*,
    release_sess.assessment_score_start AS assessment_score,
    CASE
      WHEN release_sess.assessment_score_start >= 0 AND release_sess.assessment_score_start <= 19 THEN "0-19"
      WHEN release_sess.assessment_score_start >= 20 AND release_sess.assessment_score_start <= 23 THEN "20-23"
      WHEN release_sess.assessment_score_start >= 24 AND release_sess.assessment_score_start <= 29 THEN "24-29"
      WHEN release_sess.assessment_score_start >= 30 AND release_sess.assessment_score_start <= 38 THEN "30-38"
      WHEN release_sess.assessment_score_start >= 39 AND release_sess.assessment_score_start <= 46 THEN "39-46"
      WHEN release_sess.assessment_score_start >= 47 THEN "47+"
      ELSE "OTHER"
    END AS assessment_score_bucket,
    IF(
        sentence_type = "INCARCERATION",
        CASE
          WHEN sentence_length_days_max < 365 THEN "INCARCERATION|0-1 years"
          WHEN sentence_length_days_max >= 365 AND sentence_length_days_max < 365*3 THEN "INCARCERATION|1-2 years"
          WHEN sentence_length_days_max >= 365*3 AND sentence_length_days_max < 365*6 THEN "INCARCERATION|3-5 years"
          WHEN sentence_length_days_max >= 365*6 THEN "INCARCERATION|6-? years"
        END,
        sentence_type
        ) AS cohort_group,
    admission_sess.start_date AS admission_sess_start_date,
    release_sess.start_date AS release_sess_start_date,
    IF (
      admission_sess.compartment_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE"),
      # If they're incarcerated when the sentence is imposed, then cohort start is when they're released
      release_sess.start_date,
      # Otherwise, cohort start is when the sentence is imposed
      imposed_date
    ) AS cohort_start_date
FROM admission_session admission_sess
INNER JOIN `{{project_id}}.sessions.compartment_sessions_materialized` release_sess
  USING (state_code, person_id)
WHERE
  release_sess.start_date >= admission_sess.start_date
  AND release_sess.compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE", "LIBERTY")
-- Admission session is the next session on or after the sentence imposed_date
-- Release session is the earliest SUPERVISION/LIBERTY session on or after admission
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY admission_sess.state_code, admission_sess.person_id, admission_sess.sentence_imposed_group_id
    ORDER BY
        release_sess.start_date ASC
    ) = 1
)
-- sentence_admission_release is unique per sentence_imposed_group_id, but can have multiple such ids per cohort_start_date
-- Now make this also unique per cohort_start_date by taking only the earliest sentence per cohort_start_date
SELECT *
FROM sentence_admission_release
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, cohort_start_date
    ORDER BY imposed_date ASC
    ) = 1
"""

US_ND_SENTENCING_SENTENCE_COHORT_TEMPLATE = f"""

SELECT
  all_cohorts.state_code,
  all_cohorts.person_id,
  all_cohorts.gender,
  all_cohorts.assessment_score,
  cohort_group,
  cohort_start_date,
  most_severe_charge_description AS most_severe_description,
  most_severe_charge_ncic_category_uniform AS most_severe_ncic_category_uniform,
  most_severe_charge_ncic_category_external AS most_severe_ncic_category_external,
  # US_ND uses state-specific values for violent and drug offenses
  most_severe_charge_is_violent AS any_is_violent,
  most_severe_charge_is_drug AS any_is_drug,
  most_severe_charge_is_sex_offense AS any_is_sex_offense
FROM ({ALL_COHORTS_CTE}) all_cohorts
WHERE all_cohorts.state_code = 'US_ND'
"""
