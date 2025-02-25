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
"""
Creates a view to gather data needed to calculate PSA-like risk scores for all
supervised clients.

More about PSA: https://advancingpretrial.org/psa/about
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PSA_RISK_SCORES_VIEW_NAME = "psa_risk_scores"

PSA_RISK_SCORES_VIEW_DESCRIPTION = (
    "Creates a view to gather data needed to calculate PSA risk scores for all "
    "supervised clients.\n\n"
    "The Public Safety Assessment (PSA) is an actuarial assessment that predicts "
    "failure to appear in court pretrial, new criminal arrest while on pretrial "
    "release, and new violent criminal arrest while on pretrial release. Use of the "
    "PSA, in combination with other pretrial improvements, is associated with improved "
    "outcomes. These include higher rates of pretrial release and less use of financial"
    " conditions of release. These outcomes do not negatively impact crime or court "
    "appearance rates.\n\n"
    "More about PSA: https://advancingpretrial.org/psa/about"
)

PSA_RISK_SCORES_QUERY_TEMPLATE = """

-- renaming compartment_sessions data for convenience
WITH compartment_sessions AS (
    SELECT
        *
    FROM
       `{project_id}.sessions.compartment_sessions_materialized`
)

, supervision_starts AS (
    SELECT DISTINCT
        state_code,
        person_id,
        session_id,
        start_date,
        EXTRACT(YEAR FROM start_date) AS start_year,
        age_start,

        -- safe to assume that it would be unusual to have pending charges if on supervision
        -- regardless, no access to pending charge information
        0 AS pending_charges,

    FROM
        compartment_sessions
    WHERE
        compartment_level_1 = "SUPERVISION"

        -- updated after the introduction of COMMUNITY_CONFINEMENT
        -- in PA that increased the observed -- of supervision starts.
        -- only grab supervision supersession starts
        AND inflow_from_level_1 != "SUPERVISION"
)

, reincarceration_events AS (
    SELECT
        state_code,
        person_id,
        session_id,
        start_date,
        1 AS reincarcerated,
    FROM
        compartment_sessions
    WHERE
        compartment_level_1 = "INCARCERATION"
)

, supervision_starts_w_reincarcerations AS (
    SELECT
        a.state_code,
        a.person_id,
        a.session_id,
        ANY_VALUE(a.start_date) AS start_date,
        ANY_VALUE(a.start_year) AS start_year,
        ANY_VALUE(a.age_start) AS age_start,
        ANY_VALUE(a.pending_charges) AS pending_charges,
        SUM(IFNULL(b.reincarcerated, 0)) > 0 AS
            reincarcerated_within_one_year_of_supervision_start,
    FROM
        supervision_starts a
    LEFT JOIN
        reincarceration_events b
    ON
        a.state_code = b.state_code
        AND a.person_id = b.person_id
        AND b.start_date BETWEEN a.start_date AND DATE_ADD(a.start_date, INTERVAL 1 YEAR)
    GROUP BY
        1, 2, 3
)

------------------------------------------
-- Handling Offenses --
------------------------------------------

/*
Relevant PSA components:
- current violent charge
- prior misdemeanor convictions
- prior felony convictions
- prior violent convictions
- prior incarcerations

Strategy: for each supervision session:
1. Identify whether the most recent sentence included a violent charge.
    a. We use the "*_uniform" fields in `sentences_preprocessed` to keep the score
        state agnostic (different states can have different criteria for crime categories)
2. To calculate priors, identify all sentences that were imposed on or prior to the
    start date of the session

*/

, charge_cte AS (
    SELECT
        a.state_code,
        a.person_id,
        a.session_id,
        b.sentence_id,
        b.charge_id,
        b.date_imposed,
        b.completion_date,
        a.start_date < IFNULL(b.completion_date, "9999-01-01") AS is_current_charge,
        b.sentence_type,
        IFNULL(b.is_violent_uniform, FALSE) AS is_violent_uniform,
        b.is_violent_uniform IS NULL AS is_missing_violent,
        IFNULL(classification_type = "FELONY", FALSE) AS is_felony,
        IFNULL(classification_type = "MISDEMEANOR", FALSE) AS is_misdemeanor,
        (classification_type IN ("INTERNAL_UNKNOWN", "EXTERNAL_UNKNOWN")
            OR classification_type IS NULL) AS is_missing_classification,
        sentence_type = "INCARCERATION" AS is_incarceration_sentence,
    FROM
        supervision_starts a
    -- if someone does not have priors, the left join will still produce a row of NULLs here
    -- if sentence ID is NULL, make sure not to count the NULL priors
    LEFT JOIN
        `{project_id}.sessions.sentences_preprocessed_materialized` b
    ON
        a.state_code = b.state_code
        AND a.person_id = b.person_id

        -- only look 10 years prior to supervision start
        -- otherwise older supervision starts less likely to have available criminal history
        -- than more recent starts

        AND IFNULL(b.date_imposed, b.effective_date) BETWEEN
            DATE_SUB(a.start_date, INTERVAL 10 YEAR) AND a.start_date
)

-- use the most recent sentence to determine whether the "current" charge is violent

, current_violent_cte AS (
    SELECT
        state_code,
        person_id,
        session_id,

        -- if someone does not have priors, the
        COUNTIF(is_violent_uniform AND sentence_id IS NOT NULL) AS current_violent,

        -- assume uncategorized misdemeanor priors are non-violent
        -- this is a count of non-misdemeanor priors without a violence classification
        COUNTIF(is_missing_violent AND NOT is_misdemeanor AND sentence_id IS NOT NULL)
            AS current_missing_violent,
    FROM
        charge_cte
    WHERE
        is_current_charge
    GROUP BY
        1, 2, 3
)

, prior_charges_cte AS (
    SELECT
        state_code,
        person_id,
        session_id,
        COUNTIF(is_misdemeanor AND sentence_id IS NOT NULL) AS prior_misdemeanors,
        COUNTIF(is_felony AND sentence_id IS NOT NULL) AS prior_felonies,
        COUNTIF(is_missing_classification AND sentence_id IS NOT NULL) AS prior_missing_classification,
        COUNTIF(is_violent_uniform AND sentence_id IS NOT NULL) AS prior_violent,
        COUNTIF(is_missing_violent AND NOT is_misdemeanor AND sentence_id IS NOT NULL) AS prior_missing_violent,
        COUNTIF(is_incarceration_sentence AND sentence_id IS NOT NULL) AS prior_sentences_to_incarceration,
    FROM
        charge_cte
    GROUP BY
        1, 2, 3
)

----------------------------------
-- Handling absconsions --
----------------------------------

/*
Relevant PSA components:
- number of absconsions within two years
- number of absconsions older than two years

Strategy:
- For each supervision session, identify any absconsions/bench warrants
on or prior to the start date of the session

*/

-- grab the universe of absconsions

, absconsions_cte AS (
    SELECT DISTINCT
        state_code,
        person_id,
        session_id,
        start_date,
    FROM
        compartment_sessions
    WHERE
        compartment_level_1 = "SUPERVISION"
        AND compartment_level_2 IN ("ABSCONSION", "BENCH_WARRANT")
)

-- grab absconsions within two years and older than two years before the current supervision start

, prior_absconsions_cte AS (
    SELECT
        a.state_code,
        a.person_id,
        a.session_id,
        IFNULL(COUNTIF(DATE_DIFF(a.start_date, b.start_date, DAY) / 365.25 < 2), 0)
            AS prior_absconsions_within_two_years,

        -- only look for absconsions that occurred within the past 10 years
        IFNULL(COUNTIF(
            DATE_DIFF(a.start_date, b.start_date, DAY) / 365.25 BETWEEN 2 AND 10
        ), 0) AS prior_absconsions_older_than_two_years,
    FROM
        supervision_starts a
    -- using a left join here so clients without absconsions receive a zero count
    LEFT JOIN
        absconsions_cte b
    USING
        (state_code, person_id)
    GROUP BY
        1, 2, 3
)

------------------------------------------------
-- Calculate PSA scores --
------------------------------------------------

, psa_item_scores_cte AS (
    SELECT
        *,

        ----------------------------------------------
        -- PSA FTA (absconsion) calculation --
        ----------------------------------------------

        -- the pending charge subscore is always 0 until we decide on a better correlate
        -- of pending charges
        CASE
            WHEN pending_charges > 0
                THEN 1
            WHEN pending_charges = 0
                THEN 0
            ELSE NULL
        END AS psa_fta_pending_charges_score,

        IF(
            prior_felonies > 0 OR prior_misdemeanors > 0 OR
            prior_missing_classification > 0, 1, 0
        ) AS psa_fta_prior_charge_score,

        CASE
            WHEN prior_absconsions_within_two_years >= 2
                THEN 4
            WHEN prior_absconsions_within_two_years = 1
                THEN 2
            ELSE 0
        END AS psa_fta_prior_absconsions_within_two_years_score,

        IF(prior_absconsions_older_than_two_years > 0, 1, 0)
            AS psa_fta_prior_absconsions_older_than_two_years_score,

        ----------------------------------------------
        -- PSA NCA calculation --
        ----------------------------------------------

        CASE
            WHEN age_start <= 22
                THEN 2
            WHEN age_start > 22
                THEN 0
            ELSE NULL
        END AS psa_nca_age_score,

        CASE
            WHEN pending_charges > 0
                THEN 3
            WHEN pending_charges = 0
                THEN 0
            ELSE NULL
        END AS psa_nca_pending_charges_score,

        -- assume sentences with missing classification are felonies
        IF((prior_felonies + prior_missing_classification) > 0, 1, 0)
            AS psa_nca_prior_felonies_score,

        -- replacing misdemeanor score with two or more charges score since misdemeanors not common
        IF(
            (prior_felonies + prior_misdemeanors + prior_missing_classification) > 1,
            1, 0
        ) AS psa_nca_two_or_more_priors_score,

        CASE
            WHEN prior_violent >= 3
                THEN 2
            WHEN prior_violent in (1, 2)
                THEN 1
            ELSE 0
        END as psa_nca_prior_violent_score,

        CASE
            WHEN prior_absconsions_within_two_years >= 2
                THEN 2
            WHEN prior_absconsions_within_two_years = 1
                THEN 1
            ELSE 0
        END AS psa_nca_prior_absconsions_within_two_years_score,

        IF(prior_sentences_to_incarceration > 0, 2, 0)
            AS psa_nca_prior_sentence_score,

        ------------------------------------------------
        -- PSA NVCA calculation --
        ------------------------------------------------

        IF(IFNULL(current_violent, 0) > 0, 2, 0)
            AS psa_nvca_current_violent_score,

        IF(IFNULL(current_violent, 0) > 0 AND age_start <= 20, 1, 0)
            AS psa_nvca_young_and_current_violent_score,

        CASE
            WHEN pending_charges > 0
                THEN 1
            WHEN pending_charges = 0
                THEN 0
            ELSE NULL
        END AS psa_nvca_pending_charge_score,

        IF(
            (prior_felonies + prior_misdemeanors + prior_missing_classification) > 0,
            1, 0
        ) AS psa_nvca_prior_charge_score,

        CASE
            WHEN prior_violent >= 3
                THEN 2
            WHEN prior_violent in (1, 2)
                THEN 1
            ELSE 0
        END as psa_nvca_prior_violent_score,

    FROM
        supervision_starts_w_reincarcerations

    INNER JOIN
        current_violent_cte
    USING
        (state_code, person_id, session_id)

    -- client must have a prior charge (since current charge is a subset of prior
    -- charges in the supervision setting)
    INNER JOIN
        prior_charges_cte
    USING
        (state_code, person_id, session_id)

    INNER JOIN
        prior_absconsions_cte
    USING
        (state_code, person_id, session_id)
)

, psa_raw_scores_cte AS (

    SELECT
        * EXCEPT (current_violent, current_missing_violent),
        IFNULL(current_violent, 0) AS current_violent,
        IFNULL(current_missing_violent, 0) AS current_missing_violent,

        /*
        (
            psa_fta_pending_charges_score
            + psa_fta_prior_charge_score
            + psa_fta_prior_absconsions_within_two_years_score
            + psa_fta_prior_absconsions_older_than_two_years_score
        ) AS total_fta_points,
        */

        (
            psa_nca_age_score
            + psa_nca_pending_charges_score
            + psa_nca_prior_felonies_score

            -- replacing misdemeanor score with two or more felonies score
            -- + psa_nca_prior_misdemeanor_score

            + psa_nca_two_or_more_priors_score
            + psa_nca_prior_violent_score
            + psa_nca_prior_absconsions_within_two_years_score
            + psa_nca_prior_sentence_score
        ) AS total_nca_points,

        /*
        (
            psa_nvca_current_violent_score
            + psa_nvca_young_and_current_violent_score
            + psa_nvca_pending_charge_score
            + psa_nvca_prior_charge_score
            + psa_nvca_prior_violent_score
        ) AS total_nvca_points,
        */

    FROM
        psa_item_scores_cte
)

, scaled_by_state_year_reincarceration_rate_cte AS (
    SELECT
        *,
        AVG(CAST(reincarcerated_within_one_year_of_supervision_start AS INT64)) OVER
            (PARTITION BY state_code, start_year, total_nca_points)
            AS prob_reincarceration_within_one_year_for_state_year,
    FROM
        psa_raw_scores_cte
)

SELECT
    *,
FROM
    scaled_by_state_year_reincarceration_rate_cte

"""

PSA_RISK_SCORES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PSA_RISK_SCORES_VIEW_NAME,
    description=PSA_RISK_SCORES_VIEW_DESCRIPTION,
    view_query_template=PSA_RISK_SCORES_QUERY_TEMPLATE,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PSA_RISK_SCORES_VIEW_BUILDER.build_and_print()
