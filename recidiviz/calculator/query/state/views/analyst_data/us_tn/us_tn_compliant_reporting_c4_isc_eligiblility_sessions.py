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
"""Creates a view that surfaces people eligible for Compliant Reporting"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_NAME = (
    "us_tn_compliant_reporting_c4_isc_eligibility_sessions"
)

US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_DESCRIPTION = """Creates a view that surfaces the time people are ineligible for compliant reporting based on the C4 ISC criteria"""

US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_QUERY_TEMPLATE = """
    WITH isc_cohort AS (
        -- Pull out all ISC supervision periods
        SELECT
            state_code,
            person_id,
            OffenderID AS external_id,
            SAFE_CAST(SentenceImposedDate AS DATETIME) AS start_date,
            -- Use the `ExpirationDate` if it has passed the `ActualReleaseDate` isn't set, otherwise assume it is open
            CASE WHEN SAFE_CAST(ExpirationDate AS DATETIME) < CURRENT_DATE AND ActualReleaseDate IS NULL
                THEN SAFE_CAST(ExpirationDate AS DATETIME)
                ELSE SAFE_CAST(ActualReleaseDate AS DATETIME)
            END AS end_date,
            (Sentence NOT LIKE "%LIFE%") AS isc_sentence_eligible,
        FROM `{project_id}.{raw_dataset}.ISCSentence_latest` isc
        LEFT JOIN `{project_id}.{base_state_dataset}.state_person_external_id` pei
            ON isc.OffenderID = pei.external_id
            AND pei.state_code = "US_TN"
        -- Limit to 1 row per person/sentence group
        -- Pick the max ActualReleaseDate for each unique SentenceImposedDate, and the min
        -- SentenceImposedDate for each unique `end_date` (coalesced between ExpirationDate and ActualReleaseDate)
        QUALIFY
            ROW_NUMBER() OVER
                (PARTITION BY OffenderID, SentenceImposedDate ORDER BY COALESCE(ActualReleaseDate, '9999-12-31') DESC) = 1
            AND ROW_NUMBER() OVER (PARTITION BY OffenderID, end_date ORDER BY SentenceImposedDate ASC) = 1
    ),
    isc_supervision_level AS (
        -- Break up the ISC supervision period by supervision level
        SELECT
            isc_cohort.state_code,
            isc_cohort.person_id,
            isc_cohort.external_id,
            isc_cohort.isc_sentence_eligible,
            sup_level.supervision_level,
            (sup_level.supervision_level LIKE "%MINIMUM%" AND sup_level.previous_supervision_level = 'UNASSIGNED') AS minimum_supervision_flag,
            GREATEST(isc_cohort.start_date, sup_level.start_date) AS start_date,
            CASE WHEN isc_cohort.end_date IS NOT NULL OR sup_level.end_date IS NOT NULL
                THEN LEAST(COALESCE(isc_cohort.end_date, CURRENT_DATE), COALESCE(sup_level.end_date, CURRENT_DATE))
                ELSE NULL
            END AS end_date
        FROM isc_cohort
        JOIN `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` sup_level
            ON sup_level.state_code = isc_cohort.state_code
            AND sup_level.person_id = isc_cohort.person_id
            AND sup_level.start_date < COALESCE(isc_cohort.end_date, CURRENT_DATE)
            AND isc_cohort.start_date < COALESCE(sup_level.end_date, CURRENT_DATE)
    ), eligibility_sessions AS (
        SELECT
            isc_supervision_level.state_code,
            isc_supervision_level.person_id,
            isc_supervision_level.external_id AS tdoc_id,
            CASE WHEN in_state_sentence.date_imposed IS NOT NULL
                THEN GREATEST(isc_supervision_level.start_date, in_state_sentence.date_imposed)
                ELSE isc_supervision_level.start_date
            END AS start_date,
            CASE WHEN isc_supervision_level.end_date IS NOT NULL OR in_state_sentence.completion_date IS NOT NULL
                THEN LEAST(COALESCE(isc_supervision_level.end_date, CURRENT_DATE), COALESCE(in_state_sentence.completion_date, CURRENT_DATE))
                ELSE NULL
            END AS end_date,
            isc_sentence_eligible,
            minimum_supervision_flag,
            (in_state_sentence.person_id IS NOT NULL) AS in_state_sentence_flag
        FROM isc_supervision_level
        -- Known issue: need to determine when the TN sentence ends and the eligibility can start
        LEFT JOIN `{project_id}.{base_state_dataset}.state_supervision_sentence` in_state_sentence
            ON isc_supervision_level.state_code = in_state_sentence.state_code
            AND isc_supervision_level.person_id = in_state_sentence.person_id
            AND isc_supervision_level.start_date < COALESCE(in_state_sentence.completion_date, CURRENT_DATE)
            AND in_state_sentence.date_imposed < COALESCE(isc_supervision_level.end_date, CURRENT_DATE)
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        -- Aggregate all the fields into 1 flag variable
        (isc_sentence_eligible AND minimum_supervision_flag AND NOT in_state_sentence_flag) AS c4_isc_eligibility_flag,
    FROM eligibility_sessions
"""

US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_QUERY_TEMPLATE,
    base_state_dataset=STATE_BASE_DATASET,
    raw_dataset=US_TN_RAW_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_BUILDER.build_and_print()
