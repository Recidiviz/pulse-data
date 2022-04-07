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
"""North Dakota state-specific preprocessing for early discharge sessions.
   In ND we look for cases ending with a "positive termination" reason which is
   OFTEN an early discharge, but not always. This is a known issue within their
   system, and means we're certainly overcounting EDs. Some text analysis we've
   done suggests about 75-80% of the sentences ending with a positive
   termination are in fact early discharges, but its worth noting that its not a
   perfect mapping."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_nd_early_discharge_sessions_preprocessing"
)

US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = (
    """North Dakota state-specific preprocessing for early discharge sessions"""
)

US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = """
    WITH us_nd_ed_deferred_sentence AS (
        SELECT
            * EXCEPT(projected_completion_date),
            'DEFERRED' AS sentence_type
        FROM (
            SELECT
                offendercases.*,
                EXTRACT(date FROM PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S%p', term_date)) AS term_date_clean,
                projected_completion_date
            FROM `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offendercasestable_latest` offendercases
            LEFT JOIN `{project_id}.{state_dataset}.state_supervision_sentence` supervision_sentence
                ON offendercases.CASE_NUMBER = supervision_sentence.external_id
            WHERE TA_type = '1'
        )
        WHERE term_date_clean < projected_completion_date
    ),
    us_nd_ed_regular AS (
        SELECT
            *,
            EXTRACT(date FROM PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S%p', term_date)) AS term_date_clean,
            'SUSPENDED' AS sentence_type,
        FROM `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offendercasestable_latest` offendercases
        -- To identify early discharges from probation in US_ND, we were told to look for TA_TYPE = '5'
        -- (term type = 5) corresponds to a "positive termination" reason. However, this designation is
        -- only available for SUSPENDED sentence types. There are a smaller number of deferred sentences,
        -- and there is no term type designated for early termination of deferred sentence types. Instead,
        -- we have to identify deferred sentences (TA_TYPE = '1') then compare the term date with the
        -- projected end date. If the term date is earlier than the projected end date, it would be
        -- considered an early termination
        WHERE TA_TYPE = '5'
    ),
    us_nd_ed_all AS (
        SELECT * FROM us_nd_ed_regular
        UNION ALL
        SELECT * FROM us_nd_ed_deferred_sentence
    ),
    # Next - there are a handful of duplicates on SID and term_date i.e. same person/termination date but maybe different case numbers
    # For our purposes we want to keep 1 person / term_date, and deterministically dedup by keeping the first case_number
    us_nd_ed_all_dedup AS (
        SELECT *
        FROM us_nd_ed_all
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY SID, term_date_clean
            ORDER BY case_number
        ) = 1
    ),
    -- Join to sessions based on state/person, and where discharge date lands between start/end date
    -- Due to differences in when sessions determines start/end dates, expand the interval by 1 day
    us_nd_ed_sessions AS (
        SELECT
            sessions.state_code,
            sessions.person_id,
            sessions.session_id,
            us_nd_ed_all_dedup.term_date_clean AS ed_date,
            sessions.end_date,
            ABS(DATE_DIFF(us_nd_ed_all_dedup.term_date_clean, sessions.end_date, DAY)) AS discharge_to_session_end_days,
            sessions.outflow_to_level_1,
        FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
        LEFT JOIN `{project_id}.{state_dataset}.state_person_external_id` pei
            ON sessions.person_id = pei.person_id
            AND sessions.state_code = pei.state_code
        LEFT JOIN us_nd_ed_all_dedup
            ON us_nd_ed_all_dedup.SID = pei.external_id
            -- TODO(#10706) check if this value should be different from what we use in Idaho
            AND DATE_SUB(us_nd_ed_all_dedup.term_date_clean, INTERVAL 1 day) BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
        WHERE sessions.state_code = 'US_ND'
            AND id_type = 'US_ND_SID'
            AND sessions.end_date IS NOT NULL
            AND compartment_level_1 = "SUPERVISION"
            -- Before 2016, the old system US_ND used had few if any checks and
            -- balances for what a PO could enter as a termination reason. We
            -- restrict to looking at positive terminations after 2016 when
            -- termination reasons are more reliable
            AND term_date_clean >= '2016-01-01'
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY sessions.person_id, sessions.session_id
            ORDER BY term_date_clean DESC
        ) = 1
    )

    SELECT * FROM us_nd_ed_sessions
"""

US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    state_dataset=STATE_BASE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_nd"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()
