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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.    If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Sessionized view of parole board hearing decisions pre-processed raw TN data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.compartment_level_1_super_sessions import (
    COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "us_tn_parole_board_hearing_decisions"

_VIEW_DESCRIPTION = (
    "Sessionized view of parole board hearing decisions constructed off of pre-processed "
    "raw TN data. Each row is a member's decision for a specific hearing."
)

_QUERY_TEMPLATE = f"""
WITH decision_tbl AS (
    -- primary key is state-person-member-hearing_date
    SELECT
        b.state_code,
        b.person_id,
        CAST(HearingDate AS DATE) AS hearing_date,
        StaffID AS member_external_id,
        CASE
            WHEN BoardAction IN ("DS", "BS", "DE") THEN "DENIED"
            WHEN BoardAction IN ("RR", "EF", "RD", "ER", "PP", "RP") THEN "APPROVED"
            WHEN BoardAction IN ("CR") THEN "CONTINUED"
            WHEN BoardAction IN ("EX") THEN "RECUSED"
            ELSE "INTERNAL_UNKNOWN" END AS member_decision,
        BoardAction AS member_decision_raw,
        STRING_AGG(Comments, "; ") AS member_comments_raw,
    FROM
        `{{project_id}}.{{us_tn_raw_dataset}}.BoardAction_latest` a
    INNER JOIN
        `{{project_id}}.{{normalized_state}}.state_person_external_id` b
    ON
        a.OffenderID = b.external_id
        AND b.state_code = "US_TN"
    WHERE
        -- filter to relevant hearing types
        -- ignore revocation/recission hearings (including time setting) as well as 
        -- "custodial hearings"
        -- see https://www.tn.gov/bop/parole-hearings-division/types-of-parole-hearings-lnklst-pg.html
        HearingType IN (
            "IP", -- INITIAL PAROLE
            "PV" -- PAROLE REVIEW
        )
        -- member must have voted
        AND BoardAction IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    a.state_code,
    a.person_id,
    compartment_level_1_super_session_id,
    hearing_date,
    member_external_id,
    member_decision,
    member_decision_raw,
    member_comments_raw,
FROM
    decision_tbl a
-- join incarceration super sessions to ensure hearings occurred during incarceration
INNER JOIN
    `{{project_id}}.{{compartment_level_1_super_sessions}}` b
ON
    a.person_id = b.person_id
    AND a.hearing_date BETWEEN b.start_date AND
        {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
    AND b.compartment_level_1 = "INCARCERATION"
"""

US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    normalized_state=NORMALIZED_STATE_DATASET,
    us_tn_raw_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    compartment_level_1_super_sessions=COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER.table_for_query.to_str(),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER.build_and_print()
