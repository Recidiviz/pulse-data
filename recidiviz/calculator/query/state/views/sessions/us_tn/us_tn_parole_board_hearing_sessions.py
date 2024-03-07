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
"""Sessionized view of parole board hearings off of pre-processed raw TN data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_1_super_sessions import (
    COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "us_tn_parole_board_hearing_sessions"

_VIEW_DESCRIPTION = (
    "Sessionized view of parole board hearings constructed off of pre-processed raw TN "
    "data"
)

_QUERY_TEMPLATE = """
WITH hearing_tbl AS (
    SELECT
        pei.state_code,
        pei.person_id,
        DATE(HearingDate) AS hearing_date,
        DATE(ParoleDecisionDate) AS decision_date,
        DATE(CertificateReleaseDate) AS tentative_parole_date,
        IF(
            -- rows from early 90s and before have placeholder hearing officer staff ID
            -- with no associated recommendation information
            HearingOfficerStaffID = "OBSCIS01",
            NULL,
            HearingOfficerStaffID
        ) AS recommendation_officer_external_id,

        -- recommended decision
        CASE
            WHEN RecommendedDecision IN (
                "BS", "DB", "DC", "DE", "DG", "DN", "DR", "DS", "DT", "OB", "OF", "OL",
                "OX", "OY", "PF", "RB", "RC", "RE", "RS", "RV", "SR", "TS"
            ) THEN "DENIED"
            WHEN RecommendedDecision IN (
                "CP", "EF", "ER", "MC", "OA", "OH", "OU", "PN", "PP", "PR", "RD", "RG",
                "RM", "RN", "RP", "RR", "RT", "ZR"
            ) THEN "APPROVED"
            WHEN RecommendedDecision IN (
                "CN", "CO", "CR", "OV", "ZC"
            ) THEN "CONTINUED"
            WHEN RecommendedDecision IS NULL THEN "EXTERNAL_UNKNOWN"
            ELSE "INTERNAL_UNKNOWN" END AS recommended_decision,
        RecommendedDecision AS recommended_decision_raw,

        -- actual decision
        CASE
            WHEN ParoleDecision IN (
                "BS", "DB", "DC", "DE", "DG", "DN", "DR", "DS", "DT", "OB", "OF", "OL",
                "OX", "OY", "PF", "RB", "RC", "RE", "RS", "RV", "SR", "TS"
            ) THEN "DENIED"
            WHEN ParoleDecision IN (
                "CP", "EF", "ER", "MC", "OA", "OH", "OU", "PN", "PP", "PR", "RD", "RG",
                "RM", "RN", "RP", "RR", "RT", "ZR"
            ) THEN "APPROVED"
            WHEN ParoleDecision IN (
                "CN", "CO", "CR", "OV", "ZC"
            ) THEN "CONTINUED"
            WHEN ParoleDecision IS NULL THEN "EXTERNAL_UNKNOWN"
            ELSE "INTERNAL_UNKNOWN" END AS decision,

        ParoleDecision AS decision_raw,

        ARRAY_TO_STRING(ARRAY_AGG(
            DISTINCT decision_reasons_unnested IGNORE NULLS
            ORDER BY decision_reasons_unnested
        ), ",") AS decision_reasons_raw,

    FROM
        `{project_id}.{us_tn_raw_dataset}.Hearing_latest` hearings,
    UNNEST(
        [DecisionReason1, DecisionReason2, DecisionReason3, DecisionReason4]
    ) AS decision_reasons_unnested
    INNER JOIN
        `{project_id}.{normalized_state}.state_person_external_id` pei
    ON
        hearings.OffenderID = pei.external_id
    WHERE
        pei.state_code = "US_TN"
        -- filter to relevant hearing types
        -- ignore revocation/recission hearings (including time setting) as well as
        -- "custodial hearings"
        -- see https://www.tn.gov/bop/parole-hearings-division/types-of-parole-hearings-lnklst-pg.html
        AND HearingType IN (
            "IP", -- INITIAL PAROLE
            "PV" -- PAROLE REVIEW
        )
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        -- drop same-day hearings, prioritizing by alphabetical order of decision value
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id, hearing_date ORDER BY decision ASC
        ) = 1
)

SELECT
    a.state_code,
    a.person_id,

    -- CL1SS details
    compartment_level_1_super_session_id,
    start_date AS incarceration_start_date,
    end_date_exclusive AS incarceration_end_date_exclusive,

    -- hearing number within CL1SS
    ROW_NUMBER() OVER w AS hearing_number,

    -- session start and 'end' dates
    IFNULL(LAG(hearing_date) OVER w, start_date) AS start_date,
    hearing_date,
    decision_date,

    -- decision information
    recommendation_officer_external_id,
    recommended_decision,
    recommended_decision_raw,
    decision,
    decision_raw,
    decision_reasons_raw,
    tentative_parole_date,

    -- days from incarceration start to hearing
    DATE_DIFF(
        hearing_date, start_date, DAY
    ) AS days_since_incarceration_start,

    -- days from prior hearing to current hearing
    DATE_DIFF(hearing_date,
        LAG(hearing_date) OVER w, DAY)
    AS days_since_prior_hearing,

FROM
    hearing_tbl a
-- join incarceration super sessions so that date diffs fall within same SS
INNER JOIN
    `{project_id}.{compartment_level_1_super_sessions}` b
ON
    a.person_id = b.person_id
    AND a.hearing_date BETWEEN b.start_date AND IFNULL(b.end_date, "9999-01-01")
WHERE
    b.compartment_level_1 = "INCARCERATION"
WINDOW
    w AS (PARTITION BY a.person_id, b.start_date ORDER BY hearing_date ASC)

"""

US_TN_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
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
        US_TN_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER.build_and_print()
