# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query containing incarceration periods information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
    -- This CTE grabs all movements
    incarceration_start_movements AS (
        SELECT 
            OffenderCd, 
            MovementDesc, 
            CAST(MovementDt AS DATETIME) AS MovementDt,
            MovementId
        FROM {IA_DOC_Movements}
    ),
    -- This CTE grabs all spans of time where somebody had a relevant incarceration related legal status
    incarceration_status_dates AS (
        SELECT 
            OffenderCd, 
            SupervisionStatus, 
            CAST(SupervisionStatusStartDt AS DATETIME) AS SupervisionStatusStartDt, 
            CAST(SupervisionStatusEndDt AS DATETIME) AS SupervisionStatusEndDt, 
            SupervisionStatusReasonForChange,
            SupervisionStatusId
        FROM {IA_DOC_SupervisionStatuses}
        WHERE SupervisionStatus IN (
            "Prison",
            "Prison Safekeeper",
            "OWI Continuum",
            "Prison Compact",
            "Jail (Designated Site)",
            "Work Release"
        )  
    ),
    -- This CTE grabs all spans of time where somebody had a relevant incarceration related supervision modifier
    incarceration_modifier_dates AS (
        SELECT 
            OffenderCd, 
            SupervisionModifier, 
            CAST(SupervisionModifierStartDt AS DATETIME) AS SupervisionModifierStartDt, 
            CAST(SupervisionModifierEndDt AS DATETIME) AS SupervisionModifierEndDt,
            SupervisionModifierId
        FROM {IA_DOC_Supervision_Modifiers}
        WHERE SupervisionModifier IN (
            "Concurrent Prison Sentence - Non-Iowa",
            "Paroled to Detainer - ICE",
            "In Prison",
            "Paroled to Detainer - INS",
            "Temp release to U.S. Marshall",
            "In Jail - Mental Health Assistance",
            "Detained by Non-Iowa Jurisdiction",
            "Detained by Another State",
            "In Jail",
            "Paroled to Detainer - Out of State",
            "Paroled to Detainer - Iowa",
            "Paroled to Detainer",
            "Paroled to Detainer - U. S. Marshall"
        )
    ),
    -- This CTE compiles all movements (from the movements table as well as the start and end reasons for statuses and modifiers)
    -- into a single CTE, designating a priority to be used for ordering later
    all_movements AS (
        SELECT DISTINCT
            OffenderCd,
            MovementDesc AS movement,
            MovementDt AS movement_date,
            1 AS movement_priority,
            MovementId as movement_pk
        FROM incarceration_start_movements

        UNION ALL

        SELECT DISTINCT
            OffenderCd,
            CONCAT("Status start - ", SupervisionStatus) AS movement,
            SupervisionStatusStartDt AS movement_date,
            2 AS movement_priority,
            SupervisionStatusId as movement_pk
        FROM incarceration_status_dates

        UNION ALL

        SELECT DISTINCT
            OffenderCd,
            SupervisionStatusReasonForChange AS movement,
            SupervisionStatusEndDt AS movement_date,
            2 AS movement_priority,
            SupervisionStatusId as movement_pk
        FROM incarceration_status_dates

        UNION ALL

        SELECT DISTINCT
            OffenderCd,
            CONCAT("Modifier start - ", SupervisionModifier) AS movement,
            SupervisionModifierStartDt AS movement_date,
            3 AS movement_priority,
            SupervisionModifierId as movement_pk
        FROM incarceration_modifier_dates
    ),
    -- this CTE unions all relevant dates together
    all_dates AS (
        SELECT DISTINCT OffenderCd, movement_date AS start_date, movement, 
            ROW_NUMBER() OVER(PARTITION BY OffenderCd, movement_date ORDER BY movement_priority, CAST(movement_pk AS INT64)) as sort_order
        FROM (
            SELECT * 
            FROM all_movements
            QUALIFY RANK() OVER(PARTITION BY OffenderCd, movement_date ORDER BY movement_priority ASC) = 1
        )

        UNION DISTINCT

        SELECT DISTINCT OffenderCd, SupervisionStatusStartDt AS start_date, 
            CAST(NULL AS STRING) AS movement, CAST(NULL AS INT64) as sort_order
        FROM incarceration_status_dates

        UNION DISTINCT

        SELECT DISTINCT OffenderCd, SupervisionStatusEndDt AS start_date, 
            CAST(NULL AS STRING) AS movement, CAST(NULL AS INT64) as sort_order
        FROM incarceration_status_dates

        UNION DISTINCT

        SELECT DISTINCT OffenderCd, SupervisionModifierStartDt AS start_date, 
            CAST(NULL AS STRING) AS movement, CAST(NULL AS INT64) as sort_order
        FROM incarceration_modifier_dates      

        UNION DISTINCT

        SELECT DISTINCT OffenderCd, SupervisionModifierEndDt AS start_date, 
            CAST(NULL AS STRING) AS movement, CAST(NULL AS INT64) as sort_order
        FROM incarceration_modifier_dates      
    ),
    -- This CTE creates tiny spans based on the relevant dates, keeping multiple copies
    -- of a single date only if there are multiple movements on the same date
    tiny_spans AS (
        SELECT *,
            LEAD(start_date) OVER(w) AS end_date,
            LEAD(movement) OVER(w) AS next_movement
        FROM ( 
            SELECT *
            FROM all_dates
            -- only keep multiple copies of a single date if there are multiple movements on that date
            QUALIFY RANK() OVER(PARTITION BY OffenderCd, start_date ORDER BY CASE WHEN movement IS NOT NULL THEN 1 ELSE 2 END ASC) = 1
        )
        WINDOW w AS (PARTITION BY OffenderCd ORDER BY start_date ASC)
    ),
    -- This CTE takes the tiny spans and joins on the relevant status and modifier information for each span
    spans_with_info AS (
        SELECT
            sp.OffenderCd,
            start_date,
            end_date,
            movement,
            next_movement,
            SupervisionStatus,
            SupervisionModifier,
            sort_order
        FROM tiny_spans sp
        LEFT JOIN incarceration_status_dates s
            ON sp.OffenderCd = s.OffenderCd 
            AND s.SupervisionStatusStartDt <= sp.start_date
            AND COALESCE(sp.end_date, DATE(9999,9,9)) <= COALESCE(s.SupervisionStatusEndDt, DATE(9999,9,9))
            AND s.SupervisionStatusEndDt IS DISTINCT FROM sp.start_date
        LEFT JOIN incarceration_modifier_dates m
            ON sp.OffenderCd = m.OffenderCd 
            AND m.SupervisionModifierStartDt <= sp.start_date
            AND COALESCE(sp.end_date, DATE(9999,9,9)) <= COALESCE(m.SupervisionModifierEndDt, DATE(9999,9,9))
            AND m.SupervisionModifierEndDt IS DISTINCT FROM sp.start_date
    ),
    -- This CTE filters down to only spans where there is either an ongoing incarceration legal status or
    -- an ongoing incarceration modifier.  Then this CTE creates a period_seq_num for ordering and the external id
    final AS (
        SELECT * EXCEPT (sort_order),
            ROW_NUMBER() OVER(PARTITION BY OffenderCd ORDER BY start_date, end_date NULLS LAST, sort_order) as period_seq_num
        FROM spans_with_info
        WHERE SupervisionModifier IS NOT NULL
        OR SupervisionStatus IS NOT NULL
    )
    
    SELECT * FROM final
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
