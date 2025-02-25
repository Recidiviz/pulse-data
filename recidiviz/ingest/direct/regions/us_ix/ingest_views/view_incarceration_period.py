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
"""Query that generates the incarceration period entity using the following tables:
com_Transfer, com_TransferReason, com_TransferStatus, com_TransferType, hsn_Bed, hsn_BedAssignment, hsn_BedType,
hsn_Bed_SecurityLevel, hsn_ChangeReason, hsn_SecurityLevel, ind_LegalStatus, ind_LegalStatusObjectCharge,
ind_OffenderLegalStatus, ref_Location, ref_LocationSubType, ref_LocationType, scl_Charge"""
from recidiviz.ingest.direct.regions.us_ix.ingest_views.query_fragments import (
    BED_ASSIGNMENT_PERIODS_CTE,
    CLIENT_ADDRESS_CTE,
    LEGAL_STATUS_PERIODS_CTE,
    LEGAL_STATUS_PERIODS_INCARCERATION_CTE,
    LOCATION_DETAILS_CTE,
    SECURITY_LEVEL_PERIODS_CTE,
    TRANSFER_DETAILS_CTE,
    TRANSFER_PERIODS_CTE,
    TRANSFER_PERIODS_INCARCERATION_CTE,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
{LOCATION_DETAILS_CTE},
{BED_ASSIGNMENT_PERIODS_CTE},
{LEGAL_STATUS_PERIODS_CTE},
{LEGAL_STATUS_PERIODS_INCARCERATION_CTE},
{TRANSFER_DETAILS_CTE},
{TRANSFER_PERIODS_CTE},
{TRANSFER_PERIODS_INCARCERATION_CTE},
{SECURITY_LEVEL_PERIODS_CTE},
{CLIENT_ADDRESS_CTE},

-- This cte treats the transfer period as the primary source of truth for when a person
-- is incarcerated, and attempts to map a legal status on to the transfer period by
-- searching for what legal status was active at the time of the transfer. This usually
-- works quite well, since legal statuses are only at day-level granularity but transfers
-- have datetime granularity, so a legal status becomes active at 00:00:00 but the transfer
-- does not occur until later in the day.
transfer_periods_with_legal_status AS (
    SELECT * FROM (
        SELECT
            t.OffenderId,
            Start_TransferReasonDesc,
            End_TransferReasonDesc,
            Start_TransferTypeDesc,
            End_TransferTypeDesc,
            Start_TransferDate,
            End_TransferDate,
            DOCLocationToName,
            DOCLocationToTypeName,
            DOCLocationToSubTypeName,
            LegalStatusDesc,
            ROW_NUMBER() OVER (
                PARTITION BY t.OffenderId, Start_TransferDate
                ORDER BY Priority
            ) AS rn,
        FROM transfer_periods_incarceration_cte t
        LEFT JOIN legal_status_periods_incarceration_cte ls
            ON t.OffenderId = ls.OffenderId
            AND t.Start_TransferDate BETWEEN ls.LegalStatus_StartDate AND IFNULL(ls.LegalStatus_EndDate, '9999-12-31')
    ) a
    WHERE a.rn = 1
),

-- Adds the security level based on bed assignment at the time of transfer
transfer_periods_with_legal_status_with_beds_and_security_level AS (
    SELECT
        p.OffenderId,
        p.period_start as Start_TransferDate,
        p.period_end as End_TransferDate,
        -- Only populate transfer details if this period matches the transfer date
        -- This prevents us from improperly setting the admission reason for subsequent
        -- periods following the actual transfer when the actual admission reason is due
        -- to some other attribute changing, like supervision level or supervising officer
        CASE WHEN p.period_start = tls.Start_TransferDate THEN tls.Start_TransferReasonDesc
             ELSE 'Housing Unit Transfer'
             END AS Start_TransferReasonDesc,
        CASE WHEN p.period_end = tls.End_TransferDate THEN tls.End_TransferReasonDesc
             ELSE 'Housing Unit Transfer'
             END AS End_TransferReasonDesc,
        -- We use Start_TransferTypeDesc to determine whether this period is an incarceration period or not, 
        -- so we keep this valued for all rows
        tls.Start_TransferTypeDesc,
        tls.LegalStatusDesc,
        tls.DOCLocationToName,
        tls.DOCLocationToTypeName,
        tls.DOCLocationToSubTypeName,
        slp.SecurityLevelName,
        b.LevelPath,
        cac.JurisdictionId
    FROM (
        -- Create periods based on all the dates in transfer_periods_with_legal_status and bed_assignment_periods_cte and security_level_cte
        SELECT
            OffenderId,
            dte as period_start,
            LEAD(dte) OVER(PARTITION BY OffenderId ORDER BY dte) as period_end
        FROM (
            SELECT 
                DISTINCT
                OffenderId,
                Start_TransferDate as dte
            FROM transfer_periods_with_legal_status
            
            UNION DISTINCT 

            SELECT 
                DISTINCT
                OffenderId,
                End_TransferDate as dte
            FROM transfer_periods_with_legal_status

            UNION DISTINCT

            SELECT
                DISTINCT
                OffenderId,
                FromDate as dte
            FROM bed_assignment_periods_cte  

            UNION DISTINCT

            SELECT
                DISTINCT
                OffenderId,
                ToDate as dte
            FROM bed_assignment_periods_cte

            UNION DISTINCT

            SELECT
                DISTINCT
                OffenderId,
                startDate as dte
            FROM security_level_periods_cte  

            UNION DISTINCT

            SELECT
                DISTINCT
                OffenderId,
                endDate as dte
            FROM security_level_periods_cte  

            UNION DISTINCT

            SELECT
                DISTINCT
                OffenderId,
                StartDate AS dte,
            FROM client_addresses_cte

            UNION DISTINCT

            SELECT
                DISTINCT
                OffenderId,
                EndDate AS dte,
            FROM client_addresses_cte
        ) spans 
    ) p
    -- merge on all relevant info from transfer_periods_with_legal_status and bed_assignment_periods_cte for each period
    LEFT JOIN transfer_periods_with_legal_status tls
        ON p.OffenderId = tls.OffenderId
        AND p.period_start >= tls.Start_TransferDate
        AND COALESCE(p.period_end, DATE(9999,12,31)) <= COALESCE(tls.End_TransferDate, DATE(9999,12,31))
    LEFT JOIN bed_assignment_periods_cte b
        ON p.OffenderId = b.OffenderId
        AND p.period_start >= b.FromDate
        AND COALESCE(p.period_end, DATE(9999,12,31)) <= COALESCE(b.ToDate, DATE(9999,12,31))
    LEFT JOIN client_addresses_cte cac
        ON p.OffenderId = cac.OffenderId
        AND p.period_start >= cac.StartDate
        AND COALESCE(p.period_end, DATE(9999,12,31)) <= COALESCE(cac.EndDate, DATE(9999,12,31))
    LEFT JOIN security_level_periods_cte slp
        ON p.OffenderId = slp.OffenderId
        AND p.period_start >= slp.startDate
        AND COALESCE(p.period_end, DATE(9999,12,31)) <= COALESCE(slp.endDate, DATE(9999,12,31))
    -- filter out periods that start in the future
    WHERE p.period_start <= @update_timestamp
),

-- final cte, adds a period_id and drops the final period per person that starts with
-- Out from DOC since this is the start of a period of liberty. We wait until this point
-- to drop that period because we need information from that row for window functions in
-- previous CTEs.
incarceration_periods AS (
    SELECT *, ROW_NUMBER() OVER incarceration_period_window AS period_id
    FROM (
        SELECT
            OffenderId,
            Start_TransferDate,
            IF(End_TransferDate = '9999-12-31', NULL, End_TransferDate) AS End_TransferDate,
            Start_TransferReasonDesc,
            End_TransferReasonDesc,
            DOCLocationToName,
            DOCLocationToTypeName,
            LegalStatusDesc,
            SecurityLevelName,
            LevelPath,
            JurisdictionId
        FROM transfer_periods_with_legal_status_with_beds_and_security_level
        WHERE Start_TransferTypeDesc != 'Out from DOC'
    ) a
    WINDOW incarceration_period_window AS (
        PARTITION BY OffenderId
        ORDER BY Start_TransferDate
    )
)

SELECT *
FROM incarceration_periods
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="incarceration_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
