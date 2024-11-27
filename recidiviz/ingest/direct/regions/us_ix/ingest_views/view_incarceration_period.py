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
        tls.DOCLocationToName,
        tls.DOCLocationToTypeName,
        tls.DOCLocationToSubTypeName,
        slp.SecurityLevelName,
        b.LevelPath,
        b.BedAssignmentId,
        cac.JurisdictionId,
        ls.LegalStatusDesc
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
            FROM transfer_periods_incarceration_cte
            
            UNION DISTINCT 

            SELECT 
                DISTINCT
                OffenderId,
                End_TransferDate as dte
            FROM transfer_periods_incarceration_cte

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

            UNION DISTINCT

            SELECT
                DISTINCT 
                OffenderId,
                LegalStatus_StartDate AS dte
            FROM legal_status_periods_incarceration_cte

            UNION DISTINCT

            SELECT
                DISTINCT 
                OffenderId,
                LegalStatus_EndDate AS dte
            FROM legal_status_periods_incarceration_cte
            
        ) spans 
    ) p
    -- merge on all relevant info from transfer_periods_with_legal_status and bed_assignment_periods_cte for each period
    LEFT JOIN transfer_periods_incarceration_cte tls
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
    LEFT JOIN legal_status_periods_incarceration_cte ls
        ON p.OffenderId = ls.OffenderId
        AND p.period_start >= ls.LegalStatus_StartDate
        AND COALESCE(p.period_end, DATE(9999,12,31)) <= COALESCE(ls.LegalStatus_EndDate, DATE(9999,12,31))
    -- filter out periods that start in the future
    WHERE p.period_start <= @update_timestamp
),

-- final cte, adds a period_id and drops the final period per person that starts with
-- Out from DOC since this is the start of a period of liberty. We wait until this point
-- to drop that period because we need information from that row for window functions in
-- previous CTEs.
incarceration_periods AS (
    SELECT * EXCEPT(keep_prio), 
        ROW_NUMBER() OVER incarceration_period_window AS period_id
    FROM (
        SELECT DISTINCT
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
            JurisdictionId,
            RANK() OVER(PARTITION BY OffenderId, Start_TransferDate, End_TransferDate
                              ORDER BY 
                                CASE LegalStatusDesc WHEN 'Parole Violator' THEN 1
                                                     WHEN 'Rider' THEN 2
                                                     WHEN 'Termer' THEN 3
                                                     ELSE 4 END,
                                CAST(BedAssignmentId AS INT64) DESC) AS keep_prio
        FROM transfer_periods_with_legal_status_with_beds_and_security_level
        WHERE Start_TransferTypeDesc != 'Out from DOC'
    ) 
    WHERE keep_prio = 1
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
