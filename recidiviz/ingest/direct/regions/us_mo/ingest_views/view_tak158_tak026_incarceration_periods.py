# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing incarceration period from sentence information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH status_bw AS (
        SELECT
            *
        FROM
            {LBAKRDTA_TAK026}
        WHERE
            BW_SCD IS NOT NULL
            AND BW_SCD != ''
        ),
    -- To determine spans of time for incarceration periods, we use TAK158 (body status) and TAK026 (status) to determine when someone changes 
    -- statuses. Based on the information from TAK158 and TAK026, we create partitions that tell us whether someone has an open status, 
    -- a closed status, or a change in status (a partition). The following 3 CTEs create separate tables for these situations and then
    -- `all_sub_sub_cycle_critical_dates` unions the spans together to get all the spans with status changes for a given individual. 
    -- `sub_subcycle_spans` then self joins these start and end dates to create the unique set of periods for a person according to their statuses.
    board_holdover_parole_revocation_partition_statuses AS (
        SELECT
            BW_DOC AS DOC,
            BW_CYC AS CYC,
            MIN(BW_SSO) AS SSO,
            -- When the parole update happens there might be multiple related
            -- statuses on the same day (multiple updates), but they all should
            -- correspond to the same revocation edge so I group them and pick
            -- one (doesn't matter which one since they'll all get mapped to the
            -- same enum).
            MIN(BW_SCD) AS SCD,
            BW_SY AS STATUS_CODE_CHG_DT,
            'I' AS SUBCYCLE_TYPE_STATUS_CAN_PARTITION
        FROM
            status_bw
        WHERE (
            BW_SCD LIKE '50N10%' OR -- Parole Update statuses
            BW_SCD LIKE '50N30%' -- Conditional Release Update statuses
        )
        GROUP BY BW_DOC, BW_CYC, BW_SY
    ),
    sub_cycle_partition_statuses AS (
        SELECT
            DOC,
            CYC,
            SSO,
            SCD,
            STATUS_CODE_CHG_DT,
            SUBCYCLE_TYPE_STATUS_CAN_PARTITION
        FROM
            board_holdover_parole_revocation_partition_statuses
    ),
    -- This CTE filters for only periods where someone is listed as "I" (Institution).
    incarceration_subcycle_body_status AS (
        SELECT * 
        FROM {LBAKRDTA_TAK158} body_status_f1
        WHERE body_status_f1.F1_DOC IS NOT NULL
    ),
    subcycle_partition_status_change_dates AS (
        SELECT
            sub_cycle_partition_statuses.DOC AS DOC,
            sub_cycle_partition_statuses.CYC AS CYC,
            body_status_f1.F1_SQN AS SQN,
            body_status_f1.F1_SST AS SST,
            body_status_f1.F1_PFI AS PFI,
            sub_cycle_partition_statuses.SSO AS STATUS_SEQ_NUM,
            sub_cycle_partition_statuses.SCD AS STATUS_CODE,
            '' AS STATUS_SUBTYPE,
            sub_cycle_partition_statuses.STATUS_CODE_CHG_DT AS STATUS_CODE_CHG_DT,
            '2-PARTITION' AS SUBCYCLE_DATE_TYPE
        FROM
            incarceration_subcycle_body_status body_status_f1
        LEFT OUTER JOIN
            sub_cycle_partition_statuses
        ON
            body_status_f1.F1_DOC = sub_cycle_partition_statuses.DOC AND
            body_status_f1.F1_CYC = sub_cycle_partition_statuses.CYC AND
            body_status_f1.F1_SST = sub_cycle_partition_statuses.SUBCYCLE_TYPE_STATUS_CAN_PARTITION AND
            body_status_f1.F1_CD < sub_cycle_partition_statuses.STATUS_CODE_CHG_DT AND
            sub_cycle_partition_statuses.STATUS_CODE_CHG_DT < body_status_f1.F1_WW
        WHERE sub_cycle_partition_statuses.DOC IS NOT NULL
    ),
    subcycle_open_status_change_dates AS (
        SELECT
            F1_DOC AS DOC,
            F1_CYC AS CYC,
            F1_SQN AS SQN,
            F1_SST AS SST,
            F1_PFI AS PFI,
            '0' AS STATUS_SEQ_NUM,
            F1_ORC AS STATUS_CODE,
            F1_OPT AS STATUS_SUBTYPE,
            F1_CD AS STATUS_CODE_CHG_DT,
            '1-OPEN' AS SUBCYCLE_DATE_TYPE
        FROM
            incarceration_subcycle_body_status body_status_f1
    ),
    subcycle_close_status_change_dates AS (
        SELECT
            F1_DOC AS DOC,
            F1_CYC AS CYC,
            F1_SQN AS SQN,
            F1_SST AS SST,
            F1_PFI AS PFI,
            '0' AS STATUS_SEQ_NUM,
            F1_CTP AS STATUS_CODE,
            F1_ARC AS STATUS_SUBTYPE,
            F1_WW AS STATUS_CODE_CHG_DT,
            '3-CLOSE' AS SUBCYCLE_DATE_TYPE
        FROM
            incarceration_subcycle_body_status body_status_f1
    ),
    all_sub_sub_cycle_critical_dates AS (
        SELECT
            DOC, CYC, SQN, SST, PFI, STATUS_SEQ_NUM, STATUS_CODE, STATUS_SUBTYPE, STATUS_CODE_CHG_DT, SUBCYCLE_DATE_TYPE,
            ROW_NUMBER() OVER (
                PARTITION BY DOC, CYC, SQN
                ORDER BY
                    /* Order open edges, then partition edges, then close edges */
                    SUBCYCLE_DATE_TYPE,
                    /* Orders edges by date (open edges and close edges will
                       already come first and last, respectively */
                    STATUS_CODE_CHG_DT,
                    /* Within partition statuses that happen on the same day,
                       order by the status SSO number */
                    STATUS_SEQ_NUM ASC
            ) AS SUB_SQN_SEQ
        FROM (
            SELECT * FROM subcycle_open_status_change_dates
            UNION DISTINCT
            SELECT * FROM subcycle_partition_status_change_dates
            UNION DISTINCT
            SELECT * FROM subcycle_close_status_change_dates
        ) all_dates
    ),
    sub_subcycle_spans AS (
        SELECT
            start_date.DOC, start_date.CYC, start_date.SQN, start_date.SST, start_date.PFI,
            start_date.STATUS_CODE_CHG_DT AS SUB_SUBCYCLE_START_DT,
            start_date.STATUS_SEQ_NUM AS START_STATUS_SEQ_NUM,
            start_date.STATUS_CODE AS START_STATUS_CODE,
            start_date.STATUS_SUBTYPE AS START_STATUS_SUBTYPE,
            CASE WHEN end_date.STATUS_CODE_CHG_DT NOT IN ('0') THEN end_date.STATUS_CODE_CHG_DT ELSE NULL END AS SUB_SUBCYCLE_END_DT,
            end_date.STATUS_SEQ_NUM AS END_STATUS_SEQ_NUM,
            end_date.STATUS_CODE AS END_STATUS_CODE,
            end_date.STATUS_SUBTYPE AS END_STATUS_SUBTYPE
        FROM
            all_sub_sub_cycle_critical_dates start_date
        LEFT OUTER JOIN
            all_sub_sub_cycle_critical_dates end_date
        ON
            start_date.DOC = end_date.DOC AND
            start_date.CYC = end_date.CYC AND
            start_date.SQN = end_date.SQN AND
            start_date.SUB_SQN_SEQ = end_date.SUB_SQN_SEQ - 1

        /* Filter out rows created by the join which start with a 'CLOSE'
         * status - periods can only start with 'OPEN' or 'PARTITION' statuses
         */
        WHERE start_date.SUBCYCLE_DATE_TYPE != '3-CLOSE'
    ),
    -- This CTE aggregates all stasuses for a given date for a person to use for reason mapping
    all_scd_codes_by_date AS (
        -- All SCD status codes grouped by DOC, CYC, and SY (Date).
        SELECT
            BW_DOC,
            BW_CYC,
            BW_SY AS STATUS_DATE,
            STRING_AGG(DISTINCT BW_SCD, ',' ORDER BY BW_SCD) AS STATUS_CODES
        FROM
            status_bw
        GROUP BY BW_DOC, BW_CYC, BW_SY
    ),
    most_recent_status_updates as (
        SELECT
            BW_DOC, BW_CYC,
            MAX(BW_SY) AS MOST_RECENT_SENTENCE_STATUS_DATE
        FROM status_bw
        GROUP BY BW_DOC, BW_CYC
    ),
    cleaned_facility_locations AS (
        SELECT 
            CS_DOC,
            CS_CYC,
            CS_NM,
            NULLIF(CS_DD, '0') as CS_DD,
            CS_OLA
        FROM {LBAKRDTA_TAK065} locations
        WHERE CS_OLA IS NOT NULL
    ),
    cleaned_housing_details AS (
        SELECT
            BN_DOC,
            BN_CYC,
            BN_HS,
            NULLIF(BN_HE, '0') AS BN_HE,
            BN_LRU,
            CASE BN_LRU
                WHEN 'TAS' THEN 1
                WHEN 'ADS' THEN 2
                WHEN 'DIS' THEN 3
                WHEN 'NOC' THEN 4
                WHEN 'PRC' THEN 5
                WHEN 'HOS' THEN 6
                WHEN 'INF' THEN 7
                WHEN 'GNP' THEN 8
                WHEN '***' THEN 9
                ELSE 10
            END AS housing_type_rank
        FROM {LBAKRDTA_TAK017} housing
        WHERE BN_HS != '0' AND BN_LRU IS NOT NULL
    ),
    -- To join the status spans of time with facility location spans of time, we use the following 7 CTEs to find all possible start dates
    -- and end dates among the two sources and join them in order to get a comprehensive  list of any time someone changed either 
    -- status, location, or both. We also create specific CTEs to handle same day periods for either status of facility then union those
    -- to have a comprehensive list of all possible spans across status, facility, different days, and same days. 
    unioned_date_cte AS (
        SELECT
            DOC,
            CYC,
            SUB_SUBCYCLE_START_DT as StartDate,
            SUB_SUBCYCLE_END_DT as EndDate
        FROM sub_subcycle_spans 
        UNION ALL 
        SELECT
            CS_DOC as DOC,
            CS_CYC as CYC,
            CS_NM AS StartDate,
            cleaned_facility_locations.CS_DD AS EndDate,
        FROM cleaned_facility_locations
        UNION ALL 
        SELECT
            BN_DOC as DOC,
            BN_CYC as CYC,
            BN_HS AS StartDate,
            BN_HE AS EndDate,
        FROM cleaned_housing_details
    ),
    start_date_cte AS (
        /*
        Generate full list of the period start dates. This will include those dates currently labeled as start dates as
        well as the end date (or end date + 1 day if end dates are not inclusive) when that value comes between
        another period that a person has.
        */
        SELECT DISTINCT
            DOC,
            CYC,
            StartDate,
        FROM unioned_date_cte
        UNION DISTINCT
        SELECT DISTINCT
            orig.DOC,
            orig.CYC,
            new_start_dates.EndDate AS StartDate,
        FROM unioned_date_cte orig
        JOIN unioned_date_cte new_start_dates
            ON orig.DOC = new_start_dates.DOC
            AND orig.CYC = new_start_dates.CYC
            AND new_start_dates.EndDate > orig.StartDate
            AND new_start_dates.EndDate < COALESCE(orig.EndDate, '99990101')
    ),
    end_date_cte AS (
        /*
        Generate full list of the period end dates. This will include those dates currently labeled as end dates as
        well as the start date (or start date - 1 day if end dates are not inclusive) when that value comes between
        another period that a person has.
        */
        SELECT DISTINCT
            DOC,
            CYC,
            EndDate AS EndDate,
        FROM unioned_date_cte
        UNION DISTINCT
        SELECT DISTINCT
            orig.DOC,
            orig.CYC,
            new_end_dates.StartDate AS EndDate,   
        FROM unioned_date_cte orig
        JOIN unioned_date_cte new_end_dates
            ON orig.DOC = new_end_dates.DOC
            AND orig.CYC = new_end_dates.CYC
            AND new_end_dates.StartDate > orig.StartDate
            AND new_end_dates.StartDate < COALESCE(orig.EndDate,'99990101')
    ),
    start_and_end_dates_join AS (
        /*
        Join start and end dates together. The end date for each start date will be the first end date that comes 
        after the start date. At this point we have a CTE that defines new period boundaries and can be joined 
        back to the original cte to get attributes of the period.
        */
        SELECT
            start_date_cte.DOC,
            start_date_cte.CYC,
            start_date_cte.StartDate,
            end_date_cte.EndDate
        FROM start_date_cte
        JOIN end_date_cte
            ON start_date_cte.DOC = end_date_cte.DOC
            AND start_date_cte.CYC = end_date_cte.CYC
            AND start_date_cte.StartDate< COALESCE(end_date_cte.EndDate, '99990101')
    ),
    start_and_end_dates_different_days AS (
        SELECT 
            sej.*
        FROM (
            SELECT 
                sej.*,
                ROW_NUMBER() OVER(PARTITION BY DOC, StartDate ORDER BY COALESCE(EndDate, '99990101') ASC) as RowNumber
            FROM start_and_end_dates_join sej
            ) sej
        WHERE RowNumber = 1
    ),
    start_and_end_dates_same_day_status AS (
        SELECT 
            DOC, 
            CYC, 
            SUB_SUBCYCLE_START_DT as StartDate, 
            SUB_SUBCYCLE_END_DT as EndDate,
            CAST('1' AS INT64) as RowNumber
        FROM sub_subcycle_spans
        WHERE SUB_SUBCYCLE_START_DT = SUB_SUBCYCLE_END_DT
    ),
    start_and_end_dates_same_day_facility AS (
        SELECT 
            CS_DOC as DOC,
            CS_CYC as CYC,
            CS_NM as StartDate,
            CS_DD as EndDate,
            CAST('1' as INT64) as RowNumber
        FROM cleaned_facility_locations
        WHERE CS_NM=CS_DD
    ),
    start_and_end_dates_same_day_housing AS (
        SELECT 
            BN_DOC as DOC,
            BN_CYC as CYC,
            BN_HS as StartDate,
            BN_HE as EndDate,
            CAST('1' as INT64) as RowNumber
        FROM cleaned_housing_details
        WHERE BN_HS=BN_HE
    ),
    start_and_end_dates_cte AS (
        SELECT 
            *
        FROM start_and_end_dates_different_days
        UNION ALL 
        SELECT 
            *
        FROM start_and_end_dates_same_day_status
        UNION ALL 
        SELECT 
            *
        FROM start_and_end_dates_same_day_facility
        UNION ALL 
        SELECT 
            *
        FROM start_and_end_dates_same_day_housing
    ),
    start_end_facility AS (
        SELECT *
        FROM (
            SELECT 
                se.*,
                f.CS_OLA,
                ROW_NUMBER() OVER (PARTITION BY se.DOC, se.CYC, se.StartDate, se.EndDate 
                ORDER BY ((CAST(CS_DD AS INT64)) - CAST(CS_NM AS INT64)) ASC NULLS LAST) as f_rn
            FROM start_and_end_dates_cte se
            LEFT JOIN cleaned_facility_locations f
            ON se.DOC = f.CS_DOC
                AND se.CYC = f.CS_CYC
                AND se.StartDate >= f.CS_NM 
                AND COALESCE(se.EndDate,'99990101') <= COALESCE(f.CS_DD,'99990101')
        ) ranked
        WHERE f_rn = 1
    ),
    start_end_facility_housing AS (
        SELECT *
        FROM (
            SELECT 
                se.*,
                h.BN_LRU,
                -- When multiple housing periods overlap, prioritize the housing type 
                -- with the highest priority rank (assigned in cleaned_housing_details); 
                -- if there are multiple, use the type from the shortest period.
                ROW_NUMBER() OVER (
                    PARTITION BY se.DOC, se.CYC, se.StartDate, se.EndDate 
                    ORDER BY
                        housing_type_rank ASC,
                        CAST(BN_HE AS INT64) - CAST(BN_HS AS INT64) ASC 
                        NULLS LAST
                ) as h_rn
            FROM start_end_facility se
            LEFT JOIN cleaned_housing_details h
            ON se.DOC = h.BN_DOC
                AND se.CYC = h.BN_CYC
                AND se.StartDate >= h.BN_HS
                AND COALESCE(se.EndDate,'99990101') <= COALESCE(h.BN_HE,'99990101')
        ) ranked
        WHERE h_rn = 1
    ),
    -- Using the unioned table from above, we join the comprehensive list of start and end dates to the sub_subcycle_span 
    -- information and the facility information in the following 2 CTEs, handling zero days periods with window functions. 
    periods_with_location_and_status AS (
        SELECT * 
        FROM (
            SELECT 
                se.DOC,
                se.CYC,
                se.StartDate as START_DATE,
                se.EndDate as END_DATE,
                se.CS_OLA as FACILITY,
                se.BN_LRU as HOUSING_TYPE,
                full_span_details.SST,

                -- End status codes, end status subtypes, and the dates used to obtain status code
                -- lists are no longer set for periods within the boundaries of a sub-subcycle
                -- (i.e., this information only applies to periods that start when a sub-subcycle
                -- opens or end when a sub-subcycle closes). This prevents admission/release reasons
                -- from being set erroneously for periods constructed within sub-subcycles based
                -- on change in facility/housing status. Transitions between these periods will lack
                -- status info and therefore be treated as transfers. 
                -- TODO(#15905): Do away with the sub-subcycle structure altogether in a full refactor 
                -- of this view.

                CASE WHEN se.StartDate = full_span_details.SUB_SUBCYCLE_START_DT THEN se.StartDate ELSE NULL END AS subcycle_start,
                CASE WHEN se.EndDate = full_span_details.SUB_SUBCYCLE_END_DT AND se.EndDate IS NOT NULL THEN se.EndDate ELSE NULL END AS subcycle_end,
                CASE WHEN se.EndDate = full_span_details.SUB_SUBCYCLE_END_DT AND se.EndDate IS NOT NULL THEN full_span_details.END_STATUS_CODE ELSE NULL END AS END_STATUS_CODE,
                CASE WHEN se.EndDate = full_span_details.SUB_SUBCYCLE_END_DT AND se.EndDate IS NOT NULL THEN full_span_details.END_STATUS_SUBTYPE ELSE NULL END AS END_STATUS_SUBTYPE,

                -- PFI is still set via the parent sub-subcycle, unlike start/end statuses.
                full_span_details.PFI,

                -- If there are multiple for a single start and end date then we want to keep the one that is smallest as that will 
                -- be the most relevant to our start and end dates. This is the logic that allows us to handle zero day periods, 
                -- as a zero day period will always match both the matching zero day sub cycle span as well as the previous sub cycle span. 
                -- In this case we want to only keep the row with the zero day sub cycle span. 
                ROW_NUMBER() OVER (PARTITION BY se.DOC, se.CYC, se.StartDate, se.EndDate 
                    ORDER BY ((CAST(full_span_details.SUB_SUBCYCLE_END_DT AS INT64)) - CAST(full_span_details.SUB_SUBCYCLE_START_DT AS INT64)) ASC NULLS LAST) as fsd_rn
            FROM start_end_facility_housing se 
            LEFT JOIN sub_subcycle_spans full_span_details
                ON se.DOC = full_span_details.DOC
                AND se.CYC = full_span_details.CYC
                AND se.StartDate >= full_span_details.SUB_SUBCYCLE_START_DT 
                AND COALESCE(se.EndDate,'99990101') <= COALESCE(full_span_details.SUB_SUBCYCLE_END_DT,'99990101')
            WHERE full_span_details.SUB_SUBCYCLE_START_DT IS NOT NULL AND 
            (
                full_span_details.SST = 'I' OR 
                (
                    full_span_details.SST IS NOT NULL AND
                    (se.CS_OLA IS NOT NULL OR se.BN_LRU IS NOT NULL)
                )
            )
        ) ordered
        WHERE fsd_rn = 1
    )

    SELECT
        periods_with_location_and_status.DOC,
        periods_with_location_and_status.CYC,
        periods_with_location_and_status.PFI,
        periods_with_location_and_status.END_STATUS_CODE,
        periods_with_location_and_status.END_STATUS_SUBTYPE,
        periods_with_location_and_status.START_DATE,
        CASE WHEN periods_with_location_and_status.END_DATE IN ('99999999') 
            THEN most_recent_status_updates.MOST_RECENT_SENTENCE_STATUS_DATE
            ELSE periods_with_location_and_status.END_DATE END AS END_DATE,
        periods_with_location_and_status.FACILITY,
        periods_with_location_and_status.HOUSING_TYPE,
        ROW_NUMBER() OVER (
            PARTITION BY DOC, CYC 
            ORDER BY START_DATE, END_DATE
        ) AS SQN,
        start_codes.STATUS_CODES AS START_SCD_CODES,
        end_codes.STATUS_CODES AS END_SCD_CODES,
    FROM
        periods_with_location_and_status
    LEFT OUTER JOIN
        all_scd_codes_by_date start_codes
    ON
        periods_with_location_and_status.DOC = start_codes.BW_DOC AND
        periods_with_location_and_status.CYC = start_codes.BW_CYC AND
        -- Set start status codes for a period if the period starts at the same time as 
        -- a sub-subcycle span does. Previously, start status codes were set based on the 
        -- start date of the parent sub-subcycle, such that every period within the 
        -- sub-subcycle would have the same start status codes. 
        periods_with_location_and_status.subcycle_start = start_codes.STATUS_DATE
    LEFT OUTER JOIN
        all_scd_codes_by_date end_codes
    ON
        periods_with_location_and_status.DOC = end_codes.BW_DOC AND
        periods_with_location_and_status.CYC = end_codes.BW_CYC AND
        -- Likewise, set end status codes for a period if the period ends at the same time as 
        -- a sub-subcycle span does.
        periods_with_location_and_status.subcycle_end = end_codes.STATUS_DATE
    LEFT OUTER JOIN
        most_recent_status_updates
    ON
        periods_with_location_and_status.DOC = most_recent_status_updates.BW_DOC AND
        periods_with_location_and_status.CYC = most_recent_status_updates.BW_CYC
    WHERE DOC IS NOT NULL
    """

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="tak158_tak026_incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
