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
"""Query containing incarceration period from supervision information."""

from recidiviz.ingest.direct.regions.us_mo.ingest_views.us_mo_view_query_fragments import (
    ALL_OFFICERS_FRAGMENT,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFICER_ROLE_SPANS_FRAGMENT = f"""
    {ALL_OFFICERS_FRAGMENT},
    officers_with_role_time_ranks AS(
        -- Officers with their roles ranked from least recent to most recent,
        -- based on start date, then end date (current assignments with
        -- DTEORD=1 ranked last). If roles have the same start and end date,
        -- we look at the job ID to retain deterministic ordering.
        SELECT
            BDGNO,
            DEPCLS,
            CLSTTL,
            LNAME,
            FNAME,
            MINTL,
            STRDTE,
            ENDDTE,
            ROW_NUMBER() OVER (
                PARTITION BY BDGNO ORDER BY STRDTE, DTEORD, ENDDTE DESC, DEPCLS DESC
            ) AS ROLE_TIME_RANK
        FROM
            normalized_all_officers
    ),
    officer_role_spans AS (
        SELECT
            start_role.BDGNO,
            start_role.DEPCLS,
            start_role.CLSTTL,
            start_role.LNAME,
            start_role.FNAME,
            start_role.MINTL,
            start_role.STRDTE AS START_DATE,
            CASE
                -- Pick the next role start if not null, otherwise leave as 0
                WHEN start_role.ENDDTE = '0' THEN COALESCE(end_role.STRDTE, start_role.ENDDTE)
                WHEN (start_role.ENDDTE < start_role.STRDTE) THEN COALESCE(end_role.STRDTE, '0')
                ELSE start_role.ENDDTE
            END AS END_DATE,
            start_role.ROLE_TIME_RANK
        FROM
            officers_with_role_time_ranks start_role
        LEFT OUTER JOIN
            officers_with_role_time_ranks end_role
        ON
            start_role.BDGNO = end_role.BDGNO AND
            start_role.ROLE_TIME_RANK = end_role.ROLE_TIME_RANK - 1
    )
    """

VIEW_QUERY_TEMPLATE = f"""
WITH field_assignments_ce AS (
        SELECT
            {{LBAKRDTA_TAK034}}.*,
            CE_DOC AS DOC,
            CE_CYC AS CYC,
            -- We correct potential input error on closed periods with flipped start and end dates
            IF(CE_EH != '0', LEAST(CE_HF, CE_EH), CE_HF) AS FLD_ASSN_BEG_DT,
            IF(CE_EH != '0', GREATEST(CE_HF, CE_EH), CE_EH)  AS FLD_ASSN_END_DT,
            CE_PLN AS LOC_ACRO,
        FROM
            {{LBAKRDTA_TAK034}}
        -- We discard field assignments prior to 2000 due to unreliable data with a large number of overlapping periods
        WHERE 
            CE_HF >= '20000101'        
    ),
    field_assignments_with_unique_date_spans as (
        -- Where there are multiple rows for the same period, pick the one with the most recent update date.
        -- Also filter out 0-day periods.
        SELECT * EXCEPT(rn),
        LEAD(FLD_ASSN_BEG_DT)
                OVER (
                    PARTITION BY DOC, CYC 
                    ORDER BY FLD_ASSN_BEG_DT, CASE WHEN FLD_ASSN_END_DT = '0' THEN 1 ELSE 0 END, FLD_ASSN_END_DT)
            AS NEXT_FLD_ASSN_BEG_DT
        FROM (
          SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY DOC,CYC,FLD_ASSN_BEG_DT,FLD_ASSN_END_DT 
                               ORDER BY CAST(CE_DCR AS INT64) DESC, CAST(CE_TCR AS INT64) DESC) AS rn 
          FROM field_assignments_ce
        ) 
        WHERE rn = 1 AND FLD_ASSN_BEG_DT != FLD_ASSN_END_DT
    ),
    augmented_field_assignments AS (
        SELECT
            field_assignments_with_unique_date_spans.* REPLACE (
                -- Close open spans that are followed by new periods
                CASE 
                    WHEN FLD_ASSN_END_DT = '0' and NEXT_FLD_ASSN_BEG_DT IS NOT NULL THEN NEXT_FLD_ASSN_BEG_DT
                    ELSE FLD_ASSN_END_DT END
                AS FLD_ASSN_END_DT
            ),
            level_2_supervision_location_external_id AS REGION,
            ROW_NUMBER() OVER (
                PARTITION BY DOC, CYC
                ORDER BY
                    FLD_ASSN_BEG_DT,
                    FLD_ASSN_END_DT
            ) AS FIELD_ASSIGNMENT_SEQ_NUM
        FROM field_assignments_with_unique_date_spans
        LEFT OUTER JOIN
            {{RECIDIVIZ_REFERENCE_supervision_district_to_region}}
        ON
            LOC_ACRO = level_1_supervision_location_external_id
    ),
    field_assignments_with_valid_region AS (
        SELECT *
        FROM augmented_field_assignments
        WHERE REGION != 'UNCLASSIFIED_REGION'
    ),
    status_bw AS (
        SELECT
            *
        FROM
            {{LBAKRDTA_TAK026}}
        WHERE
            BW_SCD IS NOT NULL
            AND BW_SCD != ''
    ),
    non_inv_start_status_codes AS (
        SELECT
            BW_DOC,
            BW_CYC,
            BW_SY,
            BW_SSO,
            BW_SCD,
            ROW_NUMBER() OVER (PARTITION BY BW_DOC, BW_CYC ORDER BY BW_SY, BW_SCD, CAST(BW_SSO AS INT64)) 
                AS START_STATUS_RANK
        FROM status_bw
        WHERE (
            (
                BW_SCD LIKE '%I%' -- Start statuses (I = IN)
                AND BW_SCD NOT LIKE '05I5%' -- Invesigation start statuses
                AND BW_SCD NOT LIKE '25I5%' -- Investigation - Additional Charge statuses
                AND BW_SCD NOT LIKE '35I5%' -- Investigation Revisit statuses
            ) OR
            BW_SCD IN (
                '10L6000', -- New CC Fed/State (Papers Only)
                '20L6000', -- CC Fed/State (Papers Only)-AC
                '30L6000'  -- CC Fed/State(Papers Only)-Revt
            )
            OR (
                BW_SCD IN (
                    -- (In very old cases in the 1980s, this is used as the first code to indicate entering probation)
                    '40O9010'  -- Release to Probation
                )
                AND BW_SSO = '1'
            )

        )  AND BW_SCD NOT IN (
            '15I3000', -- New PreTrial Bond Supervision
            '25I3000', -- PreTrial Bond Supv-Addl Charge
            '35I3000'  -- PreTrial Bond Supv-Revisit
        )
    ),
    first_non_inv_start_status_code AS (
        SELECT
            BW_DOC AS DOC,
            BW_CYC AS CYC,
            BW_SSO AS SSO,
            BW_SCD AS SCD,
            BW_SY AS STATUS_CODE_CHG_DT
        FROM non_inv_start_status_codes
        WHERE START_STATUS_RANK = 1
    ),
    supv_period_partition_statuses AS (
        SELECT
            BW_DOC AS DOC,
            BW_CYC AS CYC,
            BW_SSO AS SSO,
            BW_SCD AS SCD,
            BW_SY AS STATUS_CODE_CHG_DT
        FROM
            status_bw
         WHERE (
            BW_SCD IN (
                -- Declared Absconder
                '65O1010', '65L9100',
                -- Offender re-engaged
                '65N9500'
            )
        )
        UNION DISTINCT
        (SELECT * FROM first_non_inv_start_status_code)
    ),
    all_supv_period_critical_dates AS (
        SELECT
            DOC,
            CYC,
            FIELD_ASSIGNMENT_SEQ_NUM,
            STATUS_SEQ_NUM,
            STATUS_CODE,
            CHANGE_DATE,
            DATE_TYPE,
            ROW_NUMBER() OVER (
                PARTITION BY DOC, CYC, FIELD_ASSIGNMENT_SEQ_NUM
                ORDER BY
                    /* Order open edges, then partition edges, then close edges */
                    DATE_TYPE,
                    /* Orders edges by date (open edges and close edges will
                       already come first and last, respectively */
                    CHANGE_DATE,
                    /* Within partition statuses that happen on the same day,
                       order by the status SSO number */
                    CAST(STATUS_SEQ_NUM AS INT64) ASC
            ) AS SUB_PERIOD_SEQ
        FROM (
            -- Field assignment open dates
            SELECT
                DOC,
                CYC,
                FIELD_ASSIGNMENT_SEQ_NUM,
                '0' AS STATUS_SEQ_NUM,
                '' AS STATUS_CODE,
                field_assignments_with_valid_region.FLD_ASSN_BEG_DT AS CHANGE_DATE,
                '1-OPEN' AS DATE_TYPE
            FROM
                field_assignments_with_valid_region
            UNION DISTINCT
            -- Supervision period partition status change dates
            SELECT
                field_assignments_with_valid_region.DOC AS DOC,
                field_assignments_with_valid_region.CYC AS CYC,
                field_assignments_with_valid_region.FIELD_ASSIGNMENT_SEQ_NUM AS FIELD_ASSIGNMENT_SEQ_NUM,
                supv_period_partition_statuses.SSO AS STATUS_SEQ_NUM,
                supv_period_partition_statuses.SCD AS STATUS_CODE,
                supv_period_partition_statuses.STATUS_CODE_CHG_DT AS CHANGE_DATE,
                '2-PARTITION' AS DATE_TYPE

            FROM
                field_assignments_with_valid_region
            LEFT OUTER JOIN
                supv_period_partition_statuses
            ON
                field_assignments_with_valid_region.CE_DOC =
                    supv_period_partition_statuses.DOC AND
                field_assignments_with_valid_region.CE_CYC =
                    supv_period_partition_statuses.CYC AND
                field_assignments_with_valid_region.FLD_ASSN_BEG_DT <
                    supv_period_partition_statuses.STATUS_CODE_CHG_DT AND
                (supv_period_partition_statuses.STATUS_CODE_CHG_DT <
                    field_assignments_with_valid_region.FLD_ASSN_END_DT
                    OR field_assignments_with_valid_region.FLD_ASSN_END_DT = '0'
                )
            WHERE supv_period_partition_statuses.DOC IS NOT NULL
            UNION DISTINCT
            -- Field assignment close dates
            SELECT
                DOC,
                CYC,
                FIELD_ASSIGNMENT_SEQ_NUM,
                '0' AS STATUS_SEQ_NUM,
                '' AS STATUS_CODE,
                field_assignments_with_valid_region.FLD_ASSN_END_DT AS CHANGE_DATE,
                '3-CLOSE' AS DATE_TYPE
            FROM
                field_assignments_with_valid_region
        )
    ),
    supv_period_spans AS (
        SELECT
            start_date.DOC, start_date.CYC, start_date.FIELD_ASSIGNMENT_SEQ_NUM,

            start_date.CHANGE_DATE AS SUPV_PERIOD_BEG_DT,
            start_date.STATUS_SEQ_NUM AS START_STATUS_SEQ_NUM,
            start_date.STATUS_CODE AS START_STATUS_CODE,

            end_date.CHANGE_DATE AS SUPV_PERIOD_END_DT,
            end_date.STATUS_SEQ_NUM AS END_STATUS_SEQ_NUM,
            end_date.STATUS_CODE AS END_STATUS_CODE
        FROM
            all_supv_period_critical_dates start_date
        LEFT OUTER JOIN
            all_supv_period_critical_dates end_date
        ON
            start_date.DOC = end_date.DOC AND
            start_date.CYC = end_date.CYC AND
            start_date.FIELD_ASSIGNMENT_SEQ_NUM =
                end_date.FIELD_ASSIGNMENT_SEQ_NUM AND
            start_date.SUB_PERIOD_SEQ = end_date.SUB_PERIOD_SEQ - 1

        /* Filter out rows created by the join which start with a 'CLOSE'
         * status - periods can only start with 'OPEN' or 'PARTITION' statuses
         */
        WHERE start_date.DATE_TYPE != '3-CLOSE'
    ),
    non_investigation_supv_period_spans AS (
        SELECT supv_period_spans.*
        FROM
            first_non_inv_start_status_code
        JOIN
            supv_period_spans
        ON
            first_non_inv_start_status_code.DOC = supv_period_spans.DOC AND
            first_non_inv_start_status_code.CYC = supv_period_spans.CYC AND
            first_non_inv_start_status_code.STATUS_CODE_CHG_DT <= supv_period_spans.SUPV_PERIOD_BEG_DT
    ),
    basic_supervision_periods AS (
        SELECT
            non_investigation_supv_period_spans.DOC,
            non_investigation_supv_period_spans.CYC,
            non_investigation_supv_period_spans.FIELD_ASSIGNMENT_SEQ_NUM,
            non_investigation_supv_period_spans.START_STATUS_SEQ_NUM,
            SUPV_PERIOD_BEG_DT,
            SUPV_PERIOD_END_DT,
            CE_PLN AS LOCATION_ACRONYM,
            CE_PON AS SUPV_OFFICER_ID
        FROM
            non_investigation_supv_period_spans
        LEFT OUTER JOIN
            field_assignments_with_valid_region
        ON
            non_investigation_supv_period_spans.DOC = field_assignments_with_valid_region.DOC AND
            non_investigation_supv_period_spans.CYC = field_assignments_with_valid_region.CYC AND
            non_investigation_supv_period_spans.FIELD_ASSIGNMENT_SEQ_NUM =
                field_assignments_with_valid_region.FIELD_ASSIGNMENT_SEQ_NUM
    ),
    {OFFICER_ROLE_SPANS_FRAGMENT},
    periods_with_officer_info AS (
        -- The officer may have changed roles during the middle of the period -
        -- this picks the most recent role to record
        SELECT *
        FROM (
            SELECT
                basic_supervision_periods.*,
                SUPV_OFFICER_ID AS BDGNO,
                DEPCLS,
                CLSTTL,
                LNAME,
                FNAME,
                MINTL,
                ROW_NUMBER() OVER (
                    PARTITION BY DOC, CYC, FIELD_ASSIGNMENT_SEQ_NUM, START_STATUS_SEQ_NUM
                    ORDER BY ROLE_TIME_RANK DESC
                ) AS OFFICER_ROLE_RECENCY_RANK
            FROM
                basic_supervision_periods
            LEFT OUTER JOIN
                officer_role_spans
            ON
                -- Joins with any role info for that officer that overlaps at
                -- all with this period
                basic_supervision_periods.SUPV_OFFICER_ID = officer_role_spans.BDGNO AND
                (officer_role_spans.START_DATE <= basic_supervision_periods.SUPV_PERIOD_END_DT
                    OR basic_supervision_periods.SUPV_PERIOD_END_DT = '0') AND
                (officer_role_spans.END_DATE = '0'
                    OR officer_role_spans.END_DATE > basic_supervision_periods.SUPV_PERIOD_BEG_DT)
        )
        WHERE OFFICER_ROLE_RECENCY_RANK = 1
    ),
    supervision_case_types AS (
        SELECT
            DOC_ID,
            CYCLE_NO,
            CASE_TYPE_START_DATE,
            CASE WHEN CASE_TYPE_STOP_DATE = '77991231'
                THEN '0' ELSE CASE_TYPE_STOP_DATE
            END AS CASE_TYPE_STOP_DATE,
            SUPERVSN_ENH_TYPE_CD
        FROM (
            SELECT
                DOC_ID,
                CYCLE_NO,
                REGEXP_REPLACE(ACTUAL_START_DT, r'-', '') AS CASE_TYPE_START_DATE,
                REGEXP_REPLACE(ACTUAL_STOP_DT, r'-', '') AS CASE_TYPE_STOP_DATE,
                SUPERVSN_ENH_TYPE_CD
            FROM
                {{OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW}}
            WHERE
                SUPERVSN_ENH_TYPE_CD IN ('DOM', 'ISO', 'DSO', 'DVS', 'SMI')
        )
    ),
    periods_with_officer_and_case_type_info AS (
        SELECT
            DOC,
            CYC,
            FIELD_ASSIGNMENT_SEQ_NUM,
            START_STATUS_SEQ_NUM,
            SUPV_PERIOD_BEG_DT,
            SUPV_PERIOD_END_DT,
            LOCATION_ACRONYM,
            BDGNO,
            CLSTTL,
            DEPCLS,
            LNAME,
            FNAME,
            MINTL,
            STRING_AGG(DISTINCT SUPERVSN_ENH_TYPE_CD, ',' ORDER BY SUPERVSN_ENH_TYPE_CD) AS CASE_TYPE_LIST
        FROM
            periods_with_officer_info
        LEFT OUTER JOIN
            supervision_case_types
        ON
            periods_with_officer_info.DOC = supervision_case_types.DOC_ID AND
            periods_with_officer_info.CYC = supervision_case_types.CYCLE_NO AND
            (supervision_case_types.CASE_TYPE_START_DATE <=
                    periods_with_officer_info.SUPV_PERIOD_END_DT
                OR periods_with_officer_info.SUPV_PERIOD_END_DT = '0') AND
            (supervision_case_types.CASE_TYPE_STOP_DATE = '0'
                OR supervision_case_types.CASE_TYPE_STOP_DATE >
                        periods_with_officer_info.SUPV_PERIOD_BEG_DT)
        GROUP BY
            DOC,
            CYC,
            FIELD_ASSIGNMENT_SEQ_NUM,
            START_STATUS_SEQ_NUM,
            SUPV_PERIOD_BEG_DT,
            SUPV_PERIOD_END_DT,
            LOCATION_ACRONYM,
            BDGNO,
            CLSTTL,
            DEPCLS,
            LNAME,
            FNAME,
            MINTL
    ),
    supervision_type_assessments AS (
        SELECT
            DN_DOC,
            DN_CYC,
            DN_NSN AS SUP_TYPE_SCORE_SEQ_NUM,
            DN_RC AS SUP_TYPE_SCORE_REPORT_DATE,
            DN_PST AS SUP_TYPE
        FROM {{LBAKRDTA_TAK039}}
        WHERE DN_PST IS NOT NULL AND DN_PST != ''
    ),
    supervision_type_with_seq_num AS (
        SELECT
            supervision_type_assessments.*,
            ROW_NUMBER() OVER (PARTITION BY DN_DOC, DN_CYC 
                                    ORDER BY SUP_TYPE_SCORE_REPORT_DATE, SUP_TYPE_SCORE_SEQ_NUM) AS SYNTHETIC_SEQ_NUM
        FROM supervision_type_assessments
    ),
    supervision_type_spans AS (
        SELECT
            first.DN_DOC,
            first.DN_CYC,
            first.SUP_TYPE_SCORE_SEQ_NUM,
            first.SYNTHETIC_SEQ_NUM,
            first.SUP_TYPE_SCORE_REPORT_DATE AS START_DATE,
            next.SUP_TYPE_SCORE_REPORT_DATE AS END_DATE,
            first.SUP_TYPE
        FROM
            supervision_type_with_seq_num first
        LEFT JOIN
            supervision_type_with_seq_num next
        ON
            first.DN_DOC = next.DN_DOC
            AND first.DN_CYC = next.DN_CYC
            AND first.SYNTHETIC_SEQ_NUM = next.SYNTHETIC_SEQ_NUM - 1
    ),
    periods_with_officer_case_type_and_supervision_type_info AS (
        -- Select the most recent supervision type that overlaps with the supervision period
        SELECT
            *
        FROM (
            SELECT
                periods_with_officer_and_case_type_info.*,
                SUP_TYPE,
                ROW_NUMBER() OVER (
                    PARTITION BY DOC, CYC, FIELD_ASSIGNMENT_SEQ_NUM, START_STATUS_SEQ_NUM
                    ORDER BY SYNTHETIC_SEQ_NUM DESC
                ) AS supervision_type_recency_rank
            FROM
                periods_with_officer_and_case_type_info
            LEFT JOIN
                supervision_type_spans
            ON
                -- Joins with any supervision type info for this DOC/CYC that overlaps at
                -- all with this period
                periods_with_officer_and_case_type_info.DOC = supervision_type_spans.DN_DOC
                AND periods_with_officer_and_case_type_info.CYC = supervision_type_spans.DN_CYC
                AND (periods_with_officer_and_case_type_info.SUPV_PERIOD_END_DT >= supervision_type_spans.START_DATE
                        OR periods_with_officer_and_case_type_info.SUPV_PERIOD_END_DT = '0')
                AND (periods_with_officer_and_case_type_info.SUPV_PERIOD_BEG_DT < supervision_type_spans.END_DATE
                        OR supervision_type_spans.END_DATE IS NULL)
            )
        WHERE
            supervision_type_recency_rank = 1
    ),
    statuses_on_days AS (
        SELECT
            BW_DOC AS DOC,
            BW_CYC AS CYC,
            BW_SY AS STATUSES_DATE,
            STRING_AGG(DISTINCT BW_SCD, ',' ORDER BY BW_SCD) AS STATUS_CODE_LIST
        FROM
            status_bw
        GROUP BY BW_DOC, BW_CYC, BW_SY
    )
    SELECT
        periods_with_officer_case_type_and_supervision_type_info.DOC,
        periods_with_officer_case_type_and_supervision_type_info.CYC,
        ROW_NUMBER() OVER (
            PARTITION BY 
                periods_with_officer_case_type_and_supervision_type_info.DOC,
                periods_with_officer_case_type_and_supervision_type_info.CYC
            ORDER BY 
                periods_with_officer_case_type_and_supervision_type_info.SUPV_PERIOD_BEG_DT, 
                periods_with_officer_case_type_and_supervision_type_info.SUPV_PERIOD_END_DT, 
                periods_with_officer_case_type_and_supervision_type_info.FIELD_ASSIGNMENT_SEQ_NUM
        ) AS FIELD_ASSIGNMENT_SEQ_NUM,
        periods_with_officer_case_type_and_supervision_type_info.START_STATUS_SEQ_NUM,
        periods_with_officer_case_type_and_supervision_type_info.SUPV_PERIOD_BEG_DT,
        start_statuses.STATUS_CODE_LIST AS START_STATUS_CODE_LIST,
        periods_with_officer_case_type_and_supervision_type_info.SUPV_PERIOD_END_DT,
        end_statuses.STATUS_CODE_LIST AS END_STATUS_CODE_LIST,
        periods_with_officer_case_type_and_supervision_type_info.LOCATION_ACRONYM,
        periods_with_officer_case_type_and_supervision_type_info.CASE_TYPE_LIST,
        periods_with_officer_case_type_and_supervision_type_info.BDGNO,
        periods_with_officer_case_type_and_supervision_type_info.CLSTTL,
        periods_with_officer_case_type_and_supervision_type_info.DEPCLS,
        periods_with_officer_case_type_and_supervision_type_info.LNAME,
        periods_with_officer_case_type_and_supervision_type_info.FNAME,
        periods_with_officer_case_type_and_supervision_type_info.MINTL,
        periods_with_officer_case_type_and_supervision_type_info.SUP_TYPE
    FROM
        periods_with_officer_case_type_and_supervision_type_info
    LEFT OUTER JOIN
        statuses_on_days start_statuses
    ON
        periods_with_officer_case_type_and_supervision_type_info.DOC =  start_statuses.DOC AND
        periods_with_officer_case_type_and_supervision_type_info.CYC =  start_statuses.CYC AND
        periods_with_officer_case_type_and_supervision_type_info.SUPV_PERIOD_BEG_DT =
            start_statuses.STATUSES_DATE
    LEFT OUTER JOIN
        statuses_on_days end_statuses
    ON
        periods_with_officer_case_type_and_supervision_type_info.DOC =  end_statuses.DOC AND
        periods_with_officer_case_type_and_supervision_type_info.CYC =  end_statuses.CYC AND
        periods_with_officer_case_type_and_supervision_type_info.SUPV_PERIOD_END_DT =
            end_statuses.STATUSES_DATE
    """

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mo",
    ingest_view_name="tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="DOC, CYC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
