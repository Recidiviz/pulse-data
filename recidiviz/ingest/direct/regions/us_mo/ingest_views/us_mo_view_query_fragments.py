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

"""Shared helper fragments for the US_MO ingest view queries."""

INCARCERATION_SUB_SUBCYCLE_SPANS_FRAGMENT = \
    """
    status_bw AS (
        SELECT
            *
        FROM
            {LBAKRDTA_TAK026}
        WHERE
            BW_SCD IS NOT NULL
            AND BW_SCD != ''
        ),
    statuses_by_sentence AS (
        SELECT
            *
        FROM
            {LBAKRDTA_TAK025} status_xref_bv
        LEFT OUTER JOIN
            status_bw
        ON
            status_xref_bv.BV_DOC = status_bw.BW_DOC AND
            status_xref_bv.BV_CYC = status_bw.BW_CYC AND
            status_xref_bv.BV_SSO = status_bw.BW_SSO
    ),
    board_holdover_parole_revocation_partition_statuses AS (
        SELECT
            BW_DOC AS DOC,
            BW_CYC AS CYC,
            MIN(BW_SSO) AS SSO,
            BV_SEO AS SEO,
            -- When the parole update happens there might be multiple related
            -- statuses on the same day (multiple updates), but they all should
            -- correspond to the same revocation edge so I group them and pick
            -- one (doesn't matter which one since they'll all get mapped to the
            -- same enum).
            MIN(BW_SCD) AS SCD,
            BW_SY AS STATUS_CODE_CHG_DT,
            'I' AS SUBCYCLE_TYPE_STATUS_CAN_PARTITION
        FROM
            statuses_by_sentence
         WHERE (
            BW_SCD LIKE '50N10%' OR -- Parole Update statuses
            BW_SCD LIKE '50N30%' -- Conditional Release Update statuses
        )
        GROUP BY BW_DOC, BW_CYC, BV_SEO, BW_SY
    ),
    sub_cycle_partition_statuses AS (
        SELECT
            DOC,
            CYC,
            SSO,
            SEO,
            SCD,
            STATUS_CODE_CHG_DT,
            SUBCYCLE_TYPE_STATUS_CAN_PARTITION
        FROM
            board_holdover_parole_revocation_partition_statuses
        -- NOTE: Add more subcycle partition status unions as needed here
    ),
    subcycle_partition_status_change_dates AS (
        SELECT
            sub_cycle_partition_statuses.DOC AS DOC,
            sub_cycle_partition_statuses.CYC AS CYC,
            body_status_f1.F1_SQN AS SQN,
            sub_cycle_partition_statuses.SSO AS STATUS_SEQ_NUM,
            sub_cycle_partition_statuses.SCD AS STATUS_CODE,
            '' AS STATUS_SUBTYPE,
            sub_cycle_partition_statuses.STATUS_CODE_CHG_DT AS STATUS_CODE_CHG_DT,
            '2-PARTITION' AS SUBCYCLE_DATE_TYPE

        FROM
            {LBAKRDTA_TAK158} body_status_f1
        LEFT OUTER JOIN
            sub_cycle_partition_statuses
        ON
            body_status_f1.F1_DOC = sub_cycle_partition_statuses.DOC AND
            body_status_f1.F1_CYC = sub_cycle_partition_statuses.CYC AND
            body_status_f1.F1_SEO = sub_cycle_partition_statuses.SEO AND
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
            '0' AS STATUS_SEQ_NUM,
            F1_ORC AS STATUS_CODE,
            F1_OPT AS STATUS_SUBTYPE,
            F1_CD AS STATUS_CODE_CHG_DT,
            '1-OPEN' AS SUBCYCLE_DATE_TYPE
        FROM
            {LBAKRDTA_TAK158} body_status_f1
    ),
    subcycle_close_status_change_dates AS (
        SELECT
            F1_DOC AS DOC,
            F1_CYC AS CYC,
            F1_SQN AS SQN,
            '0' AS STATUS_SEQ_NUM,
            F1_CTP AS STATUS_CODE,
            F1_ARC AS STATUS_SUBTYPE,
            F1_WW AS STATUS_CODE_CHG_DT,
            '3-CLOSE' AS SUBCYCLE_DATE_TYPE
        FROM
            {LBAKRDTA_TAK158} body_status_f1
    ),
    all_sub_sub_cycle_critical_dates AS (
        SELECT
            DOC, CYC, SQN, STATUS_SEQ_NUM, STATUS_CODE, STATUS_SUBTYPE, STATUS_CODE_CHG_DT, SUBCYCLE_DATE_TYPE,
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
        )
    ),
    sub_subcycle_spans AS (
        SELECT
            start_date.DOC, start_date.CYC, start_date.SQN,
            start_date.STATUS_CODE_CHG_DT AS SUB_SUBCYCLE_START_DT,
            start_date.STATUS_SEQ_NUM AS START_STATUS_SEQ_NUM,
            start_date.STATUS_CODE AS START_STATUS_CODE,
            start_date.STATUS_SUBTYPE AS START_STATUS_SUBTYPE,
            end_date.STATUS_CODE_CHG_DT AS SUB_SUBCYCLE_END_DT,
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
    )
    """

STATUSES_BY_DATE_FRAGMENT = \
    """
    all_scd_codes_by_date AS (
        -- All SCD status codes grouped by DOC, CYC, and SY (Date).
        SELECT
            BW_DOC,
            BW_CYC,
            BW_SY AS STATUS_DATE,
            STRING_AGG(BW_SCD, ',') AS STATUS_CODES
        FROM
            status_bw
        GROUP BY BW_DOC, BW_CYC, BW_SY
    )
    """
