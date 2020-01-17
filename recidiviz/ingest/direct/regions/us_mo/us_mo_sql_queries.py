# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""The queries below can be used to generate the tables of Missouri Department
of Corrections data that we export as CSV files for ingest.
Most of the queries below will include WHERE clauses that filter against columns
usually, but not always, named `XX$DLU` and `XX$DCR`--these stand for "date last
updated" and "date created," respectively. By updating the
`lower_bound_update_date` constant and re-printing the queries, you can filter
the exportable results to only those records which were updated or created since
a certain date. Those dates are in the "JDE Julian format" which is documented
offline.
"""

# pylint: disable=line-too-long, trailing-whitespace


lower_bound_update_date = 0

NON_INVESTIGATION_PROBATION_SENTENCES = \
    """
    non_investigation_prob_sentences_bu AS (
        -- Chooses only probation sentences that are non-investigation (not INV)
        SELECT *
        FROM LBAKRDTA.TAK024 sentence_prob_bu
        WHERE BU$PBT != 'INV'
    )
    """

TAK001_OFFENDER_IDENTIFICATION_QUERY = \
    f"""
    -- tak001_offender_identification

    SELECT * 
    FROM 
        LBAKRDTA.TAK001 offender_identification_ek
    LEFT OUTER JOIN 
        LBAKRDTA.VAK003 dob_view
    ON EK$DOC = dob_view.DOC_ID_DOB
    WHERE
        MAX(EK$DLU, EK$DCR, UPDATE_DT, CREATE_DT) >= {lower_bound_update_date}
    ORDER BY EK$DOC DESC;
    """

TAK040_OFFENDER_CYCLES = \
    f"""
    -- tak040_offender_cycles

    SELECT *
    FROM LBAKRDTA.TAK040
    WHERE
        MAX(DQ$DLU, DQ$DCR) >= {lower_bound_update_date}
    ORDER BY DQ$DOC;
    """

TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_INSTITUTION = \
    f"""
    -- tak022_tak023_tak025_tak026_offender_sentence_institution

    WITH incarceration_status_xref_bv AS (
        /* Chooses only status codes that are associated with incarceration 
        sentences */
        SELECT *
        FROM LBAKRDTA.TAK025 status_xref_bv 
        WHERE BV$FSO = 0
    ),
    incarceration_status_xref_with_dates AS (
        /* Joins status code ids with table containing update date and status 
        code info */
        SELECT *
        FROM 
            incarceration_status_xref_bv
        LEFT OUTER JOIN 
            LBAKRDTA.TAK026 status_bw
        ON
            incarceration_status_xref_bv.BV$DOC = status_bw.BW$DOC AND
            incarceration_status_xref_bv.BV$CYC = status_bw.BW$CYC AND
            incarceration_status_xref_bv.BV$SSO = status_bw.BW$SSO
    ),
    max_update_date_for_sentence AS (
        -- Max status update date for a given incarceration sentence
        SELECT BV$DOC, BV$CYC, BV$SEO, MAX(BW$SY) AS MAX_UPDATE_DATE
        FROM 
            incarceration_status_xref_with_dates
        GROUP BY BV$DOC, BV$CYC, BV$SEO
    ),
    max_status_seq_with_max_update_date AS (
        /* For max dates where there are two updates on the same date, we pick
        the status with the largest sequence number */
        SELECT 
            incarceration_status_xref_with_dates.BV$DOC, 
            incarceration_status_xref_with_dates.BV$CYC, 
            incarceration_status_xref_with_dates.BV$SEO, 
            MAX_UPDATE_DATE, 
            COUNT(*) AS SEQ_ON_SAME_DAY_CNT, 
            MAX(incarceration_status_xref_with_dates.BV$SSO) AS MAX_STATUS_SEQ
        FROM 
            incarceration_status_xref_with_dates
        LEFT OUTER JOIN
            max_update_date_for_sentence
        ON
            incarceration_status_xref_with_dates.BV$DOC = max_update_date_for_sentence.BV$DOC AND
            incarceration_status_xref_with_dates.BV$CYC = max_update_date_for_sentence.BV$CYC AND
            incarceration_status_xref_with_dates.BV$SEO = max_update_date_for_sentence.BV$SEO AND
            incarceration_status_xref_with_dates.BW$SY = max_update_date_for_sentence.MAX_UPDATE_DATE
        WHERE MAX_UPDATE_DATE IS NOT NULL
        GROUP BY 
            incarceration_status_xref_with_dates.BV$DOC, 
            incarceration_status_xref_with_dates.BV$CYC, 
            incarceration_status_xref_with_dates.BV$SEO, 
            MAX_UPDATE_DATE
    ),
    incarceration_sentence_status_explosion AS (
            /* Explosion of all incarceration sentences with one row per status 
            update */
            SELECT *
            FROM 
                LBAKRDTA.TAK022 sentence_bs
            LEFT OUTER JOIN
                LBAKRDTA.TAK023 sentence_inst_bt
            ON 
                sentence_bs.BS$DOC = sentence_inst_bt.BT$DOC AND
                sentence_bs.BS$CYC = sentence_inst_bt.BT$CYC AND
                sentence_bs.BS$SEO = sentence_inst_bt.BT$SEO
            LEFT OUTER JOIN 
                incarceration_status_xref_with_dates
            ON
                sentence_bs.BS$DOC = 
                    incarceration_status_xref_with_dates.BV$DOC AND
                sentence_bs.BS$CYC = 
                    incarceration_status_xref_with_dates.BV$CYC AND 
                sentence_bs.BS$SEO = incarceration_status_xref_with_dates.BV$SEO
            WHERE sentence_inst_bt.BT$DOC IS NOT NULL
    )
    /* Choose the incarceration sentence and status info with max date / status 
    sequence numbers */
    SELECT incarceration_sentence_status_explosion.*
    FROM 
        incarceration_sentence_status_explosion
    LEFT OUTER JOIN
        max_status_seq_with_max_update_date
    ON 
        incarceration_sentence_status_explosion.BS$DOC = 
            max_status_seq_with_max_update_date.BV$DOC AND
        incarceration_sentence_status_explosion.BS$CYC = 
            max_status_seq_with_max_update_date.BV$CYC AND
        incarceration_sentence_status_explosion.BS$SEO = 
            max_status_seq_with_max_update_date.BV$SEO AND
        incarceration_sentence_status_explosion.BW$SSO = MAX_STATUS_SEQ
    WHERE
        MAX_STATUS_SEQ IS NOT NULL
        AND MAX(BS$DLU, BS$DCR, BT$DLU, 
                BT$DCR, BV$DLU, BV$DCR, 
                BW$DLU, BW$DCR) >= {lower_bound_update_date}
    ORDER BY BS$DOC, BS$CYC, BS$SEO;
    """

TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_PROBATION = \
    f"""
    -- tak022_tak024_tak025_tak026_offender_sentence_probation

    WITH probation_status_xref_bv AS (
        /* Chooses only status codes that are associated with 
        supervision sentences */
        SELECT *
        FROM LBAKRDTA.TAK025 status_xref_bv 
        WHERE BV$FSO != 0
    ),
    probation_status_xref_with_dates AS (
        SELECT *
        FROM 
            probation_status_xref_bv
        LEFT OUTER JOIN 
            LBAKRDTA.TAK026 status_bw
        ON
            probation_status_xref_bv.BV$DOC = status_bw.BW$DOC AND
            probation_status_xref_bv.BV$CYC = status_bw.BW$CYC AND
            probation_status_xref_bv.BV$SSO = status_bw.BW$SSO
    ),
    max_update_date_for_sentence AS (
        SELECT BV$DOC, BV$CYC, BV$SEO, MAX(BW$SY) AS MAX_UPDATE_DATE
        FROM 
            probation_status_xref_with_dates
        GROUP BY BV$DOC, BV$CYC, BV$SEO
    ),
    max_status_seq_with_max_update_date AS (
        SELECT 
            probation_status_xref_with_dates.BV$DOC, 
            probation_status_xref_with_dates.BV$CYC, 
            probation_status_xref_with_dates.BV$SEO, 
            MAX_UPDATE_DATE, 
            MAX(probation_status_xref_with_dates.BV$SSO) AS MAX_STATUS_SEQ
        FROM 
            probation_status_xref_with_dates
        LEFT OUTER JOIN
            max_update_date_for_sentence
        ON
            probation_status_xref_with_dates.BV$DOC = max_update_date_for_sentence.BV$DOC AND
            probation_status_xref_with_dates.BV$CYC = max_update_date_for_sentence.BV$CYC AND
            probation_status_xref_with_dates.BV$SEO = max_update_date_for_sentence.BV$SEO AND
            probation_status_xref_with_dates.BW$SY = max_update_date_for_sentence.MAX_UPDATE_DATE
        WHERE MAX_UPDATE_DATE IS NOT NULL
        GROUP BY 
            probation_status_xref_with_dates.BV$DOC, 
            probation_status_xref_with_dates.BV$CYC, 
            probation_status_xref_with_dates.BV$SEO, 
            MAX_UPDATE_DATE
    ),
    {NON_INVESTIGATION_PROBATION_SENTENCES},
    probation_sentence_status_explosion AS (
            SELECT *
            FROM 
                LBAKRDTA.TAK022 sentence_bs
            LEFT OUTER JOIN
                non_investigation_prob_sentences_bu
            ON 
                sentence_bs.BS$DOC = non_investigation_prob_sentences_bu.BU$DOC AND
                sentence_bs.BS$CYC = non_investigation_prob_sentences_bu.BU$CYC AND
                sentence_bs.BS$SEO = non_investigation_prob_sentences_bu.BU$SEO
            LEFT OUTER JOIN 
                probation_status_xref_with_dates
            ON
                sentence_bs.BS$DOC = probation_status_xref_with_dates.BV$DOC AND
                sentence_bs.BS$CYC = probation_status_xref_with_dates.BV$CYC AND 
                sentence_bs.BS$SEO = probation_status_xref_with_dates.BV$SEO AND
                non_investigation_prob_sentences_bu.BU$FSO = probation_status_xref_with_dates.BV$FSO
            WHERE non_investigation_prob_sentences_bu.BU$DOC IS NOT NULL
    ),
    last_updated_field_seq AS (
        SELECT 
            probation_sentence_status_explosion.BS$DOC,
            probation_sentence_status_explosion.BS$CYC,
            probation_sentence_status_explosion.BS$SEO,
            MAX_STATUS_SEQ,
            MAX(probation_sentence_status_explosion.BU$FSO) AS MAX_UPDATED_FSO
        FROM 
            probation_sentence_status_explosion
        LEFT OUTER JOIN
            max_status_seq_with_max_update_date
        ON 
            probation_sentence_status_explosion.BS$DOC = max_status_seq_with_max_update_date.BV$DOC AND
            probation_sentence_status_explosion.BS$CYC = max_status_seq_with_max_update_date.BV$CYC AND
            probation_sentence_status_explosion.BS$SEO = max_status_seq_with_max_update_date.BV$SEO AND
            probation_sentence_status_explosion.BW$SSO = MAX_STATUS_SEQ
        WHERE MAX_STATUS_SEQ IS NOT NULL
        GROUP BY         
            probation_sentence_status_explosion.BS$DOC,
            probation_sentence_status_explosion.BS$CYC,
            probation_sentence_status_explosion.BS$SEO,
            MAX_STATUS_SEQ
    )
    SELECT probation_sentence_status_explosion.*
    FROM 
        probation_sentence_status_explosion
    LEFT OUTER JOIN
        last_updated_field_seq
    ON 
        probation_sentence_status_explosion.BS$DOC = last_updated_field_seq.BS$DOC AND
        probation_sentence_status_explosion.BS$CYC = last_updated_field_seq.BS$CYC AND
        probation_sentence_status_explosion.BS$SEO = last_updated_field_seq.BS$SEO AND
        probation_sentence_status_explosion.BW$SSO = MAX_STATUS_SEQ AND
        probation_sentence_status_explosion.BU$FSO = MAX_UPDATED_FSO
    WHERE
        MAX_STATUS_SEQ IS NOT NULL AND MAX_UPDATED_FSO IS NOT NULL
        AND MAX(BS$DLU, BS$DCR, BU$DLU, 
                BU$DCR, BV$DLU, BV$DCR, 
                BW$DLU, BW$DCR) >= {lower_bound_update_date}
    ORDER BY BS$DOC, BS$CYC, BS$SEO;
    """

# TODO(2649) - Finalize the list of Board holdover related releases below and
#  create enum mappings.
SUB_SUBCYCLE_SPANS_FRAGMENT = \
    """
    status_bw AS (
        SELECT 
            * 
        FROM
            LBAKRDTA.TAK026
        WHERE 
            BW$SCD IS NOT NULL
            AND BW$SCD != ''
        ),
    statuses_by_sentence AS (
        SELECT 
            *
        FROM 
            LBAKRDTA.TAK025 status_xref_bv
        LEFT OUTER JOIN 
            status_bw
        ON
            status_xref_bv.BV$DOC = status_bw.BW$DOC AND
            status_xref_bv.BV$CYC = status_bw.BW$CYC AND
            status_xref_bv.BV$SSO = status_bw.BW$SSO
    ),
    absconsion_subcycle_partition_statuses AS (
        SELECT
            BW$DOC AS DOC,
            BW$CYC AS CYC,
            BW$SSO AS SSO,
            BV$SEO AS SEO,
            BW$SCD AS SCD,
            BW$SY AS STATUS_CODE_CHG_DT,
            'F' AS SUBCYCLE_TYPE_STATUS_CAN_PARTITION
        FROM
            statuses_by_sentence
         WHERE (
            BW$SCD IN (
                -- Declared Absconder
                '65O1010', '65O1020', '65O1030', '99O2035', '65L9100',
                -- Offender re-engaged
                '65N9500'
            )
        )
    ),
    board_holdover_parole_revocation_partition_statuses AS (
        SELECT
            BW$DOC AS DOC,
            BW$CYC AS CYC,
            MIN(BW$SSO) AS SSO,
            BV$SEO AS SEO,
            -- When the parole update happens there might be multiple related 
            -- statuses on the same day (multiple updates), but they all should 
            -- correspond to the same revocation edge so I group them and pick 
            -- one (doesn't matter which one since they'll all get mapped to the
            -- same enum).
            MIN(BW$SCD) AS SCD,
            BW$SY AS STATUS_CODE_CHG_DT,
            'I' AS SUBCYCLE_TYPE_STATUS_CAN_PARTITION
        FROM
            statuses_by_sentence
         WHERE (
            BW$SCD LIKE '50N10%' OR -- Parole Update statuses
            BW$SCD LIKE '50N30%' -- Conditional Release Update statuses
        )
        GROUP BY BW$DOC, BW$CYC, BV$SEO, BW$SY
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
            absconsion_subcycle_partition_statuses
        UNION
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
            body_status_f1.F1$SQN AS SQN,
            sub_cycle_partition_statuses.SSO AS STATUS_SEQ_NUM,
            sub_cycle_partition_statuses.SCD AS STATUS_CODE,
            '' AS STATUS_SUBTYPE,
            sub_cycle_partition_statuses.STATUS_CODE_CHG_DT AS STATUS_CODE_CHG_DT,
            '2-PARTITION' AS SUBCYCLE_DATE_TYPE

        FROM
            LBAKRDTA.TAK158 body_status_f1
        LEFT OUTER JOIN
            sub_cycle_partition_statuses
        ON
            body_status_f1.F1$DOC = sub_cycle_partition_statuses.DOC AND
            body_status_f1.F1$CYC = sub_cycle_partition_statuses.CYC AND
            body_status_f1.F1$SEO = sub_cycle_partition_statuses.SEO AND
            body_status_f1.F1$SST = sub_cycle_partition_statuses.SUBCYCLE_TYPE_STATUS_CAN_PARTITION AND
            body_status_f1.F1$CD < sub_cycle_partition_statuses.STATUS_CODE_CHG_DT AND
            sub_cycle_partition_statuses.STATUS_CODE_CHG_DT < body_status_f1.F1$WW
        WHERE sub_cycle_partition_statuses.DOC IS NOT NULL
    ),
    subcycle_open_status_change_dates AS (
        SELECT
            F1$DOC AS DOC,
            F1$CYC AS CYC,
            F1$SQN AS SQN,
            0 AS STATUS_SEQ_NUM,
            F1$ORC AS STATUS_CODE,
            F1$OPT AS STATUS_SUBTYPE,
            F1$CD AS STATUS_CODE_CHG_DT,
            '1-OPEN' AS SUBCYCLE_DATE_TYPE
        FROM
            LBAKRDTA.TAK158 body_status_f1
    ),
    subcycle_close_status_change_dates AS (
        SELECT
            F1$DOC AS DOC,
            F1$CYC AS CYC,
            F1$SQN AS SQN,
            0 AS STATUS_SEQ_NUM,
            F1$CTP AS STATUS_CODE,
            F1$ARC AS STATUS_SUBTYPE,
            F1$WW AS STATUS_CODE_CHG_DT,
            '3-CLOSE' AS SUBCYCLE_DATE_TYPE
        FROM
            LBAKRDTA.TAK158 body_status_f1
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
             UNION
            SELECT * FROM subcycle_partition_status_change_dates
             UNION
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

STATUSES_BY_SENTENCE_AND_DATE_FRAGMENT = \
    """
    all_scd_codes_by_date AS (
        -- All SCD status codes grouped by DOC, CYC, SEO and SY (Date).
        -- Note about joining this with TAK158 (body status): Because we're 
        -- grouping by SEO, we're excluding any statuses that are not associated
        -- with the sentence arbitrarily picked by the body status table.
        SELECT 
            BV$DOC, 
            BV$CYC, 
            BV$SEO, 
            BW$SY AS STATUS_DATE, 
            LISTAGG(BW$SCD, ',') AS STATUS_CODES
        FROM
            statuses_by_sentence
        GROUP BY BV$DOC, BV$CYC, BV$SEO, BW$SY
    )
    """

TAK158_TAK023_TAK026_INCARCERATION_PERIOD_FROM_INCARCERATION_SENTENCE = \
    f"""
    -- tak158_tak023_tak026_incarceration_period_from_incarceration_sentence
    WITH {SUB_SUBCYCLE_SPANS_FRAGMENT},
    {STATUSES_BY_SENTENCE_AND_DATE_FRAGMENT},
    incarceration_subcycle_from_incarceration_sentence AS (
        SELECT 
            sentence_inst_ids.BT$DOC, 
            sentence_inst_ids.BT$CYC, 
            sentence_inst_ids.BT$SEO, 
            body_status_f1.*
        FROM (
            SELECT BT$DOC, BT$CYC, BT$SEO
            FROM LBAKRDTA.TAK023 sentence_inst_bt
            GROUP BY BT$DOC, BT$CYC, BT$SEO
        ) sentence_inst_ids
        LEFT OUTER JOIN
            LBAKRDTA.TAK158 body_status_f1
        ON
            sentence_inst_ids.BT$DOC = body_status_f1.F1$DOC AND
            sentence_inst_ids.BT$CYC = body_status_f1.F1$CYC AND
            sentence_inst_ids.BT$SEO = body_status_f1.F1$SEO
        WHERE body_status_f1.F1$DOC IS NOT NULL 
            AND body_status_f1.F1$SST = 'I'
    ),
    incarceration_periods_from_incarceration_sentence AS (
        SELECT *
        FROM 
            incarceration_subcycle_from_incarceration_sentence
        LEFT OUTER JOIN
            sub_subcycle_spans
        ON
            incarceration_subcycle_from_incarceration_sentence.F1$DOC = sub_subcycle_spans.DOC AND
            incarceration_subcycle_from_incarceration_sentence.F1$CYC = sub_subcycle_spans.CYC AND
            incarceration_subcycle_from_incarceration_sentence.F1$SQN = sub_subcycle_spans.SQN
        )
    SELECT 
        incarceration_periods_from_incarceration_sentence.*, 
        start_codes.STATUS_CODES AS START_SCD_CODES, 
        end_codes.STATUS_CODES AS END_SCD_CODES
    FROM 
        incarceration_periods_from_incarceration_sentence
    LEFT OUTER JOIN 
        all_scd_codes_by_date start_codes
    ON
        incarceration_periods_from_incarceration_sentence.F1$DOC = start_codes.BV$DOC AND
        incarceration_periods_from_incarceration_sentence.F1$CYC = start_codes.BV$CYC AND
        incarceration_periods_from_incarceration_sentence.F1$SEO = start_codes.BV$SEO AND
        incarceration_periods_from_incarceration_sentence.SUB_SUBCYCLE_START_DT = start_codes.STATUS_DATE
    LEFT OUTER JOIN 
        all_scd_codes_by_date end_codes
    ON
        incarceration_periods_from_incarceration_sentence.F1$DOC = end_codes.BV$DOC AND
        incarceration_periods_from_incarceration_sentence.F1$CYC = end_codes.BV$CYC AND
        incarceration_periods_from_incarceration_sentence.F1$SEO = end_codes.BV$SEO AND
        incarceration_periods_from_incarceration_sentence.SUB_SUBCYCLE_END_DT = end_codes.STATUS_DATE
    ORDER BY BT$DOC, BT$CYC, BT$SEO, F1$SQN;
    """

TAK158_TAK023_TAK026_SUPERVISION_PERIOD_FROM_INCARCERATION_SENTENCE = \
    f"""
    -- tak158_tak023_tak026_supervision_period_from_incarceration_sentence

    WITH {SUB_SUBCYCLE_SPANS_FRAGMENT},
    {STATUSES_BY_SENTENCE_AND_DATE_FRAGMENT},
    supervision_subcycle_from_incarceration_sentence AS (
        SELECT 
            sentence_inst_ids.BT$DOC, 
            sentence_inst_ids.BT$CYC, 
            sentence_inst_ids.BT$SEO, 
            body_status_f1.*
        FROM (
            SELECT BT$DOC, BT$CYC, BT$SEO
            FROM LBAKRDTA.TAK023 sentence_inst_bt
            GROUP BY BT$DOC, BT$CYC, BT$SEO
        ) sentence_inst_ids
        LEFT OUTER JOIN
            LBAKRDTA.TAK158 body_status_f1
        ON
            sentence_inst_ids.BT$DOC = body_status_f1.F1$DOC AND
            sentence_inst_ids.BT$CYC = body_status_f1.F1$CYC AND
            sentence_inst_ids.BT$SEO = body_status_f1.F1$SEO
        WHERE body_status_f1.F1$DOC IS NOT NULL AND body_status_f1.F1$SST = 'F'
    ),
    supervision_periods_from_incarceration_sentences AS (
        SELECT *
        FROM 
            supervision_subcycle_from_incarceration_sentence
        LEFT OUTER JOIN
            sub_subcycle_spans
        ON
            supervision_subcycle_from_incarceration_sentence.F1$DOC = sub_subcycle_spans.DOC AND
            supervision_subcycle_from_incarceration_sentence.F1$CYC = sub_subcycle_spans.CYC AND
            supervision_subcycle_from_incarceration_sentence.F1$SQN = sub_subcycle_spans.SQN
        )
    SELECT 
        supervision_periods_from_incarceration_sentences.*, 
        start_codes.STATUS_CODES AS START_SCD_CODES, 
        end_codes.STATUS_CODES AS END_SCD_CODES
    FROM 
        supervision_periods_from_incarceration_sentences
    LEFT OUTER JOIN 
        all_scd_codes_by_date start_codes
    ON
        supervision_periods_from_incarceration_sentences.F1$DOC = start_codes.BV$DOC AND
        supervision_periods_from_incarceration_sentences.F1$CYC = start_codes.BV$CYC AND
        supervision_periods_from_incarceration_sentences.F1$SEO = start_codes.BV$SEO AND
        supervision_periods_from_incarceration_sentences.SUB_SUBCYCLE_START_DT = start_codes.STATUS_DATE
    LEFT OUTER JOIN 
        all_scd_codes_by_date end_codes
    ON
        supervision_periods_from_incarceration_sentences.F1$DOC = end_codes.BV$DOC AND
        supervision_periods_from_incarceration_sentences.F1$CYC = end_codes.BV$CYC AND
        supervision_periods_from_incarceration_sentences.F1$SEO = end_codes.BV$SEO AND
        supervision_periods_from_incarceration_sentences.SUB_SUBCYCLE_END_DT = end_codes.STATUS_DATE
    ORDER BY BT$DOC, BT$CYC, BT$SEO, F1$SQN;
    """

TAK158_TAK024_TAK026_INCARCERATION_PERIOD_FROM_SUPERVISION_SENTENCE = \
    f"""
    -- tak158_tak024_tak026_incarceration_period_from_supervision_sentence

    WITH {SUB_SUBCYCLE_SPANS_FRAGMENT},
    {NON_INVESTIGATION_PROBATION_SENTENCES},
    {STATUSES_BY_SENTENCE_AND_DATE_FRAGMENT},
    incarceration_subcycle_from_supervision_sentence AS (
        SELECT 
            non_investigation_probation_sentence_ids.BU$DOC, 
            non_investigation_probation_sentence_ids.BU$CYC, 
            non_investigation_probation_sentence_ids.BU$SEO, 
            body_status_f1.*
        FROM (
            SELECT BU$DOC, BU$CYC, BU$SEO
            FROM non_investigation_prob_sentences_bu
            GROUP BY BU$DOC, BU$CYC, BU$SEO
        ) non_investigation_probation_sentence_ids
        LEFT OUTER JOIN
            LBAKRDTA.TAK158 body_status_f1
        ON
            non_investigation_probation_sentence_ids.BU$DOC = body_status_f1.F1$DOC AND
            non_investigation_probation_sentence_ids.BU$CYC = body_status_f1.F1$CYC AND
            non_investigation_probation_sentence_ids.BU$SEO = body_status_f1.F1$SEO
        WHERE body_status_f1.F1$DOC IS NOT NULL AND body_status_f1.F1$SST = 'I'    
    ),
    incarceration_periods_from_supervision_sentence AS (
        SELECT *
        FROM 
            incarceration_subcycle_from_supervision_sentence
        LEFT OUTER JOIN
            sub_subcycle_spans
        ON
            incarceration_subcycle_from_supervision_sentence.F1$DOC = sub_subcycle_spans.DOC AND
            incarceration_subcycle_from_supervision_sentence.F1$CYC = sub_subcycle_spans.CYC AND
            incarceration_subcycle_from_supervision_sentence.F1$SQN = sub_subcycle_spans.SQN
        )
    SELECT 
        incarceration_periods_from_supervision_sentence.*, 
        start_codes.STATUS_CODES AS START_SCD_CODES, 
        end_codes.STATUS_CODES AS END_SCD_CODES
    FROM 
        incarceration_periods_from_supervision_sentence
    LEFT OUTER JOIN 
        all_scd_codes_by_date start_codes
    ON
        incarceration_periods_from_supervision_sentence.F1$DOC = start_codes.BV$DOC AND
        incarceration_periods_from_supervision_sentence.F1$CYC = start_codes.BV$CYC AND
        incarceration_periods_from_supervision_sentence.F1$SEO = start_codes.BV$SEO AND
        incarceration_periods_from_supervision_sentence.SUB_SUBCYCLE_START_DT = start_codes.STATUS_DATE
    LEFT OUTER JOIN 
        all_scd_codes_by_date end_codes
    ON
        incarceration_periods_from_supervision_sentence.F1$DOC = end_codes.BV$DOC AND
        incarceration_periods_from_supervision_sentence.F1$CYC = end_codes.BV$CYC AND
        incarceration_periods_from_supervision_sentence.F1$SEO = end_codes.BV$SEO AND
        incarceration_periods_from_supervision_sentence.SUB_SUBCYCLE_END_DT = end_codes.STATUS_DATE
    ORDER BY BU$DOC, BU$CYC, BU$SEO, F1$SQN;
    """

TAK158_TAK024_TAK026_SUPERVISION_PERIOD_FROM_SUPERVISION_SENTENCE = \
    f"""
    -- tak158_tak024_tak026_supervision_period_from_supervision_sentence

    WITH {SUB_SUBCYCLE_SPANS_FRAGMENT},
    {NON_INVESTIGATION_PROBATION_SENTENCES},
    {STATUSES_BY_SENTENCE_AND_DATE_FRAGMENT},
    supervision_subcycle_from_supervision_sentence AS (
        SELECT 
            non_investigation_probation_sentence_ids.BU$DOC, 
            non_investigation_probation_sentence_ids.BU$CYC, 
            non_investigation_probation_sentence_ids.BU$SEO, 
            body_status_f1.*
        FROM (
            SELECT BU$DOC, BU$CYC, BU$SEO
            FROM non_investigation_prob_sentences_bu
            GROUP BY BU$DOC, BU$CYC, BU$SEO
        ) non_investigation_probation_sentence_ids
        LEFT OUTER JOIN
            LBAKRDTA.TAK158 body_status_f1
        ON
            non_investigation_probation_sentence_ids.BU$DOC = body_status_f1.F1$DOC AND
            non_investigation_probation_sentence_ids.BU$CYC = body_status_f1.F1$CYC AND
            non_investigation_probation_sentence_ids.BU$SEO = body_status_f1.F1$SEO
        WHERE body_status_f1.F1$DOC IS NOT NULL AND body_status_f1.F1$SST = 'F'    
    ),
    supervision_periods_from_supervision_sentence AS (
        SELECT *
        FROM 
            supervision_subcycle_from_supervision_sentence
        LEFT OUTER JOIN
            sub_subcycle_spans
        ON
            supervision_subcycle_from_supervision_sentence.F1$DOC = sub_subcycle_spans.DOC AND
            supervision_subcycle_from_supervision_sentence.F1$CYC = sub_subcycle_spans.CYC AND
            supervision_subcycle_from_supervision_sentence.F1$SQN = sub_subcycle_spans.SQN
    )
    SELECT 
        supervision_periods_from_supervision_sentence.*, 
        start_codes.STATUS_CODES AS START_SCD_CODES, 
        end_codes.STATUS_CODES AS END_SCD_CODES
    FROM 
        supervision_periods_from_supervision_sentence
    LEFT OUTER JOIN 
        all_scd_codes_by_date start_codes
    ON
        supervision_periods_from_supervision_sentence.F1$DOC = start_codes.BV$DOC AND
        supervision_periods_from_supervision_sentence.F1$CYC = start_codes.BV$CYC AND
        supervision_periods_from_supervision_sentence.F1$SEO = start_codes.BV$SEO AND
        supervision_periods_from_supervision_sentence.SUB_SUBCYCLE_START_DT = start_codes.STATUS_DATE
    LEFT OUTER JOIN 
        all_scd_codes_by_date end_codes
    ON
        supervision_periods_from_supervision_sentence.F1$DOC = end_codes.BV$DOC AND
        supervision_periods_from_supervision_sentence.F1$CYC = end_codes.BV$CYC AND
        supervision_periods_from_supervision_sentence.F1$SEO = end_codes.BV$SEO AND
        supervision_periods_from_supervision_sentence.SUB_SUBCYCLE_END_DT = end_codes.STATUS_DATE
    ORDER BY BU$DOC, BU$CYC, BU$SEO, F1$SQN;
    """

OFFICERS_WITH_MOST_RECENT_ROLE_FRAGMENT = \
    f"""
    all_officers AS (
        -- Combination of 2 officer tables into one source of truth. Both tables
        -- contain information about different groups of officers. From 
        -- conversations with MO contacts, we should use a combination of both 
        -- tables to get a full understanding of all officers.
        SELECT 
            officers_1.*	
        FROM 
            LBCMDATA.APFX90 officers_1
        UNION 
        SELECT 
            officers_2.*,
            -- These three columns are present in officers_1 and not in 
            -- officers_2, so we add dummy values just so the tables can 
            -- be combined. 
            0 AS ENDDTE,
            0 AS UPDDTE,
            0 AS UPDTME
        FROM 
            LBCMDATA.APFX91 officers_2
        WHERE BDGNO != ''
    ),
    officers_with_role_recency_ranks AS(
        -- Officers with their roles ranked from most recent to least recent.
        SELECT 
            BDGNO, 
            CLSTTL,
            LNAME,
            FNAME,
            MINTL,
            CRTDTE, 
            ROW_NUMBER() OVER (PARTITION BY BDGNO ORDER BY STRDTE DESC) AS recency_rank 
        FROM 
            all_officers),
    officers_with_recent_role AS (
        -- Officers with their most recent role only
        SELECT 
            BDGNO, 
            CLSTTL,
            LNAME,
            FNAME,
            MINTL,
            CRTDTE  
        FROM 
            officers_with_role_recency_ranks 
        WHERE 
            officers_with_role_recency_ranks.recency_rank = 1
            AND officers_with_role_recency_ranks.CLSTTL != ''
            AND officers_with_role_recency_ranks.CLSTTL IS NOT NULL)
    """

TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT = \
    """
        -- Finally formed documents are ones which are no longer in a draft
        -- state.
        SELECT 
            finally_formed_documents_e6.E6$DOC, 
            finally_formed_documents_e6.E6$CYC, 
            finally_formed_documents_e6.E6$DOS, 
            MAX(finally_formed_documents_e6.E6$DCR) as final_formed_create_date, 
            MAX(finally_formed_documents_e6.E6$DLU) as final_formed_update_date  
        FROM 
            LBAKRDTA.TAK142 finally_formed_documents_e6
        WHERE 
            finally_formed_documents_e6.E6$DON = '{document_type_code}'
        GROUP BY 
            finally_formed_documents_e6.E6$DOC, 
            finally_formed_documents_e6.E6$CYC, 
            finally_formed_documents_e6.E6$DOS"""

FINALLY_FORMED_CITATIONS_E6 = \
    TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT.format(document_type_code='XIT')
FINALLY_FORMED_VIOLATIONS_E6 = \
    TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT.format(document_type_code='XIF')

TAK028_TAK042_TAK076_TAK024_VIOLATION_REPORTS = \
    f"""
    -- tak028_tak042_tak076_tak024_violation_reports

    WITH 
    {NON_INVESTIGATION_PROBATION_SENTENCES},
    {OFFICERS_WITH_MOST_RECENT_ROLE_FRAGMENT},
    conditions_violated_cf AS (
    -- An updated version of TAK042 that only has one row per citation.
        SELECT 
            conditions_cf.CF$DOC, 
            conditions_cf.CF$CYC, 
            conditions_cf.CF$VSN, 
            LISTAGG(conditions_cf.CF$VCV, ',') AS violated_conditions,
            MAX(conditions_cf.CF$DCR) as create_dt,
            MAX(conditions_cf.CF$DLU) as update_dt
        FROM 
            LBAKRDTA.TAK042 AS conditions_cf
        GROUP BY 
            conditions_cf.CF$DOC, 
            conditions_cf.CF$CYC, 
            conditions_cf.CF$VSN
        ORDER BY 
            conditions_cf.CF$DOC, 
            conditions_cf.CF$CYC, 
            conditions_cf.CF$VSN
    ),
    valid_sentences_cz AS (
    -- Only keeps rows in TAK076 which refer to either 
    -- IncarcerationSentences or non-INV SupervisionSentences
        SELECT 
            sentence_xref_with_probation_info_cz_bu.CZ$DOC, 
            sentence_xref_with_probation_info_cz_bu.CZ$CYC, 
            sentence_xref_with_probation_info_cz_bu.CZ$SEO, 
            sentence_xref_with_probation_info_cz_bu.CZ$FSO, 
            sentence_xref_with_probation_info_cz_bu.CZ$VSN,
            sentence_xref_with_probation_info_cz_bu.CZ$DCR,
            sentence_xref_with_probation_info_cz_bu.CZ$DLU
        FROM (
            SELECT 
                * 
            FROM 
                LBAKRDTA.TAK076 sentence_xref_cz
            LEFT JOIN 
                non_investigation_prob_sentences_bu
            ON 
                sentence_xref_cz.CZ$DOC = non_investigation_prob_sentences_bu.BU$DOC
                AND sentence_xref_cz.CZ$CYC = non_investigation_prob_sentences_bu.BU$CYC
                AND sentence_xref_cz.CZ$SEO = non_investigation_prob_sentences_bu.BU$SEO
                AND sentence_xref_cz.CZ$FSO = non_investigation_prob_sentences_bu.BU$FSO
            WHERE sentence_xref_cz.CZ$FSO = 0 OR 
                non_investigation_prob_sentences_bu.BU$DOC IS NOT NULL
        ) sentence_xref_with_probation_info_cz_bu
    ),
    finally_formed_violations_e6 AS(
        -- Finally formed violation reports. As we've filtered for just 
        -- violation reports, DOS in this table is equivalent to VSN in other 
        -- tables.
        {FINALLY_FORMED_VIOLATIONS_E6})
    SELECT 
        * 
    FROM 
        LBAKRDTA.TAK028 violation_reports_by
    LEFT JOIN 
        conditions_violated_cf
    ON 
        violation_reports_by.BY$DOC = conditions_violated_cf.CF$DOC
        AND violation_reports_by.BY$CYC = conditions_violated_cf.CF$CYC
        AND violation_reports_by.BY$VSN = conditions_violated_cf.CF$VSN
    JOIN 
        valid_sentences_cz
    ON 
        violation_reports_by.BY$DOC = valid_sentences_cz.CZ$DOC
        AND violation_reports_by.BY$CYC = valid_sentences_cz.CZ$CYC
        AND violation_reports_by.BY$VSN = valid_sentences_cz.CZ$VSN
    LEFT JOIN
        finally_formed_violations_e6
    ON 
        violation_reports_by.BY$DOC = finally_formed_violations_e6.E6$DOC
        AND violation_reports_by.BY$CYC = finally_formed_violations_e6.E6$CYC
        AND violation_reports_by.BY$VSN = finally_formed_violations_e6.E6$DOS
    LEFT JOIN
        officers_with_recent_role
    ON 
        violation_reports_by.BY$PON = officers_with_recent_role.BDGNO
    WHERE
        MAX(conditions_violated_cf.UPDATE_DT, 
            conditions_violated_cf.CREATE_DT, 
            violation_reports_by.BY$DLU, 
            violation_reports_by.BY$DCR, 
            valid_sentences_cz.CZ$DLU, 
            valid_sentences_cz.CZ$DCR,
            finally_formed_violations_e6.final_formed_create_date,
            finally_formed_violations_e6.final_formed_update_date) >= {lower_bound_update_date}
    ORDER BY BY$DOC, BY$CYC, BY$VSN;
    """

TAK291_TAK292_TAK024_CITATIONS = \
    f"""
    -- tak291_tak292_tak024_citations

    WITH 
    {NON_INVESTIGATION_PROBATION_SENTENCES},
    valid_sentences_js AS (
    -- Only keeps rows in TAK291 which refer to either 
    -- IncarcerationSentences or non-INV SupervisionSentences
        SELECT 
            sentence_xref_with_probation_info_js_bu.JS$DOC, 
            sentence_xref_with_probation_info_js_bu.JS$CYC, 
            sentence_xref_with_probation_info_js_bu.JS$SEO, 
            sentence_xref_with_probation_info_js_bu.JS$FSO, 
            sentence_xref_with_probation_info_js_bu.JS$CSQ,
            sentence_xref_with_probation_info_js_bu.JS$DCR,
            sentence_xref_with_probation_info_js_bu.JS$DLU
        FROM (
            SELECT 
                * 
            FROM 
                LBAKRDTA.TAK291 sentence_xref_js
            LEFT JOIN 
                non_investigation_prob_sentences_bu
            ON 
                sentence_xref_js.JS$DOC = non_investigation_prob_sentences_bu.BU$DOC
                AND sentence_xref_js.JS$CYC = non_investigation_prob_sentences_bu.BU$CYC
                AND sentence_xref_js.JS$SEO = non_investigation_prob_sentences_bu.BU$SEO
                AND sentence_xref_js.JS$FSO = non_investigation_prob_sentences_bu.BU$FSO
            WHERE sentence_xref_js.JS$FSO = 0 OR 
                non_investigation_prob_sentences_bu.BU$DOC IS NOT NULL
        ) sentence_xref_with_probation_info_js_bu
    ),
    citations_with_multiple_violations_jt AS (
    -- An updated version of TAK292 that only has one row per citation.
        SELECT 
            citations_jt.JT$DOC, 
            citations_jt.JT$CYC, 
            citations_jt.JT$CSQ, 
            MAX(citations_jt.JT$VG) AS max_date,
            LISTAGG(citations_jt.JT$VCV, ',') AS violated_conditions,
            MAX(citations_jt.JT$DCR) AS create_dt,
            MAX(citations_jt.JT$DLU) AS update_dt
        FROM 
            LBAKRDTA.TAK292 citations_jt
        GROUP BY 
            citations_jt.JT$DOC, 
            citations_jt.JT$CYC, 
            citations_jt.JT$CSQ 
        ORDER BY 
            citations_jt.JT$DOC, 
            citations_jt.JT$CYC, 
            citations_jt.JT$CSQ 
    ),
    finally_formed_citations_e6 AS(
        -- Finally formed citations. As we've filtered for just citations 
        -- DOS in this table is equivalent to CSQ in other tables.
        {FINALLY_FORMED_CITATIONS_E6})
    SELECT 
        *
    FROM 
        citations_with_multiple_violations_jt
    JOIN 
        valid_sentences_js
    ON 
        citations_with_multiple_violations_jt.JT$DOC = valid_sentences_js.JS$DOC
        AND citations_with_multiple_violations_jt.JT$CYC = valid_sentences_js.JS$CYC
        AND citations_with_multiple_violations_jt.JT$CSQ = valid_sentences_js.JS$CSQ
    LEFT JOIN
        finally_formed_citations_e6
    ON 
        citations_with_multiple_violations_jt.JT$DOC = finally_formed_citations_e6.E6$DOC
        AND citations_with_multiple_violations_jt.JT$CYC = finally_formed_citations_e6.E6$CYC
        AND citations_with_multiple_violations_jt.JT$CSQ = finally_formed_citations_e6.E6$DOS
    WHERE
        MAX(citations_with_multiple_violations_jt.UPDATE_DT, 
            citations_with_multiple_violations_jt.CREATE_DT, 
            valid_sentences_js.JS$DLU, 
            valid_sentences_js.JS$DCR,
            finally_formed_citations_e6.final_formed_create_date,
            finally_formed_citations_e6.final_formed_update_date) >= {lower_bound_update_date}
    ORDER BY JT$DOC, JT$CYC, JT$CSQ;
    """

APFX90_APFX91_TAK034_CURRENT_PO_ASSIGNMENTS = \
    f"""
    -- APFX90_APFX91_TAK034_current_po_assignments
    WITH 
    {OFFICERS_WITH_MOST_RECENT_ROLE_FRAGMENT},
    pnp_officers AS (
        -- Just P&P officer information
        SELECT 
            *
        FROM 
            officers_with_recent_role
        WHERE
            officers_with_recent_role.CLSTTL LIKE '%P&P%'
            OR officers_with_recent_role.CLSTTL LIKE '%P & P%'
            OR (
                officers_with_recent_role.CLSTTL LIKE '%PROBATION%' 
                AND officers_with_recent_role.CLSTTL LIKE '%PAROLE%'
            )
        )
    SELECT 
        *
    FROM 
        LBAKRDTA.TAK034 field_assignments_ce
    JOIN 
        pnp_officers
    ON 
        field_assignments_ce.CE$PON = pnp_officers.BDGNO
    WHERE 
        -- ORD = 1 means the assignment is active
        field_assignments_ce.CE$OR0 = 1
        AND MAX(field_assignments_ce.CE$DCR, field_assignments_ce.CE$DLU) >= {lower_bound_update_date}
    ORDER BY 
        field_assignments_ce.CE$DOC, 
        field_assignments_ce.CE$HF;
"""


ORAS_ASSESSMENTS_WEEKLY = \
    f"""
    SELECT 
        * 
    FROM 
        FOCTEST.ORAS_ASSESSMENTS_WEEKLY
    WHERE
        E15 = 'Complete'
    -- explicitly filter out any test data from UCCI
        AND E08 NOT LIKE '%Test%' 
        AND E08 NOT LIKE '%test%';
    """

if __name__ == '__main__':
    print('\n\n/* TAK001_OFFENDER_IDENTIFICATION_QUERY */\n')
    print(TAK001_OFFENDER_IDENTIFICATION_QUERY)
    print('\n\n/* APFX90_APFX91_TAK034_CURRENT_PO_ASSIGNMENTS */\n')
    print(APFX90_APFX91_TAK034_CURRENT_PO_ASSIGNMENTS)
    print('\n\n/* ORAS_ASSESSMENTS_WEEKLY */\n')
    print(ORAS_ASSESSMENTS_WEEKLY)
    print('\n\n/* TAK040_OFFENDER_CYCLES */\n')
    print(TAK040_OFFENDER_CYCLES)
    print('\n\n/* TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_INSTITUTION */\n')
    print(TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_INSTITUTION)
    print('\n\n/* TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_PROBATION */\n')
    print(TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_PROBATION)
    print('\n\n/* TAK158_TAK023_TAK026_INCARCERATION_PERIOD_FROM_INCARCERATION_SENTENCE */\n')
    print(TAK158_TAK023_TAK026_INCARCERATION_PERIOD_FROM_INCARCERATION_SENTENCE)
    print('\n\n/* TAK158_TAK023_TAK026_SUPERVISION_PERIOD_FROM_INCARCERATION_SENTENCE */\n')
    print(TAK158_TAK023_TAK026_SUPERVISION_PERIOD_FROM_INCARCERATION_SENTENCE)
    print('\n\n/* TAK158_TAK024_TAK026_INCARCERATION_PERIOD_FROM_SUPERVISION_SENTENCE */\n')
    print(TAK158_TAK024_TAK026_INCARCERATION_PERIOD_FROM_SUPERVISION_SENTENCE)
    print('\n\n/* TAK158_TAK024_TAK026_SUPERVISION_PERIOD_FROM_SUPERVISION_SENTENCE */\n')
    print(TAK158_TAK024_TAK026_SUPERVISION_PERIOD_FROM_SUPERVISION_SENTENCE)
    print('\n\n/* TAK028_TAK042_TAK076_TAK024_VIOLATION_REPORTS */\n')
    print(TAK028_TAK042_TAK076_TAK024_VIOLATION_REPORTS)
    print('\n\n/* TAK291_TAK292_TAK024_CITATIONS */\n')
    print(TAK291_TAK292_TAK024_CITATIONS)
