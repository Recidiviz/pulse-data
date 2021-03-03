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

from typing import List, Tuple, Optional

from recidiviz.calculator.query.state.dataset_config import (
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.ingest.direct.query_utils import output_sql_queries


US_MO_TAK025_SENTENCE_STATUS_XREF_QUERY = f"""
/* DO NOT DROP THE RESULT OF THIS QUERY IN THE INGEST BUCKET. INSTEAD, UPLOAD IT TO THE `{STATIC_REFERENCE_TABLES_DATASET}`
DATASETS IN PROD AND STAGING USING THE FOLLOWING COMMANDS:

python -m recidiviz.tools.upload_local_file_to_bq \
    --project-id recidiviz-staging \
    --destination-table {STATIC_REFERENCE_TABLES_DATASET}.us_mo_tak025_sentence_status_xref \
    --local-filepath <path>/<to>/us_mo_tak025_sentence_status_xref.csv \
    --separator , \
    --overwrite-if-exists True \
    --dry-run [True|False]

python -m recidiviz.tools.upload_local_file_to_bq \
    --project-id recidiviz-123 \
    --destination-table {STATIC_REFERENCE_TABLES_DATASET}.us_mo_tak025_sentence_status_xref \
    --local-filepath <path>/<to>/us_mo_tak025_sentence_status_xref.csv \
    --separator , \
    --overwrite-if-exists True \
    --dry-run [True|False]
*/
SELECT * 
FROM LBAKRDTA.TAK025 status_xref_bv
ORDER BY BV$DOC, BV$CYC;
"""

US_MO_TAK026_SENTENCE_STATUS_QUERY = f"""
/* DO NOT DROP THE RESULT OF THIS QUERY IN THE INGEST BUCKET. INSTEAD, UPLOAD IT TO THE `{STATIC_REFERENCE_TABLES_DATASET}`
DATASETS IN PROD AND STAGING USING THE FOLLOWING COMMANDS:

python -m recidiviz.tools.upload_local_file_to_bq \
    --project-id recidiviz-staging \
    --destination-table {STATIC_REFERENCE_TABLES_DATASET}.us_mo_tak026_sentence_status \
    --local-filepath <path>/<to>/us_mo_tak026_sentence_status.csv \
    --separator , \
    --overwrite-if-exists True
    --dry-run [True|False]

python -m recidiviz.tools.upload_local_file_to_bq \
    --project-id recidiviz-123 \
    --destination-table {STATIC_REFERENCE_TABLES_DATASET}.us_mo_tak026_sentence_status \
    --local-filepath <path>/<to>/us_mo_tak026_sentence_status.csv \
    --separator , \
    --overwrite-if-exists True \
    --dry-run [True|False]
*/
SELECT * 
FROM LBAKRDTA.TAK026 status_bw
ORDER BY BW$DOC, BW$CYC;
"""

US_MO_TAK146_STATUS_CODE_DESCRIPTIONS_QUERY = f"""
/* DO NOT DROP THE RESULT OF THIS QUERY IN THE INGEST BUCKET. INSTEAD, UPLOAD IT TO THE `{STATIC_REFERENCE_TABLES_DATASET}`
DATASETS IN PROD AND STAGING USING THE FOLLOWING COMMANDS:

python -m recidiviz.tools.upload_local_file_to_bq \
    --project-id recidiviz-staging \
    --destination-table {STATIC_REFERENCE_TABLES_DATASET}.us_mo_tak146_status_code_descriptions \
    --local-filepath <path>/<to>/us_mo_tak146_status_code_descriptions.csv \
    --separator , \
    --overwrite-if-exists True \
    --dry-run [True|False]

python -m recidiviz.tools.upload_local_file_to_bq \
    --project-id recidiviz-123 \
    --destination-table {STATIC_REFERENCE_TABLES_DATASET}.us_mo_tak146_status_code_descriptions \
    --local-filepath <path>/<to>/us_mo_tak146_status_code_descriptions.csv \
    --separator , \
    --overwrite-if-exists True \
    --dry-run [True|False]
*/
SELECT * 
FROM LBAKRCOD.TAK146 status_descriptions_fh
ORDER BY FH$SCD;
"""

lower_bound_update_date = 0

NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT = """
    non_investigation_supervision_sentences_bu AS (
        -- Chooses only probation sentences that are non-investigation (not INV)
        SELECT *
        FROM LBAKRDTA.TAK024 sentence_prob_bu
        WHERE BU$PBT != 'INV'
    )"""

DISTINCT_SUPERVISION_SENTENCE_IDS_FRAGMENT = """
    distinct_supervision_sentence_ids AS (
        SELECT DISTINCT BU$DOC, BU$CYC, BU$SEO, BU$FSO
        FROM non_investigation_supervision_sentences_bu
    )"""

TAK001_OFFENDER_IDENTIFICATION_QUERY = f"""
    -- tak001_offender_identification

    SELECT *
    FROM
        LBAKRDTA.TAK001 offender_identification_ek
    LEFT OUTER JOIN
        LBAKRDTA.VAK003 dob_view
    ON EK$DOC = dob_view.DOC_ID_DOB
    WHERE
        MAX(COALESCE(EK$DLU, 0),
            COALESCE(EK$DCR, 0),
            COALESCE(UPDATE_DT, 0),
            COALESCE(CREATE_DT, 0)) >= {lower_bound_update_date}
    ORDER BY EK$DOC DESC;
    """

TAK040_OFFENDER_CYCLES = f"""
    -- tak040_offender_cycles

    SELECT *
    FROM LBAKRDTA.TAK040
    WHERE
        MAX(COALESCE(DQ$DLU, 0),
            COALESCE(DQ$DCR, 0)) >= {lower_bound_update_date}
    ORDER BY DQ$DOC;
    """

TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_INSTITUTION = f"""
    -- tak022_tak023_tak025_tak026_offender_sentence_institution

    WITH sentence_status_xref AS (
        /* Join all statuses with their associated sentences, create a recency
           rank for every status among all statuses for that sentence. */
        SELECT
            status_xref_bv.*,
            status_bw.*,
            ROW_NUMBER() OVER (
                PARTITION BY BV$DOC, BV$CYC, BV$SEO
                ORDER BY
                    BW$SY DESC,
                    -- If multiple statuses are on the same day, pick the larger
                    -- status code, alphabetically, giving preference to close (9*)
                    -- statuses
                    BW$SCD DESC,
                    -- If there are multiple field sequence numbers (FSO) with
                    -- the same status update on the same day, pick the largest
                    -- FSO.
                    BV$FSO DESC
            ) AS RECENCY_RANK_WITHIN_SENTENCE
        FROM
        	-- Note: We explicitly do not filter out probation sentences here -
        	-- if the SEO is the same, there may be relevant status dates that
        	-- we want to capture.
            LBAKRDTA.TAK025 status_xref_bv
        LEFT OUTER JOIN
            LBAKRDTA.TAK026 status_bw
        ON
            status_xref_bv.BV$DOC = status_bw.BW$DOC AND
            status_xref_bv.BV$CYC = status_bw.BW$CYC AND
            status_xref_bv.BV$SSO = status_bw.BW$SSO
    ),
    sentence_max_status_update_dates AS (
        /* Get the max create/update dates for all the status info for a given
          sentence. If any status changes for a given sentence, we want to
          re-ingest max status info for that sentence */
    	SELECT
    		BV$DOC, BV$CYC, BV$SEO,
    		MAX(COALESCE(BV$DCR, 0)) AS MAX_BV_DCR,
    		MAX(COALESCE(BV$DLU, 0)) AS MAX_BV_DLU,
    		MAX(COALESCE(BW$DCR, 0)) AS MAX_BW_DCR,
    		MAX(COALESCE(BW$DLU, 0)) AS MAX_BW_DLU
    	FROM
    		sentence_status_xref
    	GROUP BY BV$DOC, BV$CYC, BV$SEO
    ),
    most_recent_status_by_sentence AS (
        /* Select the most recent status for a given sentence, with max
           create/update info. */
        SELECT
        	sentence_status_xref.BV$DOC,
        	sentence_status_xref.BV$CYC,
        	sentence_status_xref.BV$SEO,
        	sentence_status_xref.BW$SSO AS MOST_RECENT_SENTENCE_STATUS_SSO,
        	sentence_status_xref.BW$SCD AS MOST_RECENT_SENTENCE_STATUS_SCD,
        	sentence_status_xref.BW$SY AS MOST_RECENT_SENTENCE_STATUS_DATE,
           	sentence_max_status_update_dates.MAX_BV_DCR,
    		sentence_max_status_update_dates.MAX_BV_DLU,
    		sentence_max_status_update_dates.MAX_BW_DCR,
    		sentence_max_status_update_dates.MAX_BW_DLU
        FROM
        	sentence_status_xref
        LEFT OUTER JOIN
        	sentence_max_status_update_dates
	    ON
	        sentence_status_xref.BV$DOC = sentence_max_status_update_dates.BV$DOC AND
	        sentence_status_xref.BV$CYC = sentence_max_status_update_dates.BV$CYC AND
	        sentence_status_xref.BV$SEO = sentence_max_status_update_dates.BV$SEO

        WHERE RECENCY_RANK_WITHIN_SENTENCE = 1
    )
    SELECT
        sentence_bs.*,
        sentence_inst_bt.*,
        most_recent_status_by_sentence.MOST_RECENT_SENTENCE_STATUS_SSO,
        most_recent_status_by_sentence.MOST_RECENT_SENTENCE_STATUS_SCD,
        most_recent_status_by_sentence.MOST_RECENT_SENTENCE_STATUS_DATE,
        most_recent_status_by_sentence.MAX_BV_DCR,
        most_recent_status_by_sentence.MAX_BV_DLU,
        most_recent_status_by_sentence.MAX_BW_DCR,
        most_recent_status_by_sentence.MAX_BW_DLU
    FROM
        LBAKRDTA.TAK022 sentence_bs
    JOIN
        LBAKRDTA.TAK023 sentence_inst_bt
    ON
        sentence_bs.BS$DOC = sentence_inst_bt.BT$DOC AND
        sentence_bs.BS$CYC = sentence_inst_bt.BT$CYC AND
        sentence_bs.BS$SEO = sentence_inst_bt.BT$SEO
    LEFT OUTER JOIN
        most_recent_status_by_sentence
    ON
        sentence_bs.BS$DOC = most_recent_status_by_sentence.BV$DOC AND
        sentence_bs.BS$CYC = most_recent_status_by_sentence.BV$CYC AND
        sentence_bs.BS$SEO = most_recent_status_by_sentence.BV$SEO
    WHERE
        MAX(COALESCE(BS$DLU, 0),
            COALESCE(BS$DCR, 0),
            COALESCE(BT$DLU, 0),
            COALESCE(BT$DCR, 0),
            COALESCE(MAX_BV_DLU, 0),
            COALESCE(MAX_BV_DCR, 0),
            COALESCE(MAX_BW_DLU, 0),
            COALESCE(MAX_BW_DCR, 0)) >= {lower_bound_update_date}
    ORDER BY BS$DOC, BS$CYC, BS$SEO;
    """

SUPERVISION_SENTENCE_STATUS_XREF_FRAGMENT = """
    classified_status_bw AS (
        /* Helper to classify statuses in ways that will help us figure out the types of their associated supervision
        sentences */
        SELECT
            classified_status_bw.*,
            CASE WHEN IS_PROBATION_REVOCATION = 1 THEN BW$SY ELSE NULL END AS PROBATION_REVOCATION_DATE,
            CASE WHEN IS_PROBATION_REVOCATION = 1 THEN BW$SCD ELSE NULL END AS PROBATION_REVOCATION_SCD
        FROM (
            SELECT
                BW$DOC, BW$CYC, BW$SCD, BW$SY, BW$SSO, BW$DCR, BW$DLU,
                CASE WHEN BW$SCD IN (
                    -- These statuses, when associated with a sentence, are strong indicators that that sentence is a
                    -- parole sentence.
                   '15I1200',  -- New Court Parole
                   '25I1200',  -- Court Parole-Addl Charge
                   '35I1200',  -- Court Parole-Revisit
                   '35I4100'   -- IS Compact-Parole-Revisit
                ) THEN BW$SCD ELSE NULL END AS PRIMARY_PAROLE_SCD,
                CASE WHEN BW$SCD IN (
                    -- These statuses, when associated with a sentence, are weak indicators that that sentence is a
                    -- parole sentence. We will mark a sentence as type PAROLE only if no probation statuses are
                    -- present.
                   '40O1010',  -- Parole Release
                   '40O1015',  -- Parolee Released From CRC
                   '40O1020',  -- Parole To Custody/Detainer
                   '40O1025',  -- Medical Parole Release
                   '40O1030',  -- Parole Re-Release
                   '40O1040',  -- Parole Return Rescinded
                   '40O1060',  -- Parolee Re-Rel From Inst Pgm
                   '40O1065',  -- Parolee Rel From Inst Pgm-New
                   '40O1080'  -- Parolee Released from Inst
                ) THEN BW$SCD ELSE NULL END AS SECONDARY_PAROLE_SCD,
                CASE WHEN BW$SCD IN (
                    -- These statuses, when associated with a sentence, are strong indicators that that sentence is a
                    -- probation sentence.

                    -- These statuses indicate the start of a new probation sentence
                   '15I1000',  -- New Court Probation
                   '15I2000',  -- New Diversion Supervision
                   '25I1000',  -- Court Probation-Addl Charge
                   '25I2000',  -- Diversion Supv-Addl Charge
                   '35I1000',  -- Court Probation-Revisit
                   '35I2000',  -- Diversion Supv-Revisit
                   '35I4000',  -- IS Compact-Prob-Revisit

                   -- Sometimes the revocation status is the only status associated with a sentence that explicitly
                   -- calls out the type
                   '40I2000', -- Prob Rev-Technical
                   '40I2005', -- Prob Rev-New Felony Conv
                   '40I2010', -- Prob Rev-New Misd Conv
                   '40I2015', -- Prob Rev-Felony Law Viol
                   '40I2020', -- Prob Rev-Misd Law Viol
                   '45O2000', -- Prob Rev-Technical
                   '45O2005', -- Prob Rev-New Felony Conv
                   '45O2010', -- Prob Rev-New Misd Conv
                   '45O2015', -- Prob Rev-Felony Law Viol
                   '45O2020', -- Prob Rev-Misd Law Viol

                   -- These are sometimes the only statuses you see if someone starts a "probation" sentence with a
                   -- treatment or shock incarceration stint.
                   '40N9010', -- Probation Assigned to DAI
                   '40O9010',  -- Release to Probation
                   '40O9020',  -- Release to Prob-Custody/Detain
                   '40O9030',  -- Statutory Probation Release
                   '40O9040',  -- Stat Prob Rel-Custody/Detainer
                   '40O9060',  -- Release to Prob-Treatment Ctr
                   '40O9070',  -- Petition Probation Release
                   '40O9080',  -- Petition Prob Rel-Cust/Detain

                   -- Sometimes there's no mention of probation until the completion status, if you complete probation,
                   -- the sentence was probably probation the whole time
                   '95O1000', -- Court Probation Completion
                   '95O1001', -- Court Probation ECC Completion
                   '99O1000', -- Court Probation Discharge
                   '99O1001' -- Court Probation ECC Discharge
                ) THEN BW$SCD ELSE NULL END AS PRIMARY_PROBATION_SCD,
                CASE WHEN BW$SCD IN (
                    -- These statuses, when associated with a sentence, are weak indicators that that sentence is a
                    -- probation sentence. We will mark a sentence as type PROBATION only if no parole statuses are
                    -- present.

                   -- This is a general status for being released to the field, but generally seems to show up with
                   -- probation statuses.
                   '40O7000' -- Rel to Field-DAI Other Sent
                ) THEN BW$SCD ELSE NULL END AS SECONDARY_PROBATION_SCD,
                CASE WHEN BW$SCD IN (
                    '45O2000',  -- Prob Rev-Technical
                    '45O2005',  -- Prob Rev-New Felony Conv
                    '45O2015',  -- Prob Rev-Felony Law Viol
                    '45O2010',  -- Prob Rev-New Misd Conv
                    '45O2020'   -- Prob Rev-Misd Law Viol
                ) THEN 1 ELSE 0 END AS IS_PROBATION_REVOCATION
            FROM LBAKRDTA.TAK026 status_bw
        ) classified_status_bw
    ),
    supervision_sentence_status_xref_bv AS (
        /* Associates each status with a particular sentence, filtering for statuses associated with supervision
        sentences */
        SELECT
            BV$DOC, BV$CYC, BV$SEO, BV$FSO, BV$DCR, BV$DLU, BW$SCD, BW$SY, BW$SSO, BW$DCR, BW$DLU,
            PRIMARY_PAROLE_SCD,
            SECONDARY_PAROLE_SCD,
            PRIMARY_PROBATION_SCD,
            SECONDARY_PROBATION_SCD,
            IS_PROBATION_REVOCATION,
            PROBATION_REVOCATION_DATE,
            PROBATION_REVOCATION_SCD
        FROM
            LBAKRDTA.TAK025 status_xref_bv
        LEFT OUTER JOIN
            classified_status_bw
        ON
            status_xref_bv.BV$DOC = classified_status_bw.BW$DOC AND
            status_xref_bv.BV$CYC = classified_status_bw.BW$CYC AND
            status_xref_bv.BV$SSO = classified_status_bw.BW$SSO
        WHERE BV$FSO > 0
    )"""

# Required that you also use DISTINCT_SUPERVISION_SENTENCE_IDS_FRAGMENT and SUPERVISION_SENTENCE_STATUS_XREF_FRAGMENT
# in your query
SUPERVISION_SENTENCE_TYPE_CLASSIFIER_FRAGMENT = """
    collapsed_sentence_status_type_classification AS (
       SELECT
          BV$DOC, BV$CYC, BV$SEO,
          MIN(PRIMARY_PAROLE_SCD) AS PRIMARY_PAROLE_SCD,
          MIN(SECONDARY_PAROLE_SCD) AS SECONDARY_PAROLE_SCD,
          MIN(PRIMARY_PROBATION_SCD) AS PRIMARY_PROBATION_SCD,
          MIN(SECONDARY_PROBATION_SCD) AS SECONDARY_PROBATION_SCD,
          MAX(PROBATION_REVOCATION_DATE) AS PROBATION_REVOCATION_DATE,
          MAX(PROBATION_REVOCATION_SCD) AS PROBATION_REVOCATION_SCD
       FROM supervision_sentence_status_xref_bv
       GROUP BY BV$DOC, BV$CYC, BV$SEO
    ),
    supervision_sentence_type_classifier AS (
        /* Helper for finding the type of a supervision sentence based on the existence of certain statuses */
        SELECT
            BU$DOC, BU$CYC, BU$SEO,
            CASE
                WHEN
                    PRIMARY_PAROLE_SCD IS NULL AND (
                        PRIMARY_PROBATION_SCD IS NOT NULL OR
                        SECONDARY_PROBATION_SCD IS NOT NULL
                    )
                THEN 'PROBATION'
                WHEN
                    PRIMARY_PROBATION_SCD IS NULL AND (
                        PRIMARY_PAROLE_SCD IS NOT NULL OR
                        SECONDARY_PAROLE_SCD IS NOT NULL
                    )
                 THEN 'PAROLE'
                ELSE 'UNKNOWN'
            END AS SENTENCE_TYPE,
            PRIMARY_PAROLE_SCD,
            SECONDARY_PAROLE_SCD,
            PRIMARY_PROBATION_SCD,
            SECONDARY_PROBATION_SCD,
            PROBATION_REVOCATION_DATE,
            PROBATION_REVOCATION_SCD
        FROM
            distinct_supervision_sentence_ids
        LEFT OUTER JOIN
            collapsed_sentence_status_type_classification
        ON
            distinct_supervision_sentence_ids.BU$DOC = collapsed_sentence_status_type_classification.BV$DOC AND
            distinct_supervision_sentence_ids.BU$CYC = collapsed_sentence_status_type_classification.BV$CYC AND
            distinct_supervision_sentence_ids.BU$SEO = collapsed_sentence_status_type_classification.BV$SEO
    )"""

SUPERVISION_SENTENCE_STATUS_MAX_UPDATE_DATES_FRAGMENT = """
    sentence_max_status_update_dates AS (
        /* Get the max create/update dates for all the status info for a given
          sentence. If any status changes for a given sentence, we want to
          re-ingest max status info for that sentence */
        SELECT
            BV$DOC, BV$CYC, BV$SEO,
            MAX(COALESCE(BV$DCR, 0)) AS MAX_BV_DCR,
            MAX(COALESCE(BV$DLU, 0)) AS MAX_BV_DLU,
            MAX(COALESCE(BW$DCR, 0)) AS MAX_BW_DCR,
            MAX(COALESCE(BW$DLU, 0)) AS MAX_BW_DLU
        FROM
            supervision_sentence_status_xref_bv
        GROUP BY BV$DOC, BV$CYC, BV$SEO
    )"""

FULL_SUPERVISION_SENTENCE_INFO_FRAGMENT = """
    full_supervision_sentence_info AS (
        SELECT sentence_bs.*, non_investigation_supervision_sentences_bu.*
        FROM
            LBAKRDTA.TAK022 sentence_bs
        JOIN
            non_investigation_supervision_sentences_bu
        ON
            sentence_bs.BS$DOC = non_investigation_supervision_sentences_bu.BU$DOC AND
            sentence_bs.BS$CYC = non_investigation_supervision_sentences_bu.BU$CYC AND
            sentence_bs.BS$SEO = non_investigation_supervision_sentences_bu.BU$SEO
    )"""


TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_SUPERVISION = f"""
    -- tak022_tak024_tak025_tak026_offender_sentence_supervision

    WITH
    {SUPERVISION_SENTENCE_STATUS_XREF_FRAGMENT},
    {SUPERVISION_SENTENCE_STATUS_MAX_UPDATE_DATES_FRAGMENT},
    {NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT},
    {DISTINCT_SUPERVISION_SENTENCE_IDS_FRAGMENT},
    {SUPERVISION_SENTENCE_TYPE_CLASSIFIER_FRAGMENT},
    {FULL_SUPERVISION_SENTENCE_INFO_FRAGMENT},
    supervision_sentence_status_xref_with_types AS (
        SELECT
            supervision_sentence_status_xref_bv.BV$DOC,
            supervision_sentence_status_xref_bv.BV$CYC,
            supervision_sentence_status_xref_bv.BV$SEO,
            supervision_sentence_status_xref_bv.BV$FSO,
            supervision_sentence_status_xref_bv.BW$SCD,
            supervision_sentence_status_xref_bv.BW$SY,
            supervision_sentence_status_xref_bv.BW$SSO,
            supervision_sentence_type_classifier.SENTENCE_TYPE,
            CASE
                WHEN SENTENCE_TYPE = 'PROBATION' AND IS_PROBATION_REVOCATION = 1 THEN 1
                ELSE 0
            END AS IS_PROBATION_REVOCATION
        FROM
            supervision_sentence_status_xref_bv
        JOIN
            supervision_sentence_type_classifier
        ON
            supervision_sentence_status_xref_bv.BV$DOC = supervision_sentence_type_classifier.BU$DOC AND
            supervision_sentence_status_xref_bv.BV$CYC = supervision_sentence_type_classifier.BU$CYC AND
            supervision_sentence_status_xref_bv.BV$SEO = supervision_sentence_type_classifier.BU$SEO
            AND (SENTENCE_TYPE != 'PROBATION' OR
                 supervision_sentence_type_classifier.PROBATION_REVOCATION_DATE IS NULL OR
                 supervision_sentence_status_xref_bv.BW$SY <=
                 supervision_sentence_type_classifier.PROBATION_REVOCATION_DATE)
    ),
    valid_sentences_with_status_xref AS (
        /* Join all statuses with valid, non-investigative sentences */
        SELECT
            BU$DOC, BU$CYC, BU$SEO,
            supervision_sentence_status_xref_with_types.IS_PROBATION_REVOCATION,
            supervision_sentence_status_xref_with_types.SENTENCE_TYPE,
            supervision_sentence_status_xref_with_types.BV$DOC,
            supervision_sentence_status_xref_with_types.BV$CYC,
            supervision_sentence_status_xref_with_types.BV$SEO,
            supervision_sentence_status_xref_with_types.BV$FSO,
            supervision_sentence_status_xref_with_types.BW$SSO,
            supervision_sentence_status_xref_with_types.BW$SCD,
            supervision_sentence_status_xref_with_types.BW$SY
        FROM
            distinct_supervision_sentence_ids
        LEFT OUTER JOIN
            supervision_sentence_status_xref_with_types
        ON
            supervision_sentence_status_xref_with_types.BV$DOC = distinct_supervision_sentence_ids.BU$DOC AND
            supervision_sentence_status_xref_with_types.BV$CYC = distinct_supervision_sentence_ids.BU$CYC AND
            supervision_sentence_status_xref_with_types.BV$SEO = distinct_supervision_sentence_ids.BU$SEO AND
            supervision_sentence_status_xref_with_types.BV$FSO = distinct_supervision_sentence_ids.BU$FSO
    ),
    ranked_supervision_sentence_status_xref AS (
        SELECT
            valid_sentences_with_status_xref.*,
            ROW_NUMBER() OVER (
                PARTITION BY BV$DOC, BV$CYC, BV$SEO
                ORDER BY
                    BW$SY DESC,
                    -- If there is a probation revocation status on a day, this is the
                    -- most important piece of information
                    IS_PROBATION_REVOCATION DESC,
                    -- If there are multiple field sequence numbers (FSO) with
                    -- the same status update on the same day, pick the largest
                    -- FSO, since this is likely the most recent status
                    BV$FSO DESC,
                    -- Otherwise, if multiple statuses are on the same day, pick the larger
                    -- status code, alphabetically, giving preference to close (9*)
                    -- statuses
                    BW$SCD DESC

            ) AS SUPERVISION_SENTENCE_STATUS_RECENCY_RANK
        FROM
            valid_sentences_with_status_xref
    ),
    most_recent_fso_and_status_for_sentence AS (
        /* Pick the FSO row with the most recent status */
        SELECT
            BU$DOC, BU$CYC, BU$SEO,
            ranked_supervision_sentence_status_xref.SENTENCE_TYPE,
            ranked_supervision_sentence_status_xref.BV$FSO AS MOST_RECENT_SUPERVISION_FSO,
            ranked_supervision_sentence_status_xref.BW$SSO AS MOST_RECENT_SENTENCE_STATUS_SSO,
            ranked_supervision_sentence_status_xref.BW$SCD AS MOST_RECENT_SENTENCE_STATUS_SCD,
            ranked_supervision_sentence_status_xref.BW$SY AS MOST_RECENT_SENTENCE_STATUS_DATE
        FROM
            ranked_supervision_sentence_status_xref
        WHERE SUPERVISION_SENTENCE_STATUS_RECENCY_RANK = 1
    )
    SELECT
        full_supervision_sentence_info.*,
        most_recent_fso_and_status_for_sentence.SENTENCE_TYPE,
        most_recent_fso_and_status_for_sentence.MOST_RECENT_SENTENCE_STATUS_SSO,
        most_recent_fso_and_status_for_sentence.MOST_RECENT_SENTENCE_STATUS_SCD,
        most_recent_fso_and_status_for_sentence.MOST_RECENT_SENTENCE_STATUS_DATE,
        sentence_max_status_update_dates.MAX_BV_DCR,
        sentence_max_status_update_dates.MAX_BV_DLU,
        sentence_max_status_update_dates.MAX_BW_DCR,
        sentence_max_status_update_dates.MAX_BW_DLU
    FROM
        full_supervision_sentence_info
    JOIN
        most_recent_fso_and_status_for_sentence
    ON
       full_supervision_sentence_info.BS$DOC = most_recent_fso_and_status_for_sentence.BU$DOC AND
       full_supervision_sentence_info.BS$CYC = most_recent_fso_and_status_for_sentence.BU$CYC AND
       full_supervision_sentence_info.BS$SEO = most_recent_fso_and_status_for_sentence.BU$SEO AND
       full_supervision_sentence_info.BU$FSO = most_recent_fso_and_status_for_sentence.MOST_RECENT_SUPERVISION_FSO
    LEFT OUTER JOIN
        sentence_max_status_update_dates
    ON
       full_supervision_sentence_info.BS$DOC = sentence_max_status_update_dates.BV$DOC AND
       full_supervision_sentence_info.BS$CYC = sentence_max_status_update_dates.BV$CYC AND
       full_supervision_sentence_info.BS$SEO = sentence_max_status_update_dates.BV$SEO
    WHERE
        MAX(COALESCE(BS$DLU, 0),
            COALESCE(BS$DCR, 0),
            COALESCE(BU$DLU, 0),
            COALESCE(BU$DCR, 0),
            COALESCE(MAX_BV_DLU, 0),
            COALESCE(MAX_BV_DCR, 0),
            COALESCE(MAX_BW_DCR, 0),
            COALESCE(MAX_BW_DCR, 0)) >= {lower_bound_update_date}
     ORDER BY
           full_supervision_sentence_info.BS$DOC,
           full_supervision_sentence_info.BS$CYC,
           full_supervision_sentence_info.BS$SEO;"""

INCARCERATION_SUB_SUBCYCLE_SPANS_FRAGMENT = """
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

STATUSES_BY_DATE_FRAGMENT = """
    all_scd_codes_by_date AS (
        -- All SCD status codes grouped by DOC, CYC, and SY (Date).
        SELECT
            BW$DOC,
            BW$CYC,
            BW$SY AS STATUS_DATE,
            LISTAGG(BW$SCD, ',') AS STATUS_CODES
        FROM
            status_bw
        GROUP BY BW$DOC, BW$CYC, BW$SY
    )
    """

# TODO(#2798): Update this query/mappings to remove explicit linking to
#  sentences - entity matching should handle date-based matching just like it
#  does for supervision periods.
TAK158_TAK023_TAK026_INCARCERATION_PERIOD_FROM_INCARCERATION_SENTENCE = f"""
    -- tak158_tak023_tak026_incarceration_period_from_incarceration_sentence
    WITH {INCARCERATION_SUB_SUBCYCLE_SPANS_FRAGMENT},
    {STATUSES_BY_DATE_FRAGMENT},
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
        incarceration_periods_from_incarceration_sentence.F1$DOC = start_codes.BW$DOC AND
        incarceration_periods_from_incarceration_sentence.F1$CYC = start_codes.BW$CYC AND
        incarceration_periods_from_incarceration_sentence.SUB_SUBCYCLE_START_DT = start_codes.STATUS_DATE
    LEFT OUTER JOIN
        all_scd_codes_by_date end_codes
    ON
        incarceration_periods_from_incarceration_sentence.F1$DOC = end_codes.BW$DOC AND
        incarceration_periods_from_incarceration_sentence.F1$CYC = end_codes.BW$CYC AND
        incarceration_periods_from_incarceration_sentence.SUB_SUBCYCLE_END_DT = end_codes.STATUS_DATE
    ORDER BY BT$DOC, BT$CYC, BT$SEO, F1$SQN;
    """

TAK158_TAK024_TAK026_TAK039_INCARCERATION_PERIOD_FROM_SUPERVISION_SENTENCE = f"""
    -- tak158_tak024_tak026_incarceration_period_from_supervision_sentence

    WITH {INCARCERATION_SUB_SUBCYCLE_SPANS_FRAGMENT},
    {STATUSES_BY_DATE_FRAGMENT},
    incarceration_subcycle_from_supervision_sentence AS (
        SELECT
            probation_sentence_ids.BU$DOC,
            probation_sentence_ids.BU$CYC,
            probation_sentence_ids.BU$SEO,
            body_status_f1.*
        FROM (
            -- We intentionally do NOT filter out INV sentences here, otherwise an incarceration subcycle that is
            -- erroneously attributed to an INV sentence in TAK158 would get dropped entirely.
            SELECT BU$DOC, BU$CYC, BU$SEO
            FROM LBAKRDTA.TAK024 sentence_prob_bu
            GROUP BY BU$DOC, BU$CYC, BU$SEO
        ) probation_sentence_ids
        LEFT OUTER JOIN
            LBAKRDTA.TAK158 body_status_f1
        ON
            probation_sentence_ids.BU$DOC = body_status_f1.F1$DOC AND
            probation_sentence_ids.BU$CYC = body_status_f1.F1$CYC AND
            probation_sentence_ids.BU$SEO = body_status_f1.F1$SEO
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
        incarceration_periods_from_supervision_sentence.F1$DOC = start_codes.BW$DOC AND
        incarceration_periods_from_supervision_sentence.F1$CYC = start_codes.BW$CYC AND
        incarceration_periods_from_supervision_sentence.SUB_SUBCYCLE_START_DT = start_codes.STATUS_DATE
    LEFT OUTER JOIN
        all_scd_codes_by_date end_codes
    ON
        incarceration_periods_from_supervision_sentence.F1$DOC = end_codes.BW$DOC AND
        incarceration_periods_from_supervision_sentence.F1$CYC = end_codes.BW$CYC AND
        incarceration_periods_from_supervision_sentence.SUB_SUBCYCLE_END_DT = end_codes.STATUS_DATE
    ORDER BY BU$DOC, BU$CYC, BU$SEO, F1$SQN;
    """

ALL_OFFICERS_FRAGMENT = """all_officers AS (
        -- Combination of 2 officer tables into one source of truth. Both tables
        -- contain information about different groups of officers. From
        -- conversations with MO contacts, we should use a combination of both
        -- tables to get a full understanding of all officers.
        SELECT
            officers_1.*
        FROM
            LBCMDATA.APFX90 officers_1
        WHERE BDGNO != ''
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
    normalized_all_officers AS (
        SELECT
            BDGNO,
            -- This is the actual job code
            DEPCLS,
            -- This is the job name, may differ slightly for the same job code
            MAX(CLSTTL) AS CLSTTL,
            LNAME,
            FNAME,
            MINTL,
            STRDTE,
            DTEORD,
             -- When we find out about an officer with the exact same role,
             -- etc from both tables, pick the largest end date.
            MAX(ENDDTE) AS ENDDTE
        FROM all_officers
        GROUP BY
            BDGNO,
            DEPCLS,
            CLSTTL,
            LNAME,
            FNAME,
            MINTL,
            STRDTE,
            DTEORD
    )
    """

OFFICERS_WITH_MOST_RECENT_ROLE_FRAGMENT = f"""
    {ALL_OFFICERS_FRAGMENT},
    officers_with_role_recency_ranks AS(
        -- Officers with their roles ranked from most recent to least recent.
        SELECT
            BDGNO,
            CLSTTL,
            LNAME,
            FNAME,
            MINTL,
            ROW_NUMBER() OVER (PARTITION BY BDGNO ORDER BY STRDTE DESC) AS recency_rank
        FROM
            normalized_all_officers),
    officers_with_recent_role AS (
        -- Officers with their most recent role only
        SELECT
            BDGNO,
            CLSTTL,
            LNAME,
            FNAME,
            MINTL
        FROM
            officers_with_role_recency_ranks
        WHERE
            officers_with_role_recency_ranks.recency_rank = 1
            AND officers_with_role_recency_ranks.CLSTTL != ''
            AND officers_with_role_recency_ranks.CLSTTL IS NOT NULL)
    """

OFFICER_ROLE_SPANS_FRAGMENT = f"""
    {ALL_OFFICERS_FRAGMENT},
    officers_with_role_time_ranks AS(
        -- Officers with their roles ranked from least recent to most recent,
        -- based on start date, then end date (current assignments with
        -- DTEORD=1 ranked last).
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
                PARTITION BY BDGNO ORDER BY STRDTE, DTEORD, ENDDTE DESC
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
                WHEN start_role.ENDDTE = 0 THEN COALESCE(end_role.STRDTE, start_role.ENDDTE)
                WHEN (start_role.ENDDTE < start_role.STRDTE) THEN COALESCE(end_role.STRDTE, 0)
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

# TODO(#3736): Incremental updates from this query will be supported automatically when we transition MO to SQL
#  pre-processing.
TAK034_TAK026_TAK039_APFX90_APFX91_SUPERVISION_ENHANCEMENTS_SUPERVISION_PERIODS = f"""
    -- tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods

    WITH field_assignments_ce AS (
        SELECT
            LBAKRDTA.TAK034.*,
            CE$DOC AS DOC,
            CE$CYC AS CYC,
            CE$HF AS FLD_ASSN_BEG_DT,
            CE$EH AS FLD_ASSN_END_DT,
            CE$PLN AS LOC_ACRO,
            SUBSTR(CE$PLN, 1,2) AS LOC_ACRO_TWO_LETTER
        FROM
            LBAKRDTA.TAK034
    ),
    augmented_field_assignments AS (
        SELECT
            field_assignments_ce.*,
            CASE
                WHEN (LOC_ACRO_TWO_LETTER IN ('EC', 'EP', '07', '08') OR LOC_ACRO = 'ERA') THEN 'EASTERN'
                WHEN LOC_ACRO_TWO_LETTER IN ('03', '11', '16', '17', '18', '26', '38') THEN 'NORTHEAST'
                WHEN LOC_ACRO_TWO_LETTER IN ('01', '04', '19', '24', '28', 'WN') THEN 'WESTERN'
                WHEN LOC_ACRO_TWO_LETTER IN (
                    '02', '05', '06', '20', '27', '29', '32', '34', '35', '39') THEN 'NORTH CENTRAL'
                WHEN LOC_ACRO_TWO_LETTER IN ('09', '10', '13', '21', '30', '33', '42', '43', '44') THEN 'SOUTHWEST'
                WHEN LOC_ACRO_TWO_LETTER IN (
                    '12', '14', '15', '22', '23', '25', '31', '36', '37', '41') THEN 'SOUTHEAST'
                WHEN LOC_ACRO = 'PPCMDCTR' THEN 'CENTRAL OFFICE'
                WHEN LOC_ACRO IN ('SLCRC', 'TCSTL') THEN 'TCSTL'
                ELSE 'UNCLASSED'
            END AS REGION,
            ROW_NUMBER() OVER (
                PARTITION BY DOC, CYC
                ORDER BY
                    FLD_ASSN_BEG_DT,
                    FLD_ASSN_END_DT,
                    CE$OR0 ASC
            ) AS FIELD_ASSIGNMENT_SEQ_NUM
        FROM field_assignments_ce
    ),
    field_assignments_with_valid_region AS (
        SELECT *
        FROM augmented_field_assignments
        WHERE REGION != 'UNCLASSED'
    ),
    status_bw AS (
        SELECT
            *
        FROM
            LBAKRDTA.TAK026
        WHERE
            BW$SCD IS NOT NULL
            AND BW$SCD != ''
    ),
    non_inv_start_status_codes AS (
        SELECT
            BW$DOC,
            BW$CYC,
            BW$SY,
            BW$SSO,
            BW$SCD,
            ROW_NUMBER() OVER (PARTITION BY BW$DOC, BW$CYC ORDER BY BW$SY, BW$SCD) AS START_STATUS_RANK
        FROM status_bw
        WHERE (
            (
                BW$SCD LIKE '%I%' -- Start statuses (I = IN)
                AND BW$SCD NOT LIKE '05I5%' -- Invesigation start statuses
                AND BW$SCD NOT LIKE '25I5%' -- Investigation - Additional Charge statuses
                AND BW$SCD NOT LIKE '35I5%' -- Investigation Revisit statuses
            ) OR
            BW$SCD IN (
                '10L6000', -- New CC Fed/State (Papers Only)
                '20L6000', -- CC Fed/State (Papers Only)-AC
                '30L6000'  -- CC Fed/State(Papers Only)-Revt
            )
            OR (
                BW$SCD IN (
                    -- (In very old cases in the 1980s, this is used as the first code to indicate entering probation)
                    '40O9010'  -- Release to Probation
                )
                AND BW$SSO = 1
            )

        )  AND BW$SCD NOT IN (
            '15I3000', -- New PreTrial Bond Supervision
            '25I3000', -- PreTrial Bond Supv-Addl Charge
            '35I3000'  -- PreTrial Bond Supv-Revisit
        )
    ),
    first_non_inv_start_status_code AS (
        SELECT
            BW$DOC AS DOC,
            BW$CYC AS CYC,
            BW$SSO AS SSO,
            BW$SCD AS SCD,
            BW$SY AS STATUS_CODE_CHG_DT
        FROM non_inv_start_status_codes
        WHERE START_STATUS_RANK = 1
    ),
    supv_period_partition_statuses AS (
        SELECT
            BW$DOC AS DOC,
            BW$CYC AS CYC,
            BW$SSO AS SSO,
            BW$SCD AS SCD,
            BW$SY AS STATUS_CODE_CHG_DT
        FROM
            status_bw
         WHERE (
            BW$SCD IN (
                -- Declared Absconder
                '65O1010', '65L9100',
                -- Offender re-engaged
                '65N9500'
            )
        )
        UNION
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
                    STATUS_SEQ_NUM ASC
            ) AS SUB_PERIOD_SEQ
        FROM (
            -- Field assignment open dates
            SELECT
                DOC,
                CYC,
                FIELD_ASSIGNMENT_SEQ_NUM,
                0 AS STATUS_SEQ_NUM,
                '' AS STATUS_CODE,
                field_assignments_with_valid_region.FLD_ASSN_BEG_DT AS CHANGE_DATE,
                '1-OPEN' AS DATE_TYPE
            FROM
                field_assignments_with_valid_region
            UNION
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
                field_assignments_with_valid_region.CE$DOC =
                    supv_period_partition_statuses.DOC AND
                field_assignments_with_valid_region.CE$CYC =
                    supv_period_partition_statuses.CYC AND
                field_assignments_with_valid_region.FLD_ASSN_BEG_DT <
                    supv_period_partition_statuses.STATUS_CODE_CHG_DT AND
                (supv_period_partition_statuses.STATUS_CODE_CHG_DT <
                    field_assignments_with_valid_region.FLD_ASSN_END_DT
                    OR field_assignments_with_valid_region.FLD_ASSN_END_DT = 0
                )
            WHERE supv_period_partition_statuses.DOC IS NOT NULL
            UNION
            -- Field assignment close dates
            SELECT
                DOC,
                CYC,
                FIELD_ASSIGNMENT_SEQ_NUM,
                0 AS STATUS_SEQ_NUM,
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
            CE$PLN AS LOCATION_ACRONYM,
            CE$PON AS SUPV_OFFICER_ID
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
                    OR basic_supervision_periods.SUPV_PERIOD_END_DT = 0) AND
                (officer_role_spans.END_DATE = 0
                    OR officer_role_spans.END_DATE > basic_supervision_periods.SUPV_PERIOD_BEG_DT)
        )
        WHERE OFFICER_ROLE_RECENCY_RANK = 1
    ),
    supervision_case_types AS (
		SELECT
			DOC_ID,
			CYCLE_NO,
			CASE_TYPE_START_DATE,
			CASE WHEN CASE_TYPE_STOP_DATE = 77991231
			    THEN 0 ELSE CASE_TYPE_STOP_DATE
			END AS CASE_TYPE_STOP_DATE,
			SUPERVSN_ENH_TYPE_CD
		FROM (
			SELECT
				DOC_ID,
				CYCLE_NO,
				CAST(VARCHAR_FORMAT(ACTUAL_START_DT, 'YYYYMMDD') AS INT) AS CASE_TYPE_START_DATE,
				CAST(VARCHAR_FORMAT(ACTUAL_STOP_DT, 'YYYYMMDD') AS INT) AS CASE_TYPE_STOP_DATE,
				SUPERVSN_ENH_TYPE_CD
			FROM
				OFNDR_PDB.FOC_SUPERVISION_ENHANCEMENTS_VW
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
	        LISTAGG(SUPERVSN_ENH_TYPE_CD, ',') AS CASE_TYPE_LIST
    	FROM
    		periods_with_officer_info
    	LEFT OUTER JOIN
    		supervision_case_types
    	ON
    		periods_with_officer_info.DOC = supervision_case_types.DOC_ID AND
    		periods_with_officer_info.CYC = supervision_case_types.CYCLE_NO AND
            (supervision_case_types.CASE_TYPE_START_DATE <=
                    periods_with_officer_info.SUPV_PERIOD_END_DT
                OR periods_with_officer_info.SUPV_PERIOD_END_DT = 0) AND
            (supervision_case_types.CASE_TYPE_STOP_DATE = 0
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
			DN$DOC,
		    DN$CYC,
		    DN$NSN AS SUP_TYPE_SCORE_SEQ_NUM,
		    DN$RC AS SUP_TYPE_SCORE_REPORT_DATE,
		    DN$PST AS SUP_TYPE
		 FROM LBAKRDTA.TAK039
		 WHERE DN$PST IS NOT NULL AND DN$PST != ''
	 ),
	supervision_type_with_seq_num AS (
		SELECT
			supervision_type_assessments.*,
		    ROW_NUMBER() OVER (PARTITION BY DN$DOC, DN$CYC
		    				   ORDER BY SUP_TYPE_SCORE_REPORT_DATE, SUP_TYPE_SCORE_SEQ_NUM) AS SYNTHETIC_SEQ_NUM
		FROM supervision_type_assessments
	),
	supervision_type_spans AS (
		SELECT
			first.DN$DOC,
			first.DN$CYC,
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
			first.DN$DOC = next.DN$DOC
			AND first.DN$CYC = next.DN$CYC
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
	    		periods_with_officer_and_case_type_info.DOC = supervision_type_spans.DN$DOC
	    		AND periods_with_officer_and_case_type_info.CYC = supervision_type_spans.DN$CYC
	    		AND (periods_with_officer_and_case_type_info.SUPV_PERIOD_END_DT >= supervision_type_spans.START_DATE
	    			 OR periods_with_officer_and_case_type_info.SUPV_PERIOD_END_DT = 0)
	    	 	AND (periods_with_officer_and_case_type_info.SUPV_PERIOD_BEG_DT < supervision_type_spans.END_DATE
	    	 		 OR supervision_type_spans.END_DATE IS NULL)
	    	)
	    WHERE
	    	supervision_type_recency_rank = 1
    ),
    statuses_on_days AS (
        SELECT
            BW$DOC AS DOC,
            BW$CYC AS CYC,
            BW$SY AS STATUSES_DATE,
            LISTAGG(BW$SCD, ',') AS STATUS_CODE_LIST
        FROM
            status_bw
        GROUP BY BW$DOC, BW$CYC, BW$SY
    )
    SELECT
        periods_with_officer_case_type_and_supervision_type_info.DOC,
        periods_with_officer_case_type_and_supervision_type_info.CYC,
        periods_with_officer_case_type_and_supervision_type_info.FIELD_ASSIGNMENT_SEQ_NUM,
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
    ORDER BY DOC, CYC;
    """

TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT = """
        -- Finally formed documents are ones which are no longer in a draft
        -- state.
        SELECT
            finally_formed_documents_e6.E6$DOC,
            finally_formed_documents_e6.E6$CYC,
            finally_formed_documents_e6.E6$DOS,
            MAX(COALESCE(finally_formed_documents_e6.E6$DCR, 0)) as final_formed_create_date,
            MAX(COALESCE(finally_formed_documents_e6.E6$DLU, 0)) as final_formed_update_date
        FROM
            LBAKRDTA.TAK142 finally_formed_documents_e6
        WHERE
            finally_formed_documents_e6.E6$DON = '{document_type_code}'
        GROUP BY
            finally_formed_documents_e6.E6$DOC,
            finally_formed_documents_e6.E6$CYC,
            finally_formed_documents_e6.E6$DOS"""

FINALLY_FORMED_CITATIONS_E6 = TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT.format(
    document_type_code="XIT"
)
FINALLY_FORMED_VIOLATIONS_E6 = TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT.format(
    document_type_code="XIF"
)

# TODO(#2805): Update to do a date-based join on OFFICER_ROLE_SPANS_FRAGMENT
TAK028_TAK042_TAK076_TAK024_VIOLATION_REPORTS = f"""
    -- tak028_tak042_tak076_tak024_violation_reports

    WITH
    {NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT},
    {OFFICERS_WITH_MOST_RECENT_ROLE_FRAGMENT},
    conditions_violated_cf AS (
    -- An updated version of TAK042 that only has one row per citation.
        SELECT
            conditions_cf.CF$DOC,
            conditions_cf.CF$CYC,
            conditions_cf.CF$VSN,
            LISTAGG(conditions_cf.CF$VCV, ',') AS violated_conditions,
            MAX(COALESCE(conditions_cf.CF$DCR, 0)) as create_dt,
            MAX(COALESCE(conditions_cf.CF$DLU, 0)) as update_dt
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
                non_investigation_supervision_sentences_bu
            ON
                sentence_xref_cz.CZ$DOC = non_investigation_supervision_sentences_bu.BU$DOC
                AND sentence_xref_cz.CZ$CYC = non_investigation_supervision_sentences_bu.BU$CYC
                AND sentence_xref_cz.CZ$SEO = non_investigation_supervision_sentences_bu.BU$SEO
                AND sentence_xref_cz.CZ$FSO = non_investigation_supervision_sentences_bu.BU$FSO
            WHERE sentence_xref_cz.CZ$FSO = 0 OR
                non_investigation_supervision_sentences_bu.BU$DOC IS NOT NULL
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
        MAX(COALESCE(conditions_violated_cf.UPDATE_DT, 0),
            COALESCE(conditions_violated_cf.CREATE_DT, 0),
            COALESCE(violation_reports_by.BY$DLU, 0),
            COALESCE(violation_reports_by.BY$DCR, 0),
            COALESCE(valid_sentences_cz.CZ$DLU, 0),
            COALESCE(valid_sentences_cz.CZ$DCR, 0),
            COALESCE(finally_formed_violations_e6.final_formed_create_date, 0),
            COALESCE(finally_formed_violations_e6.final_formed_update_date, 0)) >= {lower_bound_update_date}
    ORDER BY BY$DOC, BY$CYC, BY$VSN;
    """

TAK291_TAK292_TAK024_CITATIONS = f"""
    -- tak291_tak292_tak024_citations

    WITH
    {NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT},
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
                non_investigation_supervision_sentences_bu
            ON
                sentence_xref_js.JS$DOC = non_investigation_supervision_sentences_bu.BU$DOC
                AND sentence_xref_js.JS$CYC = non_investigation_supervision_sentences_bu.BU$CYC
                AND sentence_xref_js.JS$SEO = non_investigation_supervision_sentences_bu.BU$SEO
                AND sentence_xref_js.JS$FSO = non_investigation_supervision_sentences_bu.BU$FSO
            WHERE sentence_xref_js.JS$FSO = 0 OR
                non_investigation_supervision_sentences_bu.BU$DOC IS NOT NULL
        ) sentence_xref_with_probation_info_js_bu
    ),
    citations_with_multiple_violations_jt AS (
    -- An updated version of TAK292 that only has one row per citation.
        SELECT
            citations_jt.JT$DOC,
            citations_jt.JT$CYC,
            citations_jt.JT$CSQ,
            MAX(COALESCE(citations_jt.JT$VG, 0)) AS max_date,
            LISTAGG(citations_jt.JT$VCV, ',') AS violated_conditions,
            MAX(COALESCE(citations_jt.JT$DCR, 0)) AS create_dt,
            MAX(COALESCE(citations_jt.JT$DLU, 0)) AS update_dt
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
        MAX(COALESCE(citations_with_multiple_violations_jt.UPDATE_DT, 0),
            COALESCE(citations_with_multiple_violations_jt.CREATE_DT, 0),
            COALESCE(valid_sentences_js.JS$DLU, 0),
            COALESCE(valid_sentences_js.JS$DCR, 0),
            COALESCE(finally_formed_citations_e6.final_formed_create_date, 0),
            COALESCE(finally_formed_citations_e6.final_formed_update_date, 0)) >= {lower_bound_update_date}
    ORDER BY JT$DOC, JT$CYC, JT$CSQ;
    """

ORAS_ASSESSMENTS_WEEKLY = """
    -- oras_assessments_weekly
    SELECT
        *
    FROM
        FOCTEST.ORAS_ASSESSMENTS_WEEKLY
    WHERE
        E22 = 'Complete'
    -- explicitly filter out any test data from UCCI
        AND E14 NOT LIKE '%Test%'
        AND E14 NOT LIKE '%test%';
    """


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        # ~~~ START REFERENCE TABLE QUERIES ~~~ #
        # TODO(#3736): When we transition MO to SQL pre-processing, these tables should be imported as normal raw data
        #  imports and the calculation pipelines should pull the necessary data from
        #  `us_mo_raw_data_up_to_date_views.<table_name>_latest`.
        ("us_mo_tak025_sentence_status_xref", US_MO_TAK025_SENTENCE_STATUS_XREF_QUERY),
        ("us_mo_tak026_sentence_status", US_MO_TAK026_SENTENCE_STATUS_QUERY),
        (
            "us_mo_tak146_status_code_descriptions",
            US_MO_TAK146_STATUS_CODE_DESCRIPTIONS_QUERY,
        ),
        # ~~~ END REFERENCE TABLE QUERIES ~~~ #
        ("tak001_offender_identification", TAK001_OFFENDER_IDENTIFICATION_QUERY),
        ("oras_assessments_weekly", ORAS_ASSESSMENTS_WEEKLY),
        ("tak040_offender_cycles", TAK040_OFFENDER_CYCLES),
        (
            "tak022_tak023_tak025_tak026_offender_sentence_institution",
            TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_INSTITUTION,
        ),
        (
            "tak022_tak024_tak025_tak026_offender_sentence_supervision",
            TAK022_TAK023_TAK025_TAK026_OFFENDER_SENTENCE_SUPERVISION,
        ),
        (
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence",
            TAK158_TAK023_TAK026_INCARCERATION_PERIOD_FROM_INCARCERATION_SENTENCE,
        ),
        (
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence",
            TAK158_TAK024_TAK026_TAK039_INCARCERATION_PERIOD_FROM_SUPERVISION_SENTENCE,
        ),
        (
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
            TAK034_TAK026_TAK039_APFX90_APFX91_SUPERVISION_ENHANCEMENTS_SUPERVISION_PERIODS,
        ),
        (
            "tak028_tak042_tak076_tak024_violation_reports",
            TAK028_TAK042_TAK076_TAK024_VIOLATION_REPORTS,
        ),
        ("tak291_tak292_tak024_citations", TAK291_TAK292_TAK024_CITATIONS),
    ]


if __name__ == "__main__":
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/mo_queries')
    output_sql_queries(get_query_name_to_query_list(), output_dir)
