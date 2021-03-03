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
"""Query containing supervision sentence information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.ingest.direct.regions.us_mo.ingest_views.us_mo_view_query_fragments import (
    NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT,
)

SUPERVISION_SENTENCE_STATUS_XREF_FRAGMENT = """
    classified_status_bw AS (
        /* Helper to classify statuses in ways that will help us figure out the types of their associated supervision
        sentences */
        SELECT
            classified_status_bw.*,
            CASE WHEN IS_PROBATION_REVOCATION = 1 THEN BW_SY ELSE NULL END AS PROBATION_REVOCATION_DATE,
            CASE WHEN IS_PROBATION_REVOCATION = 1 THEN BW_SCD ELSE NULL END AS PROBATION_REVOCATION_SCD
        FROM (
            SELECT
                BW_DOC, BW_CYC, BW_SCD, BW_SY, BW_SSO, BW_DCR, BW_DLU,
                CASE WHEN BW_SCD IN (
                    -- These statuses, when associated with a sentence, are strong indicators that that sentence is a
                    -- parole sentence.
                   '15I1200',  -- New Court Parole
                   '25I1200',  -- Court Parole-Addl Charge
                   '35I1200',  -- Court Parole-Revisit
                   '35I4100'   -- IS Compact-Parole-Revisit
                ) THEN BW_SCD ELSE NULL END AS PRIMARY_PAROLE_SCD,
                CASE WHEN BW_SCD IN (
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
                ) THEN BW_SCD ELSE NULL END AS SECONDARY_PAROLE_SCD,
                CASE WHEN BW_SCD IN (
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
                ) THEN BW_SCD ELSE NULL END AS PRIMARY_PROBATION_SCD,
                CASE WHEN BW_SCD IN (
                    -- These statuses, when associated with a sentence, are weak indicators that that sentence is a
                    -- probation sentence. We will mark a sentence as type PROBATION only if no parole statuses are
                    -- present.

                   -- This is a general status for being released to the field, but generally seems to show up with
                   -- probation statuses.
                   '40O7000' -- Rel to Field-DAI Other Sent
                ) THEN BW_SCD ELSE NULL END AS SECONDARY_PROBATION_SCD,
                CASE WHEN BW_SCD IN (
                    '45O2000',  -- Prob Rev-Technical
                    '45O2005',  -- Prob Rev-New Felony Conv
                    '45O2015',  -- Prob Rev-Felony Law Viol
                    '45O2010',  -- Prob Rev-New Misd Conv
                    '45O2020'   -- Prob Rev-Misd Law Viol
                ) THEN 1 ELSE 0 END AS IS_PROBATION_REVOCATION
            FROM {LBAKRDTA_TAK026} status_bw
        ) classified_status_bw
    ),
    supervision_sentence_status_xref_bv AS (
        /* Associates each status with a particular sentence, filtering for statuses associated with supervision
        sentences */
        SELECT
            BV_DOC, BV_CYC, BV_SEO, BV_FSO, BV_DCR, BV_DLU, BW_SCD, BW_SY, BW_SSO, BW_DCR, BW_DLU,
            PRIMARY_PAROLE_SCD,
            SECONDARY_PAROLE_SCD,
            PRIMARY_PROBATION_SCD,
            SECONDARY_PROBATION_SCD,
            IS_PROBATION_REVOCATION,
            PROBATION_REVOCATION_DATE,
            PROBATION_REVOCATION_SCD
        FROM
            {LBAKRDTA_TAK025} status_xref_bv
        LEFT OUTER JOIN
            classified_status_bw
        ON
            status_xref_bv.BV_DOC = classified_status_bw.BW_DOC AND
            status_xref_bv.BV_CYC = classified_status_bw.BW_CYC AND
            status_xref_bv.BV_SSO = classified_status_bw.BW_SSO
        WHERE CAST(BV_FSO as INT64) > 0
    )"""

DISTINCT_SUPERVISION_SENTENCE_IDS_FRAGMENT = """
    distinct_supervision_sentence_ids AS (
        SELECT DISTINCT BU_DOC, BU_CYC, BU_SEO, BU_FSO
        FROM non_investigation_supervision_sentences_bu
    )"""

SUPERVISION_SENTENCE_TYPE_CLASSIFIER_FRAGMENT = """
    collapsed_sentence_status_type_classification AS (
       SELECT
          BV_DOC, BV_CYC, BV_SEO,
          MIN(PRIMARY_PAROLE_SCD) AS PRIMARY_PAROLE_SCD,
          MIN(SECONDARY_PAROLE_SCD) AS SECONDARY_PAROLE_SCD,
          MIN(PRIMARY_PROBATION_SCD) AS PRIMARY_PROBATION_SCD,
          MIN(SECONDARY_PROBATION_SCD) AS SECONDARY_PROBATION_SCD,
          MAX(PROBATION_REVOCATION_DATE) AS PROBATION_REVOCATION_DATE,
          MAX(PROBATION_REVOCATION_SCD) AS PROBATION_REVOCATION_SCD
       FROM supervision_sentence_status_xref_bv
       GROUP BY BV_DOC, BV_CYC, BV_SEO
    ),
    supervision_sentence_type_classifier AS (
        /* Helper for finding the type of a supervision sentence based on the existence of certain statuses */
        SELECT
            BU_DOC, BU_CYC, BU_SEO,
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
            distinct_supervision_sentence_ids.BU_DOC = collapsed_sentence_status_type_classification.BV_DOC AND
            distinct_supervision_sentence_ids.BU_CYC = collapsed_sentence_status_type_classification.BV_CYC AND
            distinct_supervision_sentence_ids.BU_SEO = collapsed_sentence_status_type_classification.BV_SEO
    )"""

FULL_SUPERVISION_SENTENCE_INFO_FRAGMENT = """
    full_supervision_sentence_info AS (
        SELECT sentence_bs.*, non_investigation_supervision_sentences_bu.*
        FROM
            {LBAKRDTA_TAK022} sentence_bs
        JOIN
            non_investigation_supervision_sentences_bu
        ON
            sentence_bs.BS_DOC = non_investigation_supervision_sentences_bu.BU_DOC AND
            sentence_bs.BS_CYC = non_investigation_supervision_sentences_bu.BU_CYC AND
            sentence_bs.BS_SEO = non_investigation_supervision_sentences_bu.BU_SEO
    )"""

VIEW_QUERY_TEMPLATE = f"""
    WITH
    {SUPERVISION_SENTENCE_STATUS_XREF_FRAGMENT},
    {NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT},
    {DISTINCT_SUPERVISION_SENTENCE_IDS_FRAGMENT},
    {SUPERVISION_SENTENCE_TYPE_CLASSIFIER_FRAGMENT},
    {FULL_SUPERVISION_SENTENCE_INFO_FRAGMENT},
    supervision_sentence_status_xref_with_types AS (
        SELECT
            supervision_sentence_status_xref_bv.BV_DOC,
            supervision_sentence_status_xref_bv.BV_CYC,
            supervision_sentence_status_xref_bv.BV_SEO,
            supervision_sentence_status_xref_bv.BV_FSO,
            supervision_sentence_status_xref_bv.BW_SCD,
            supervision_sentence_status_xref_bv.BW_SY,
            supervision_sentence_status_xref_bv.BW_SSO,
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
            supervision_sentence_status_xref_bv.BV_DOC = supervision_sentence_type_classifier.BU_DOC AND
            supervision_sentence_status_xref_bv.BV_CYC = supervision_sentence_type_classifier.BU_CYC AND
            supervision_sentence_status_xref_bv.BV_SEO = supervision_sentence_type_classifier.BU_SEO
            AND (SENTENCE_TYPE != 'PROBATION' OR
                 supervision_sentence_type_classifier.PROBATION_REVOCATION_DATE IS NULL OR
                 supervision_sentence_status_xref_bv.BW_SY <=
                 supervision_sentence_type_classifier.PROBATION_REVOCATION_DATE)
    ),
    valid_sentences_with_status_xref AS (
        /* Join all statuses with valid, non-investigative sentences */
        SELECT
            BU_DOC, BU_CYC, BU_SEO,
            supervision_sentence_status_xref_with_types.IS_PROBATION_REVOCATION,
            supervision_sentence_status_xref_with_types.SENTENCE_TYPE,
            supervision_sentence_status_xref_with_types.BV_DOC,
            supervision_sentence_status_xref_with_types.BV_CYC,
            supervision_sentence_status_xref_with_types.BV_SEO,
            supervision_sentence_status_xref_with_types.BV_FSO,
            supervision_sentence_status_xref_with_types.BW_SSO,
            supervision_sentence_status_xref_with_types.BW_SCD,
            supervision_sentence_status_xref_with_types.BW_SY
        FROM
            distinct_supervision_sentence_ids
        LEFT OUTER JOIN
            supervision_sentence_status_xref_with_types
        ON
            supervision_sentence_status_xref_with_types.BV_DOC = distinct_supervision_sentence_ids.BU_DOC AND
            supervision_sentence_status_xref_with_types.BV_CYC = distinct_supervision_sentence_ids.BU_CYC AND
            supervision_sentence_status_xref_with_types.BV_SEO = distinct_supervision_sentence_ids.BU_SEO AND
            supervision_sentence_status_xref_with_types.BV_FSO = distinct_supervision_sentence_ids.BU_FSO
    ),
    ranked_supervision_sentence_status_xref AS (
        SELECT
            valid_sentences_with_status_xref.*,
            ROW_NUMBER() OVER (
                PARTITION BY BV_DOC, BV_CYC, BV_SEO
                ORDER BY
                    BW_SY DESC,
                    -- If there is a probation revocation status on a day, this is the
                    -- most important piece of information
                    IS_PROBATION_REVOCATION DESC,
                    -- If there are multiple field sequence numbers (FSO) with
                    -- the same status update on the same day, pick the largest
                    -- FSO, since this is likely the most recent status
                    BV_FSO DESC,
                    -- Otherwise, if multiple statuses are on the same day, pick the larger
                    -- status code, alphabetically, giving preference to close (9*)
                    -- statuses
                    BW_SCD DESC

            ) AS SUPERVISION_SENTENCE_STATUS_RECENCY_RANK
        FROM
            valid_sentences_with_status_xref
    ),
    most_recent_fso_and_status_for_sentence AS (
        /* Pick the FSO row with the most recent status */
        SELECT
            BU_DOC, BU_CYC, BU_SEO,
            ranked_supervision_sentence_status_xref.SENTENCE_TYPE,
            ranked_supervision_sentence_status_xref.BV_FSO AS MOST_RECENT_SUPERVISION_FSO,
            ranked_supervision_sentence_status_xref.BW_SSO AS MOST_RECENT_SENTENCE_STATUS_SSO,
            ranked_supervision_sentence_status_xref.BW_SCD AS MOST_RECENT_SENTENCE_STATUS_SCD,
            ranked_supervision_sentence_status_xref.BW_SY AS MOST_RECENT_SENTENCE_STATUS_DATE
        FROM
            ranked_supervision_sentence_status_xref
        WHERE SUPERVISION_SENTENCE_STATUS_RECENCY_RANK = 1
    )
    SELECT
        full_supervision_sentence_info.*,
        most_recent_fso_and_status_for_sentence.SENTENCE_TYPE,
        most_recent_fso_and_status_for_sentence.MOST_RECENT_SENTENCE_STATUS_SSO,
        most_recent_fso_and_status_for_sentence.MOST_RECENT_SENTENCE_STATUS_SCD,
        most_recent_fso_and_status_for_sentence.MOST_RECENT_SENTENCE_STATUS_DATE
    FROM
        full_supervision_sentence_info
    JOIN
        most_recent_fso_and_status_for_sentence
    ON
       full_supervision_sentence_info.BS_DOC = most_recent_fso_and_status_for_sentence.BU_DOC AND
       full_supervision_sentence_info.BS_CYC = most_recent_fso_and_status_for_sentence.BU_CYC AND
       full_supervision_sentence_info.BS_SEO = most_recent_fso_and_status_for_sentence.BU_SEO AND
       full_supervision_sentence_info.BU_FSO = most_recent_fso_and_status_for_sentence.MOST_RECENT_SUPERVISION_FSO
    """

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mo",
    ingest_view_name="tak022_tak024_tak025_tak026_offender_sentence_supervision",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BS_DOC, BS_CYC, BS_SEO",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
