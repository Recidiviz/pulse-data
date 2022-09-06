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
"""Query containing institutional (i.e. prison) sentence information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH sentence_status_xref AS (
        /* Join all statuses with their associated sentences, create a recency
           rank for every status among all statuses for that sentence. */
        SELECT
            status_xref_bv.*,
            status_bw.*,
            ROW_NUMBER() OVER (
                PARTITION BY BV_DOC, BV_CYC, BV_SEO
                ORDER BY
                    BW_SY DESC,
                    -- If multiple statuses are on the same day, pick the larger
                    -- status code, alphabetically, giving preference to close (9*)
                    -- statuses
                    BW_SCD DESC,
                    -- If there are multiple field sequence numbers (FSO) with
                    -- the same status update on the same day, pick the largest
                    -- FSO.
                    BV_FSO DESC,
                    BV_SSO # Needed to make deterministic
            ) AS RECENCY_RANK_WITHIN_SENTENCE
        FROM
        	-- Note: We explicitly do not filter out probation sentences here -
        	-- if the SEO is the same, there may be relevant status dates that
        	-- we want to capture.
            {LBAKRDTA_TAK025} status_xref_bv
        LEFT OUTER JOIN
            {LBAKRDTA_TAK026} status_bw
        ON
            status_xref_bv.BV_DOC = status_bw.BW_DOC AND
            status_xref_bv.BV_CYC = status_bw.BW_CYC AND
            status_xref_bv.BV_SSO = status_bw.BW_SSO
    ),
    most_recent_status_by_sentence AS (
        /* Select the most recent status for a given sentence */
        SELECT
        	sentence_status_xref.BV_DOC,
        	sentence_status_xref.BV_CYC,
        	sentence_status_xref.BV_SEO,
        	sentence_status_xref.BW_DOC,
            sentence_status_xref.BW_CYC,
            sentence_status_xref.BW_SSO,
        	sentence_status_xref.BW_SSO AS MOST_RECENT_SENTENCE_STATUS_SSO,
        	sentence_status_xref.BW_SCD AS MOST_RECENT_SENTENCE_STATUS_SCD,
        	sentence_status_xref.BW_SY AS MOST_RECENT_SENTENCE_STATUS_DATE
        FROM
        	sentence_status_xref
        WHERE RECENCY_RANK_WITHIN_SENTENCE = 1
    ),
    shock_sentence AS ( #TODO(#15052): Revisit codes as we learn more about 120 day sentence from MO
        /* Codes given indicating possible 120 day shock incarceration  */
        SELECT DISTINCT BW_DOC, BW_CYC, BW_SSO, 'SENTENCE: 120 DAY' AS SENT_FLAG
        FROM {LBAKRDTA_TAK026}
        WHERE BW_SCD IN ('10I1010','10I1020','10I1030','10I1040','10I1050','10I1060','20I1010','20I1020','20I1040','20I1050','20I1060','30I1000','30I1010','30I1020',     '30I1030','30I1040','30I1050','30I1060','40I1060','40I2050','40I2055','40I2060','40I2065','40I2070','40I2105','40I2110','40I2115','40I2120','40I2130','40I2135','40I2145','40I2150','40I2160','40I2350','40I2355','40I2365','40I2370','40I2400','40I2405', '40I2410','40I2415','40I2420', '40I2100','40I7020','40I7030','40I7060','40I1060','40I3060','40I8060','40N1060','40N3060','40N8060','50N1060')
    )
    SELECT
        sentence_bs.BS_DOC,
        sentence_bs.BS_CYC,
        sentence_bs.BS_SEO,
        sentence_bs.BS_SCF,
        sentence_bs.BS_ASO,
        sentence_bs.BS_NCI,
        sentence_bs.BS_CLT,
        sentence_bs.BS_CNT,
        sentence_bs.BS_CLA,
        sentence_bs.BS_CCI,
        sentence_bs.BS_CRQ,
        sentence_bs.BS_CNS,
        sentence_bs.BS_PD,
        sentence_bs.BS_DO,
        sentence_bs.BS_COD,

        sentence_inst_bt.BT_SD,
        sentence_inst_bt.BT_SLY,
        sentence_inst_bt.BT_SLM,
        sentence_inst_bt.BT_SLD,
        sentence_inst_bt.BT_CRR,
        sentence_inst_bt.BT_PC,
        sentence_inst_bt.BT_PIE,
        sentence_inst_bt.BT_EM,
        sentence_inst_bt.BT_SDI,

        shock_sentence.SENT_FLAG,
        most_recent_status_by_sentence.MOST_RECENT_SENTENCE_STATUS_SCD,
        most_recent_status_by_sentence.MOST_RECENT_SENTENCE_STATUS_DATE
    FROM
        {LBAKRDTA_TAK022} sentence_bs
    JOIN
        {LBAKRDTA_TAK023} sentence_inst_bt
    ON
        sentence_bs.BS_DOC = sentence_inst_bt.BT_DOC AND
        sentence_bs.BS_CYC = sentence_inst_bt.BT_CYC AND
        sentence_bs.BS_SEO = sentence_inst_bt.BT_SEO
    LEFT OUTER JOIN
        most_recent_status_by_sentence
    ON
        sentence_bs.BS_DOC = most_recent_status_by_sentence.BV_DOC AND
        sentence_bs.BS_CYC = most_recent_status_by_sentence.BV_CYC AND
        sentence_bs.BS_SEO = most_recent_status_by_sentence.BV_SEO
    LEFT JOIN 
        shock_sentence
     ON 
        most_recent_status_by_sentence.BW_DOC = shock_sentence.BW_DOC AND
        most_recent_status_by_sentence.BW_CYC = shock_sentence.BW_CYC AND
        most_recent_status_by_sentence.BW_SSO = shock_sentence.BW_SSO
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mo",
    ingest_view_name="offender_sentence_institution",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BS_DOC, BS_CYC, BS_SEO",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
