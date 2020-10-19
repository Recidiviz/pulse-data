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

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
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
                    BV_FSO DESC
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
        	sentence_status_xref.BW_SSO AS MOST_RECENT_SENTENCE_STATUS_SSO,
        	sentence_status_xref.BW_SCD AS MOST_RECENT_SENTENCE_STATUS_SCD,
        	sentence_status_xref.BW_SY AS MOST_RECENT_SENTENCE_STATUS_DATE
        FROM
        	sentence_status_xref
        WHERE RECENCY_RANK_WITHIN_SENTENCE = 1
    )
    SELECT
        sentence_bs.*,
        sentence_inst_bt.*,
        most_recent_status_by_sentence.MOST_RECENT_SENTENCE_STATUS_SSO,
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
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_mo',
    ingest_view_name='tak022_tak023_tak025_tak026_offender_sentence_institution_v2',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='BS_DOC, BS_CYC, BS_SEO',
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
