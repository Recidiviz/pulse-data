# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query containing incarceration sentence information from the ELITE system.

Incarceration sentences are created from data combined from `elite_offendersentences`, `elite_offendersentenceterms`,
and `elite_offendersentenceaggs`. The first two contain different bits of information about a single sentence while
the third is a "roll-up" file that contains information aggregated from multiple sentences related to a single DOCR
"booking".
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH charges AS (
    SELECT 
        REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
        CHARGE_SEQ,
        OFFENSE_DATE,
        COUNTY_CODE,
        OFFENCE_TYPE,
        codes.OFFENCE_CODE,
        codes.severity_ranking,
        codes.description,
        INITIAL_COUNTS,
        COMMENT_TEXT,
        CHARGE_STATUS,
        orders.JUDGE_NAME,
        orders.ISSUING_AGY_LOC_ID,
    FROM {elite_offenderchargestable}
    LEFT JOIN {elite_orderstable} orders
    USING (OFFENDER_BOOK_ID, ORDER_ID)
    LEFT JOIN {RECIDIVIZ_REFERENCE_offense_codes} codes
    USING(OFFENCE_CODE)
),
sentences AS (
    SELECT 
        REPLACE(REPLACE(sentences.OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
        sentences.SENTENCE_SEQ,
        sentences.CHARGE_SEQ,
        sentences.EFFECTIVE_DATE,
        sentences.PROBABLE_RELEASE_DATE,
        sentences.SENTENCE_EXPIRY_DATE,
        sentences.SENTENCE_CALC_TYPE,
        aggs.PAROLE_DATE,
        aggs.CALC_POS_REL_DATE,
        aggs.OVR_POS_REL_DATE,
    FROM {elite_offendersentences} sentences
    LEFT JOIN {elite_offendersentenceaggs} aggs
    ON (REPLACE(REPLACE(sentences.OFFENDER_BOOK_ID,',',''), '.00', '') = aggs.OFFENDER_BOOK_ID)
),
terms AS (
    SELECT
        REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
        SENTENCE_SEQ,
        YEARS,
        MONTHS,
        DAYS,
        START_DATE
     FROM (
        SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY OFFENDER_BOOK_ID, SENTENCE_SEQ
            ORDER BY IF(END_DATE IS NULL, 1, 0), START_DATE, END_DATE
        ) AS term_priority
        FROM {elite_offendersentenceterms}
        -- We avoid bringing in "SUSPENDED" sentences that are placeholders in case someone's 
        -- probation is revoked. We get no info about these sentences other than a length (
        -- start/end dates are null).
        WHERE SENTENCE_TERM_CODE IS NULL
        OR SENTENCE_TERM_CODE != 'SUSP' 
    ) AS ranked_terms
    -- TODO(#10389): As of 6/28/23 there are about 45 sentences that have more than one
    -- non-suspended term. We are more or less arbitrarily (but deterministically!) picking
    -- one based on the START_DATE/END_DATE fields, but in the future it might make more
    -- sense to generate one sentence per term.
    WHERE term_priority = 1
)
SELECT
    sentences.OFFENDER_BOOK_ID,
    sentences.SENTENCE_SEQ,
    charges.CHARGE_SEQ,
    charges.COUNTY_CODE,
    charges.OFFENCE_TYPE,
    charges.OFFENCE_CODE,
    charges.OFFENSE_DATE,
    charges.severity_ranking,
    charges.description,
    charges.INITIAL_COUNTS,
    charges.COMMENT_TEXT,
    charges.CHARGE_STATUS,
    charges.JUDGE_NAME,
    charges.ISSUING_AGY_LOC_ID,
    sentences.EFFECTIVE_DATE,
    sentences.PROBABLE_RELEASE_DATE,
    sentences.SENTENCE_EXPIRY_DATE,
    sentences.SENTENCE_CALC_TYPE,
    sentences.PAROLE_DATE,
    sentences.CALC_POS_REL_DATE,
    sentences.OVR_POS_REL_DATE, 
    terms.YEARS,
    terms.MONTHS,
    terms.DAYS,
    terms.START_DATE
    FROM sentences
    LEFT JOIN charges
    USING (OFFENDER_BOOK_ID, CHARGE_SEQ)
    LEFT JOIN terms
    USING (OFFENDER_BOOK_ID, SENTENCE_SEQ)
    -- There are a handful of malformed dates in the EFFECTIVE_DATE field of elite_orderstable
    -- as of 6/28/23. This excludes those rows from results, since there are so few.
    WHERE CAST(EFFECTIVE_DATE AS DATETIME) > CAST('1900-01-01' AS DATETIME)
    OR EFFECTIVE_DATE IS NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_incarceration_sentences",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
