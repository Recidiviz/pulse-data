# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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

Incarceration sentences are created from data combined from `elite_offendersentences` and `elite_offendersentenceterms`.
These contain different bits of information about a single sentence. There can be many terms
per sentence, and many sentences per booking.
"""

from recidiviz.common.constants.reasonable_dates import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- The results of this CTE contain one row for each sentence and set of dates associated
-- with it.
sentences AS (
    SELECT DISTINCT
        REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
        SENTENCE_SEQ,
        CHARGE_SEQ,
        -- NULL out PROBABLE_RELEASE_DATE values that are not within a reasonable range. This clears 
        -- the PROBABLE_RELEASE_DATE values in 3 rows. 
        IF(CAST(sentences.PROBABLE_RELEASE_DATE AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND '{STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND}',
            CAST(sentences.PROBABLE_RELEASE_DATE AS DATETIME), 
            CAST(NULL AS DATETIME)) AS PROBABLE_RELEASE_DATE,
        -- NULL out SENTENCE_EXPIRY_DATE values that are not within a reasonable range. This clears 
        -- the SENTENCE_EXPIRY_DATE values in the same 3 rows as those that have invalid PROBABLE_RELEASE_DATE values. 
        IF(CAST(sentences.SENTENCE_EXPIRY_DATE AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND '{STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND}', 
            CAST(sentences.SENTENCE_EXPIRY_DATE AS DATETIME), 
            CAST(NULL AS DATETIME)) AS SENTENCE_EXPIRY_DATE,
        START_DATE,
        CONSEC_TO_SENTENCE_SEQ,
        'INCARCERATION' AS sentence_type,
        COALESCE(MODIFY_DATETIME, CREATE_DATETIME) AS SENTENCE_UPDT_DTM
    FROM {{elite_offendersentences@ALL}} sentences
),
-- This CTE pulls only the latest information about whether this is a life sentence.
-- This is necessarily separate because our schema is not designed to hold updates
-- to this information over time, but it can actually be updated over time in ND.
-- (An example of this is someone having their sentence amended from Life to a term 
-- of 50 years after serving 25 years in prison).
life_status AS (
    SELECT 
        REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
        SENTENCE_SEQ,
        SENTENCE_CALC_TYPE
    FROM {{elite_offendersentences}}
),
-- This CTE is disjointed from the previous one and will be joined in the final subquery
-- to factor charge information into the entity. There can be a one to one or a one to
-- many relationship between sentences and charges; sequences of charges are ordered by the 
-- CHARGE_SEQ field.
charges AS (
    SELECT 
        REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
        CHARGE_SEQ,
        -- NULL out OFFENSE_DATE values that are not within a reasonable range. This clears 
        -- the OFFENSE_DATE values in 9 rows. 
        IF(CAST(OFFENSE_DATE AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp, 
            CAST(OFFENSE_DATE AS DATETIME), 
            CAST(NULL AS DATETIME)) AS OFFENSE_DATE,
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
        orders.COURT_INFO_ID,
        orders.CONVICTION_DATE,
        -- Court orders relating to a small number of charges do not appear in the 
        -- elite_orderstable. The date the charge record was created is typically not
        -- more than a few days after the conviction order, where one exists, so we use
        -- that as a proxy for date imposed when there is no associated court order.
        charges.CREATE_DATETIME AS CHARGE_CREATE_DTM
    FROM {{elite_offenderchargestable}} charges
    LEFT JOIN {{elite_orderstable}} orders
    USING (OFFENDER_BOOK_ID, ORDER_ID)
    LEFT JOIN {{RECIDIVIZ_REFERENCE_offense_codes}} codes
    USING(OFFENCE_CODE)
    -- Filter out 6 rows where CONVICTION_DATE values are not within a reasonable range.
    WHERE CAST(orders.CONVICTION_DATE AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp
),
-- This CTE collects identifiers for all sentences being served consecutively.
-- This is a temporary patch that will be removed once we have completed TODO(#35310). 
-- The purpose is to refrain from hydrating child sentences for which the parent sentence
-- does not exist in the data. Once we are sure we have all of the data in each of these tables,
-- we can migrate out the bad associations if these parent cases truly do not exist.
consecutive_sentences AS (
SELECT 
    child.OFFENDER_BOOK_ID, 
    child.SENTENCE_SEQ, 
    child.parent_sentence_external_id_array
FROM (
  SELECT 
    REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
    SENTENCE_SEQ,
    STRING_AGG(CONCAT(REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', ''),'-', CONSEC_TO_SENTENCE_SEQ) ORDER BY CONSEC_TO_SENTENCE_SEQ) AS parent_sentence_external_id_array
  FROM {{elite_offendersentences}} child
  GROUP BY OFFENDER_BOOK_ID, SENTENCE_SEQ
) child
LEFT JOIN {{elite_offendersentences}} parent
ON(CONCAT(parent.OFFENDER_BOOK_ID, '-', parent.SENTENCE_SEQ) = child.parent_sentence_external_id_array)
WHERE NOT (child.parent_sentence_external_id_array IS NOT NULL 
AND parent.OFFENDER_BOOK_ID IS NULL)
),
-- This CTE collects sentence term information, which is stored separately. Even if there
-- are many sentences associated with a booking, they can each have their own set of terms
-- in this table.
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
        FROM {{elite_offendersentenceterms}}
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
SELECT DISTINCT
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
    charges.COURT_INFO_ID,
    sentences.PROBABLE_RELEASE_DATE,
    sentences.SENTENCE_EXPIRY_DATE,
    life_status.SENTENCE_CALC_TYPE,
    COALESCE(charges.CONVICTION_DATE, charges.CHARGE_CREATE_DTM) AS IMPOSED_DATE,
    sentences.sentence_type,
    sentences.SENTENCE_UPDT_DTM,
    terms.YEARS,
    terms.MONTHS,
    terms.DAYS,
    consecutive_sentences.parent_sentence_external_id_array,
    (charges.COURT_INFO_ID IS NOT NULL) AS has_sentence_group
    FROM sentences
    -- Inner join to charge information because we cannot ingest a sentence without 
    -- any charges.
    JOIN charges
    USING (OFFENDER_BOOK_ID, CHARGE_SEQ)
    LEFT JOIN terms
    USING (OFFENDER_BOOK_ID, SENTENCE_SEQ)
    LEFT JOIN consecutive_sentences
    USING (OFFENDER_BOOK_ID, SENTENCE_SEQ)
    LEFT JOIN life_status
    USING (OFFENDER_BOOK_ID, SENTENCE_SEQ)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
