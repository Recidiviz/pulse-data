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
"""Query containing sentence, sentence group, and charge information."""

from recidiviz.ingest.direct.regions.us_ia.ingest_views.query_fragments import (
    PENALTIES_TO_KEEP,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
  WITH 
    {PENALTIES_TO_KEEP},

    -- This CTE compiles all penalty modifier information associated with a penalty over time.
    -- Notes:
    --   * We set update_datetime to EnteredDt (aka last update date in ICON) or update_datetime, whichever is earlier, since 
    --     there are many record updates that would have happened before we first started receiving the data.
    --   * For the first penalty modifier ledger record for each penalty, we reset update_datetime to reflect
    --     the earliest date associated with the penalty/sentence (to account for cases where we didn't receive our 
    --     first data transfer until after the sentence started)
    status_info_from_penalties AS (
      SELECT * EXCEPT(update_datetime),
        CASE WHEN rn = 1 THEN LEAST(COALESCE(SentenceDt, DATE(9999,1,1)), update_datetime) 
          ELSE update_datetime END AS update_datetime
      FROM (
        SELECT *,
          ROW_NUMBER() OVER(PARTITION BY OffenderCd, PenaltyId ORDER BY update_datetime) AS rn
        FROM (
          SELECT DISTINCT
            OffenderCd,
            PenaltyId,
            SentencePenaltyModifier,
            LEAST(DATE(penalty.EnteredDt), DATE(update_datetime)) AS update_datetime,
            DATE(sentence.SentenceDt) AS SentenceDt,
          FROM {{IA_DOC_Penalties@ALL}} penalty
          LEFT JOIN {{IA_DOC_Sentences}} sentence USING(OffenderCd, SentenceId)
        )
      )
    ),

    -- This CTE pulls in sentence end date for each penalty since and end date tells us
    -- that the penalty has a completed status
    status_info_from_sentences AS (
      SELECT DISTINCT
        penalty.OffenderCd,
        penalty.PenaltyId,
        DATE(SentenceEndDt) AS SentenceEndDt,
        CASE 
          -- There's just one case where sentence end date is absurdly early, but I think that's a typo
          -- so let's just update_datetime to sentence date in that case
          WHEN DATE(sentence.SentenceEndDt) < DATE(1900,1,2) THEN DATE(sentence.SentenceDt)
          -- there are a small number of end dates that are 9999-12-31 from before the ICON migration,
          -- which the IA data team have confirmed are closed sentences with unknown end dates
          -- so we'll just set the update_datetime for those end dates as sentence date for now
          -- since they're all pre-2004
          WHEN DATE(sentence.SentenceEndDt) = DATE(9999,12,31) THEN DATE(sentence.SentenceDt)
          ELSE DATE(sentence.SentenceEndDt) 
          END AS update_datetime
      FROM {{IA_DOC_Penalties}} penalty
      LEFT JOIN {{IA_DOC_Sentences}} sentence USING(OffenderCd, SentenceId)
      WHERE SentenceEndDt IS NOT NULL
    ),

  -- This CTE combines the two sources of status information and creates a sequence_num variable
  combined_ledger AS (
    SELECT
      OffenderCd,
      PenaltyId,
      update_datetime,
      LAST_VALUE(SentencePenaltyModifier IGNORE NULLS) OVER(w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS SentencePenaltyModifier,
      LAST_VALUE(SentenceEndDt IGNORE NULLS) OVER(w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS SentenceEndDt,
      ROW_NUMBER() OVER(w) as sequence_num
    FROM status_info_from_penalties
    FULL OUTER JOIN status_info_from_sentences USING(OffenderCd, PenaltyId, update_datetime)
    INNER JOIN penalties_to_keep USING(PenaltyId)
    WINDOW w AS (PARTITION BY OffenderCd, PenaltyId ORDER BY update_datetime)
  )

  -- Finally, we only keep rows where either SentenceEndDt is NULL (meaning the sentence has an open status)
  -- or it's the earliest row where we see a valued SentenceEndDt for a penalty (to capture the earliest date the sentence was closed)
  select *,
  from combined_ledger
  QUALIFY 
    SentenceEndDt IS NULL OR 
    RANK() OVER(PARTITION BY OffenderCd, PenaltyId, (SentenceEndDt IS NOT NULL) ORDER BY update_datetime) = 1 

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="sentence_statuses",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
