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

    -- This CTE compiles all penalty length information associated with a penalty over time.
    -- Notes:
    --   * When the penalty has a modified penalty length, we pull the modified penalty length information.
    --   * Otherwise, we pull the original penalty length information.
    --   * We set update_datetime to EnteredDt (aka last update date in ICON) or update_datetime, whichever is earlier, since 
    --     there are many record updates that would have happened before we first started receiving the data.
    --   * We keep only penalties whose original or modified penalty storage type is "Days"
    penalty_lengths_ledger AS (
      SELECT DISTINCT
        penalty.OffenderCd,
        penalty.PenaltyId,
        CASE WHEN COALESCE(ModifierMaximumYears, ModifierMaximumMonths, ModifierMaximumDays) IS NOT NULL
          THEN ModifierMaximumDays
          ELSE PenaltyDays
          END AS penalty_length_days,
        CASE WHEN COALESCE(ModifierMaximumYears, ModifierMaximumMonths, ModifierMaximumDays) IS NOT NULL
          THEN ModifierMaximumMonths
          ELSE PenaltyMonths
          END AS penalty_length_months,
        CASE WHEN COALESCE(ModifierMaximumYears, ModifierMaximumMonths, ModifierMaximumDays) IS NOT NULL
          THEN ModifierMaximumYears
          ELSE PenaltyYears
          END AS penalty_length_years,
        LEAST(DATE(penalty.EnteredDt), DATE(penalty.update_datetime)) AS update_datetime,
        DATE(sentence.SentenceDt) AS SentenceDt
      FROM {{IA_DOC_Penalties@ALL}} penalty
      INNER JOIN {{IA_DOC_Sentences}} sentence USING(OffenderCd, SentenceId)
      INNER JOIN penalties_to_keep USING(PenaltyId)
      WHERE PenaltyStorageType = 'Days' OR ModifierStorageType = 'Days'
    )

    -- Create sequence_num and for the first length ledger record for each penalty, and
    -- reset update_datetime to reflect the earliest date associated with the associated sentence 
    -- (to account for cases where we didn't receive our first data transfer until after the sentence started)
    SELECT * EXCEPT(update_datetime, SentenceDt),
      CASE WHEN sequence_num = 1 THEN LEAST(COALESCE(SentenceDt, DATE(9999,1,1)), update_datetime) ELSE update_datetime END AS update_datetime
    FROM (
      SELECT *,
        ROW_NUMBER() OVER(PARTITION BY OffenderCd, PenaltyId ORDER BY update_datetime) AS sequence_num
      FROM penalty_lengths_ledger
    )

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="sentence_lengths",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
