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

    -- This CTE calculates the earliest sentence date associated with each charge
    -- (to use for inferring the initial update datetime later on)
    earliest_sentence_date_per_charge AS (
      SELECT  
        ChargeId,
        MIN(DATE(SentenceDt)) AS earliest_sentence_date
      FROM {{IA_DOC_Charges}}
      LEFT JOIN {{IA_DOC_Sentences}} USING(OffenderCd, ChargeId)
      GROUP BY 1
    ),

    -- This CTE compiles all charge date information associated with a charge over time.
    -- Notes:
    --   * We set update_datetime to EnteredDt (aka last update date in ICON) or update_datetime, whichever is earlier, since 
    --     there are many record updates that would have happened before we first started receiving the data.
    charge_dates_ledger AS (
      SELECT DISTINCT
        charges.OffenderCd,
        charges.ChargeId,
        -- only 3 cases that get nulled out
        CASE WHEN DATE(TDD) > DATE(2300,1,1) OR DATE(TDD) < DATE(1900,1,2) THEN NULL ELSE DATE(TDD) END AS TDD,
        -- more get nulled out here but it all seems to be from when they migrated to ICON
        CASE WHEN DATE(SDD) > DATE(2300,1,1) OR DATE(SDD) < DATE(1900,1,2) THEN NULL ELSE DATE(SDD) END AS SDD,
        LEAST(DATE(charges.EnteredDt), DATE(charges.update_datetime)) AS update_datetime
      FROM {{IA_DOC_Charges@ALL}} charges
      INNER JOIN penalties_to_keep USING(ChargeId)
    )

    -- Create sequence_num and for the first length ledger record for each charge, and
    -- reset update_datetime to reflect the earliest date associated with the charge 
    -- (to account for cases where we didn't receive our first data transfer until after the charge started)
    -- Set max date to the max between TDD and SDD
    SELECT * EXCEPT(update_datetime, earliest_sentence_date, TDD, SDD),
      CASE WHEN sequence_num = 1 THEN LEAST(COALESCE(earliest_sentence_date, DATE(9999,1,1)), update_datetime) ELSE update_datetime END AS update_datetime,
      NULLIF(GREATEST(COALESCE(TDD, DATE(1000,1,1)), COALESCE(SDD, DATE(1000,1,1))), DATE(1000,1,1)) as max_date,
    FROM (
      SELECT *,
        ROW_NUMBER() OVER(PARTITION BY OffenderCd, ChargeId ORDER BY update_datetime) AS sequence_num
      FROM charge_dates_ledger
      LEFT JOIN earliest_sentence_date_per_charge USING(ChargeId)
      WHERE TDD IS NOT NULL OR SDD IS NOT NULL
    )

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="sentence_group_lengths",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
