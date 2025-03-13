# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query that generates info for all TDCJ sentences/charges.

    Sentence/Charge NOTES:
        - We don't get sentence and charge information in sperate files in Texas. We get
        all of the data for both of these entities in the same `Charges` table.
        - OFF_SID_NO, OFF_CAUSE_NO, OFF_COUNT, OFF_CAUSE_CNTY_NO should create 
          a unique ID for each sentence / charge.  As noted from TX:
            The following fields make up what should be the unique index for our offenses:
                OFF_SID_NO is the 8-digit inmate ID
                OFF_CAUSE_COUNTY_NO is the 3-digit Texas County Number
                OFF_CAUSE_NO is the 17-digit cause number assigned by the county
                OFF_COUNT is the 2-digit count number assigned to the specific conviction under this cause number
        - There are 16 scenarios in the historical transfer we received as of 3/11 where there are two rows that appear with 
          the same OFF_SID_NO <> OFF_CAUSE_COUNTY_NO <> OFF_CAUSE_NO <> OFF_COUNT.  In those cases, both rows appear in the _latest
          view because the OFF_CAUSE_NO value has different space formatting.  For these 16 cases, we trim all excess whitespace
          from OFF_CAUSE_NO and then pick the row that was most recently updated based on OFF_UPDATE_DATE
        - All charges we receive are convicted charges, so there is no other status but
        that one. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """

-- This CTE compiles the relevant charge information and then deduplicates rows in cases
-- where there are multiple rows with the same OFF_SID_NO, OFF_CAUSE_NO, OFF_COUNT, 
-- and OFF_CAUSE_CNTY_NO by taking the most recently updated row.  This only happens in 
-- 16 cases as of 3/11/2025.

    SELECT * EXCEPT(OFF_UPDATE_DATE)
    FROM (
        SELECT DISTINCT
            OFF_SID_NO,
            REPLACE(OFF_CAUSE_NO, ' ', '') AS OFF_CAUSE_NO,
            DATE(OFF_SENT_DATE) AS OFF_SENT_DATE,
            OFF_NCIC_CODE,
            OFF_DESC,
            OFF_STAT_CITATION,
            (OFF_AGGRAVATED = "Y") AS is_violent_bool,
            (OFF_SEX_OFF_REG = "Y") AS is_sex_offense_bool,
            COALESCE(OFF_COUNT, "ZZ") AS OFF_COUNT,
            OFF_CAUSE_CNTY_NO,
            CAST(OFF_UPDATE_DATE AS DATETIME) AS OFF_UPDATE_DATE
        FROM {Charges} c
        WHERE REPLACE(OFF_CAUSE_NO, ' ', '') IS NOT NULL
        -- We can't ingest any sentences where OFF_SENT_DATE (aka date imposed) is null (currently there are no cases where
        -- date imposed is null, so )
        AND OFF_SENT_DATE IS NOT NULL
        -- As of 3/11/2025, only 2 rows in which OFF_SENT_DATE < 1900-01-02
        AND DATE(OFF_SENT_DATE) > DATE(1900,1,2)
    ) sub
    QUALIFY ROW_NUMBER() OVER(PARTITION BY OFF_SID_NO, OFF_CAUSE_NO, OFF_COUNT, OFF_CAUSE_CNTY_NO ORDER BY OFF_UPDATE_DATE DESC) = 1

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="state_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
