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
"""Query containing person demographic and identifier information from PBPP."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """WITH
-- TODO(#8895): Don't pull in contacts here once we have a real source for names.
names_from_parole_contacts as (
  SELECT
    * EXCEPT (recency_rank)
  FROM (
    SELECT
      * EXCEPT (created_date),
      -- Look at the most recent contact that has a name.
      ROW_NUMBER() OVER (PARTITION BY parole_number ORDER BY created_date DESC) AS recency_rank
    FROM (
      SELECT
        parole_number,
        first_name,
        last_name,
        suffix,
        created_date
      FROM
        {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY}
      WHERE
        first_name IS NOT NULL
        AND last_name IS NOT NULL ) )
  WHERE
    recency_rank = 1
)
SELECT 
  offender.ParoleNumber, offender.OffRaceEthnicGroup, offender.OffSex,
  names.first_name, names.last_name, names.suffix
FROM {dbo_Offender} offender
LEFT JOIN names_from_parole_contacts names
ON offender.ParoleNumber = names.parole_number
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_pa",
    ingest_view_name="dbo_Offender_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols=None,
    materialize_raw_data_table_views=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
