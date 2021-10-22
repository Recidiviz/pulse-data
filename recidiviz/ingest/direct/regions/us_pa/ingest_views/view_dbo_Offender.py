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

from recidiviz.ingest.direct.regions.us_pa.ingest_views.templates_person_external_ids import (
    PRIMARY_STATE_IDS_FRAGMENT_V2,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""WITH
{PRIMARY_STATE_IDS_FRAGMENT_V2},
base_query AS (
  SELECT 
    ids.recidiviz_primary_person_id, ParoleNumber, OffRaceEthnicGroup, OffSex,
    ROW_NUMBER() OVER (PARTITION BY recidiviz_primary_person_id ORDER BY LastModifiedDate DESC) AS recency_rank
  FROM {{dbo_Offender}} offender
  JOIN
  (SELECT DISTINCT recidiviz_primary_person_id, parole_number FROM recidiviz_primary_person_ids) ids
  ON ids.parole_number = offender.ParoleNumber
),
races_ethnicities AS (
  SELECT 
    recidiviz_primary_person_id,
    STRING_AGG(DISTINCT OffRaceEthnicGroup, ',' ORDER BY OffRaceEthnicGroup) AS races_ethnicities_list
  FROM base_query
  GROUP BY recidiviz_primary_person_id
)
SELECT * EXCEPT (recency_rank)
FROM base_query
LEFT OUTER JOIN
races_ethnicities
USING (recidiviz_primary_person_id)
WHERE recency_rank = 1
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_pa",
    ingest_view_name="dbo_Offender",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols=None,
    materialize_raw_data_table_views=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
