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
"""Query for all external ids ever associated with any person in the DOC or PBPP systems."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.ingest.direct.regions.us_pa.ingest_views.templates_person_external_ids import \
    MASTER_STATE_IDS_FRAGMENT_V2
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""WITH
{MASTER_STATE_IDS_FRAGMENT_V2}
SELECT 
  recidiviz_master_person_id,
  STRING_AGG(DISTINCT control_number, ',' ORDER BY control_number) AS control_numbers,
  STRING_AGG(DISTINCT state_id, ',' ORDER BY state_id) AS state_ids,
  STRING_AGG(DISTINCT parole_number, ',' ORDER BY parole_number) AS parole_numbers
FROM recidiviz_master_person_ids
GROUP BY recidiviz_master_person_id;"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='person_external_ids',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols=None,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
