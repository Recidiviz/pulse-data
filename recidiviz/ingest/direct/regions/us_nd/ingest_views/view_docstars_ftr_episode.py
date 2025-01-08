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
"""Query containing ftr episode information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  EPISODE_ID,
  ASSIGNED_PROVIDER_ID,
  LOCATION_ID,
  STATUS,
  SUBMITTED,
  ADMITTED_DATE,
  STATUS_DATE,
  ALLOW_VIEWING,
  PEER_SUPPORT_OFFERED,
  PEER_SUPPORT_ACCEPTED,
  SN_LAST_UPDATED_DATE,
  COORDINATOR_GUID,
  PREFERRED_PROVIDER_ID,
  PREFERRED_LOCATION_ID,
  STRENGTHS,
  NEEDS,
  IS_CLINICAL_ASSESSMENT,
  ASSESSMENT_LOCATION,
  REFERRAL_REASON,
  SPECIALIST_LAST_NAME,
  SPECIALIST_FIRST_NAME,
  SPECIALIST_INITIAL,
  SUBMITTED_BY,
  SUBMITTED_BY_NAME,
  SID,
  CURRENT_NEEDS,
FROM {docstars_ftr_episode}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_ftr_episode",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
