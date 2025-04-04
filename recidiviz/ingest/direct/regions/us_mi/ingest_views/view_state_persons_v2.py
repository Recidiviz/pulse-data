# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing MDOC client information."""

# pylint: disable=anomalous-backslash-in-string
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
latest_booking_profiles AS (
  -- MAX is being used to pick nonnull values if there are any or if hispanic_flag
  -- was ever set to 1.
  SELECT 
    offender_booking_id,
    MAX(cultural_affiliation_id) AS cultural_affiliation_id,
    MAX(hispanic_flag) AS hispanic_flag
  FROM {ADH_OFFENDER_BOOKING_PROFILE}
  GROUP BY offender_booking_id
)
SELECT DISTINCT
  LPAD(p.offender_number, 7, "0") AS offender_number,
  p.last_name,
  p.first_name,
  p.middle_name,
  p.name_suffix,
  p.birth_date,
  p.gender_id,
  p.race_id,
  bp.cultural_affiliation_id,
  bp.hispanic_flag,
FROM
  {ADH_OFFENDER_BOOKING} ids
INNER JOIN
  {ADH_OFFENDER_PROFILE_SUMMARY_WRK} p
ON 
  ids.offender_id = p.offender_id 
LEFT JOIN latest_booking_profiles bp
ON
  bp.offender_booking_id = p.offender_booking_id
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_persons_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
