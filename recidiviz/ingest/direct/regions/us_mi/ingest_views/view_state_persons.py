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
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
  offender_name_max_seq AS (
    SELECT
      offender_id,
      last_name,
      first_name,
      middle_name,
      name_suffix,
      birth_date,
      sex_id
    FROM (
      SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY offender_id 
          ORDER BY SAFE_CAST(sequence_number AS INT64) DESC) AS seq_number_order
        FROM {ADH_OFFENDER_NAME}
    ) AS o
    WHERE seq_number_order = 1
  ),
  latest_bookings AS (
    SELECT
      offender_booking_id,
      offender_id
    FROM (
      SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY offender_id 
          ORDER BY begin_date DESC
      ) AS booking_order 
      FROM {ADH_OFFENDER_BOOKING}
    ) AS b
    WHERE booking_order = 1),
  latest_profiles AS (
    SELECT
      offender_booking_id,
      cultural_affiliation_id,
      multiracial_flag,
      caucasian_racial_flag,
      native_american_racial_flag,
      black_racial_flag,
      asian_racial_flag,
      hispanic_flag,
      racial_identification_code_id
    FROM (
      SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY offender_booking_id 
          ORDER BY profile_date DESC
        ) AS profile_order
      FROM {ADH_OFFENDER_BOOKING_PROFILE}
      -- TODO(#11580) Remove this check once everything for MI is parsing correctly
      WHERE SAFE_CAST(offender_booking_id AS INT64) IS NOT NULL
    ) AS p
    -- TODO(#11580) Remove this check once everything for MI is parsing correctly
    WHERE SAFE_CAST(offender_booking_id AS INT64) IS NOT NULL
    AND profile_order = 1)
SELECT 
  offender_number,
  last_name,
  first_name,
  middle_name,
  name_suffix,
  n.birth_date,
  sex_id,
  cultural_affiliation_id,
  multiracial_flag,
  caucasian_racial_flag,
  native_american_racial_flag,
  black_racial_flag,
  asian_racial_flag,
  hispanic_flag,
  racial_identification_code_id
FROM
  {ADH_OFFENDER} ids
LEFT JOIN
  offender_name_max_seq n
ON
  ids.offender_id = n.offender_id
LEFT JOIN
  latest_bookings b
ON
  b.offender_id = n.offender_id
LEFT JOIN
  latest_profiles p
ON
  (b.offender_booking_id = p.offender_booking_id)
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mi",
    ingest_view_name="state_persons",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="offender_number",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
