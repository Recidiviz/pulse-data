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
"""Query containing MDOC employment period information using data from OMNI."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  emp.offender_booking_id,
  sequence_number,
  (DATE(employment_date)) as employment_date,
  (DATE(termination_date)) as termination_date,
  employer_name,
  position,
  employment_status_id,
  termination_reason_code,
  occupation
FROM {ADH_OFFENDER_EMPLOYMENT} emp
INNER JOIN {ADH_OFFENDER_BOOKING} book on emp.offender_booking_id = book.offender_booking_id

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="employment_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="offender_booking_id, sequence_number",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
