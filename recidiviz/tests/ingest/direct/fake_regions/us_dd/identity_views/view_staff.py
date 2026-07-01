# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""A simple staff identity ingest view."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.tests.ingest.direct import fake_regions

VIEW_QUERY_TEMPLATE = """
SELECT
  StaffId,
  BadgeId,
  FirstName,
  LastName,
  Email
FROM {identity_staff}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_dd",
    ingest_view_name="staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
    region_module=fake_regions,
)
