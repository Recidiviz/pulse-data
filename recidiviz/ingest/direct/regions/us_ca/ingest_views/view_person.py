# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query containing CDCR client information. Currently only looks at folks who are on 
supervision.

Tickets to improve this view:

Things to consider making tickets for:
1. Parsing the first few characters of the CDCNO to add information.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
  SELECT
    OffenderId,
    FirstName,
    MiddleName,
    LastName,
    NameSuffix,
    Birthday,
    Sex,
    Race,
    Ethnic,
    AddressType
  FROM {PersonParole}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca", ingest_view_name="person", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
