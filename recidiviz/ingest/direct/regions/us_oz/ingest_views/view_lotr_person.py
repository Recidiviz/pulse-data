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
"""
This file contains an ingest view related to us_oz_lotr data.

The view query takes data from three raw tables:
  - lotr_fellowship: Has basic person info
  - lotr_demographics: Has demographic info for each person
  - lotr_roles: Has aliases and roles for each person

These three tables will hydrate four entities:
  - state_person_external_id (simply using ID from each table)
  - state_person
  - state_person_race
  - state_person_alias
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  LotrPerson.ID,
  LotrPerson.FirstName,
  LotrPerson.LastName,
  CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', LotrPerson.StartDate) AS DATE) as StartDate,
  LotrRace.Race,
  LotrRole.Alias,
  LotrRole.Role
FROM {lotr_fellowship} AS LotrPerson
LEFT JOIN {lotr_demographics} AS LotrRace
ON LotrPerson.ID = LotrRace.ID
LEFT JOIN {lotr_roles} AS LotrRole
ON LotrPerson.ID = LotrRole.ID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_oz",
    ingest_view_name="lotr_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
