# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Utilities for querying supervisors and officers."""

import json
from collections import defaultdict

import attr

from recidiviz.big_query.big_query_client import BigQueryClient

_SUPERVISORS_AND_OFFICERS_QUERY = """
WITH current_ix_officers as (
    SELECT external_id AS staff_external_id, state_code, full_name, supervision_district, supervisor_external_ids, email, from `recidiviz-123.outliers_views.supervision_officers_materialized` where state_code="US_ND" and ARRAY_LENGTH(supervisor_external_ids)!=0
),
midyear_officers as(
  Select soa.staff_external_id, soa.state_code, soa.full_name, soa.supervision_district, soa.supervisor_external_ids, string(NULL) as email from `recidiviz-123.outliers_views.supervision_officers_archive_materialized` soa left join current_ix_officers c on c.staff_external_id=soa.staff_external_id where soa.state_code="US_ND" and export_date="2024-07-01" and c.staff_external_id is NULL and ARRAY_LENGTH(soa.supervisor_external_ids)!=0
),
september_officers as(
    Select soa.staff_external_id, soa.state_code, soa.full_name, soa.supervision_district, soa.supervisor_external_ids, string(NULL) as email from `recidiviz-123.outliers_views.supervision_officers_archive_materialized` soa left join current_ix_officers c on c.staff_external_id=soa.staff_external_id 
    left join midyear_officers mid on soa.staff_external_id=mid.external_id
    where soa.state_code="US_ND" and export_date="2024-09-01" and c.staff_external_id is NULL and mid.external_id is NULL and ARRAY_LENGTH(soa.supervisor_external_ids)!=0
),
officers as (select * from current_ix_officers c union all select * from midyear_officers union all select * from september_officers)

SELECT
    supervisors.full_name AS supervisor_name,
    supervisors.supervision_district AS supervisor_district,
    officers.staff_external_id AS officer_id,
    officers.full_name AS officer_name,
    officers.supervision_district AS officer_district,
    officers.email AS officer_email
FROM `recidiviz-123.outliers_views.supervision_officer_supervisors_materialized` supervisors
INNER JOIN officers
ON
    supervisors.external_id IN UNNEST(officers.supervisor_external_ids)
    AND supervisors.state_code = officers.state_code
WHERE supervisors.state_code = "US_ND"
ORDER BY supervisors.external_id, officers.full_name
"""


@attr.define(frozen=True)
class Name:
    given_names: str
    middle_names: str
    surname: str
    name_suffix: str

    @classmethod
    def from_json_string(cls, s: str) -> "Name":
        as_dict = json.loads(s)
        return cls(**as_dict)

    def formatted(self) -> str:
        return f"{self.given_names} {self.surname}"


@attr.define(frozen=True)
class Officer:
    officer_id: str
    officer_name: Name
    officer_email: str
    officer_district: str


@attr.define(frozen=True)
class Supervisor:
    supervisor_name: Name
    supervisor_district: str


@attr.define(frozen=True)
class SupervisorsAndOfficers:
    data: dict[Supervisor, list[Officer]]

    @classmethod
    def from_bigquery(cls, bq_client: BigQueryClient) -> "SupervisorsAndOfficers":
        results = bq_client.run_query_async(
            query_str=_SUPERVISORS_AND_OFFICERS_QUERY, use_query_cache=True
        )
        grouped_data = defaultdict(list)

        for row in results:
            supervisor_name = Name.from_json_string(row["supervisor_name"])
            supervisor_district = row["supervisor_district"]
            officer_name = Name.from_json_string(row["officer_name"])
            officer_email = row["officer_email"]
            officer_district = row["officer_district"]
            grouped_data[Supervisor(supervisor_name, supervisor_district)].append(
                Officer(
                    row["officer_id"], officer_name, officer_email, officer_district
                )
            )
        return cls(grouped_data)
