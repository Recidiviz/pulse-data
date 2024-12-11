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
SELECT
    supervisors.full_name AS supervisor_name,
    officers.external_id AS officer_id,
    officers.full_name AS officer_name
FROM `recidiviz-123.outliers_views.supervision_officer_supervisors_materialized` supervisors
INNER JOIN `recidiviz-123.outliers_views.supervision_officers_materialized` officers
ON
    supervisors.external_id IN UNNEST(officers.supervisor_external_ids)
    AND supervisors.state_code = officers.state_code
WHERE supervisors.state_code = "US_IX"
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


@attr.define(frozen=True)
class SupervisorsAndOfficers:
    data: dict[Name, list[Officer]]

    @classmethod
    def from_bigquery(cls, bq_client: BigQueryClient) -> "SupervisorsAndOfficers":
        results = bq_client.run_query_async(
            query_str=_SUPERVISORS_AND_OFFICERS_QUERY, use_query_cache=True
        )
        grouped_data = defaultdict(list)

        for row in results:
            supervisor_name = Name.from_json_string(row["supervisor_name"])
            officer_name = Name.from_json_string(row["officer_name"])
            grouped_data[supervisor_name].append(
                Officer(row["officer_id"], officer_name)
            )
        return cls(grouped_data)
