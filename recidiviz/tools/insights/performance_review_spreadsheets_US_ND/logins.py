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
"""Utilities for querying logins from auth0."""

from collections import defaultdict
from datetime import datetime

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause

_LOGINS_QUERY = f"""
SELECT staff_external_id, DATE_TRUNC(login_date, MONTH) AS month, COUNT(*) AS num_logins
FROM `recidiviz-123.analyst_data.all_auth0_login_events_materialized` logins
INNER JOIN `recidiviz-123.reference_views.supervision_product_staff_materialized` staff
    ON LOWER(logins.email_address) = LOWER(staff.email)
    AND login_date BETWEEN staff.start_date AND {nonnull_end_date_exclusive_clause("staff.end_date_exclusive")}
-- Keep events before 2024 so we know whether for a month with no logins if they hadn't logged in
-- for the first time yet or just didn't log in that month
WHERE login_date < "2025-04-01"
AND staff.state_code = "US_ND"
GROUP BY 1, 2
ORDER BY 1, 2
"""


@attr.define(frozen=True)
class Logins:
    """Holds logins by month by officer"""

    data: dict[str, dict[datetime, int]]  # [officer_id, [month, num_logins]]

    @classmethod
    def from_bigquery(cls, bq_client: BigQueryClient) -> "Logins":
        """Creates a Logins based on the result of querying BigQuery"""
        results = bq_client.run_query_async(
            query_str=_LOGINS_QUERY, use_query_cache=True
        )

        # Unbound data includes events from before 2024
        unbound_data: dict[str, dict[datetime, int]] = defaultdict(dict)
        for row in results:
            unbound_data[row["staff_external_id"]][row["month"]] = row["num_logins"]

        # Recreate the dict, adding zeros for missing months after first login date
        data: dict[str, dict[datetime, int]] = defaultdict(dict)
        for officer_id, logins in unbound_data.items():
            # Start with the earliest login month, or Jan 2024 if they first logged in before then
            month = max(min(list(logins.keys())), datetime(2024, 1, 1))
            while month < datetime(2025, 4, 1):
                data[officer_id][month] = logins.get(month, 0)
                month += relativedelta(months=1)

        return cls(data)
