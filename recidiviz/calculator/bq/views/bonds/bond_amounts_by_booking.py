# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Collapses a booking's bonds into a total amounts and UNKNOWN or DENIED."""
# pylint: disable=line-too-long

from recidiviz.calculator.bq.views import bqview
from recidiviz.calculator.bq.views import view_config
from recidiviz.calculator.bq.views.bonds.bond_amounts_unknown_denied import BOND_AMOUNTS_UNKNOWN_DENIED_VIEW

from recidiviz.common.constants.enum_canonical_strings import bond_type_denied

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.VIEWS_DATASET

BOND_AMOUNTS_BY_BOOKING_VIEW_NAME = 'bond_amounts_by_booking'

BOND_AMOUNTS_BY_BOOKING_DESCRIPTION = \
"""
Collapse all bonds for a booking into a Bond Amounts by Booking table.

If any of the booking's bonds are {bond_type_denied}, consider the entire booking's bonds to be {bond_type_denied}.
If any amount is present on the bonds of a non-{bond_type_denied} booking, sum it as the total_bond_dollars.
If no amounts are present and the bond is not {bond_type_denied}, consider the total booking bond amount UNKNOWN.

Constraints:
If a Bond has `total_bond_dollars`, it cannot be {bond_type_denied} or UNKNOWN.
If a Bond's `total_bond_dollars` is NULL, it must be one of {bond_type_denied} or UNKNOWN.
If a Bond is {bond_type_denied} or UNKNOWN, its `total_bond_dollars` must be NULL.
""".format(
    bond_type_denied=bond_type_denied
)

BOND_AMOUNTS_BY_BOOKING_QUERY = \
"""
/*{description}*/
SELECT BondAmounts.booking_id,
  -- We don't need to use IF statements for all of the below, but keep them for consistency.
  IF(BondAmounts.denied_count > 0, True, False) AS denied,
  IF(BondAmounts.denied_count > 0, NULL, BondAmounts.total_bond_dollars) AS total_bond_dollars,
  IF((BondAmounts.total_bond_dollars IS NULL AND BondAmounts.denied_count = 0), True, False) AS unknown
FROM (
  SELECT
    Bond.booking_id,
    SUM(Bond.amount_dollars) AS total_bond_dollars,
    COUNTIF(Bond.denied) AS denied_count,
    -- unknown_count is not used, but keep it for consistency.
    COUNTIF(Bond.unknown) AS unknown_count
  FROM `{project_id}.{views_dataset}.{bond_amounts_unknown_denied_view}` Bond
  GROUP BY booking_id
) BondAmounts
""".format(
    description=BOND_AMOUNTS_BY_BOOKING_DESCRIPTION,
    project_id=PROJECT_ID,
    views_dataset=VIEWS_DATASET,
    bond_amounts_unknown_denied_view=BOND_AMOUNTS_UNKNOWN_DENIED_VIEW.view_id
)

BOND_AMOUNTS_BY_BOOKING_VIEW = bqview.BigQueryView(
    view_id=BOND_AMOUNTS_BY_BOOKING_VIEW_NAME,
    view_query=BOND_AMOUNTS_BY_BOOKING_QUERY
)

if __name__ == '__main__':
    print(BOND_AMOUNTS_BY_BOOKING_VIEW.view_id)
    print(BOND_AMOUNTS_BY_BOOKING_VIEW.view_query)
