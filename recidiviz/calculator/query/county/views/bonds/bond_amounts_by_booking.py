# Recidiviz - a data platform for criminal justice reform
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


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.bonds.bond_amounts_unknown_denied import (
    BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_BUILDER,
)
from recidiviz.common.constants.enum_canonical_strings import bond_type_denied
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BOND_AMOUNTS_BY_BOOKING_VIEW_NAME = "bond_amounts_by_booking"

BOND_AMOUNTS_BY_BOOKING_DESCRIPTION = f"""
Collapse all bonds for a booking into a Bond Amounts by Booking table.

If any of the booking's bonds are {bond_type_denied}, consider the entire booking's bonds to be {bond_type_denied}.
If any amount is present on the bonds of a non-{bond_type_denied} booking, sum it as the total_bond_dollars.
If no amounts are present and the bond is not {bond_type_denied}, consider the total booking bond amount UNKNOWN.

Constraints:
If a Bond has `total_bond_dollars`, it cannot be {bond_type_denied} or UNKNOWN.
If a Bond's `total_bond_dollars` is NULL, it must be one of {bond_type_denied} or UNKNOWN.
If a Bond is {bond_type_denied} or UNKNOWN, its `total_bond_dollars` must be NULL.
"""

BOND_AMOUNTS_BY_BOOKING_QUERY_TEMPLATE = """
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
"""

BOND_AMOUNTS_BY_BOOKING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=BOND_AMOUNTS_BY_BOOKING_VIEW_NAME,
    view_query_template=BOND_AMOUNTS_BY_BOOKING_QUERY_TEMPLATE,
    description=BOND_AMOUNTS_BY_BOOKING_DESCRIPTION,
    views_dataset=dataset_config.VIEWS_DATASET,
    bond_amounts_unknown_denied_view=BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_BUILDER.view_id,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        BOND_AMOUNTS_BY_BOOKING_VIEW_BUILDER.build_and_print()
