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

""""Views related to Bonds."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.county.views.bonds.bond_amounts_all_bookings import (
    BOND_AMOUNTS_ALL_BOOKINGS_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.bonds.bond_amounts_all_bookings_bins import (
    BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.bonds.bond_amounts_by_booking import (
    BOND_AMOUNTS_BY_BOOKING_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.bonds.bond_amounts_unknown_denied import (
    BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.bonds.bond_is_unique import (
    BOND_IS_UNIQUE_VIEW_BUILDER,
)

BOND_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_BUILDER,
    BOND_AMOUNTS_BY_BOOKING_VIEW_BUILDER,
    BOND_AMOUNTS_ALL_BOOKINGS_VIEW_BUILDER,
    BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_BUILDER,
    BOND_IS_UNIQUE_VIEW_BUILDER,
]
