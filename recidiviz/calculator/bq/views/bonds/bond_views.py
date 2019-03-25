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
""""Views related to Bonds."""

from recidiviz.calculator.bq.views.bonds.bond_amounts_unknown_denied import \
    BOND_AMOUNTS_UNKNOWN_DENIED_VIEW
from recidiviz.calculator.bq.views.bonds.bond_amounts_by_booking import \
    BOND_AMOUNTS_BY_BOOKING_VIEW
from recidiviz.calculator.bq.views.bonds.bond_amounts_all_bookings import \
    BOND_AMOUNTS_ALL_BOOKINGS_VIEW

BOND_VIEWS = [
    BOND_AMOUNTS_UNKNOWN_DENIED_VIEW,
    BOND_AMOUNTS_BY_BOOKING_VIEW,
    BOND_AMOUNTS_ALL_BOOKINGS_VIEW
]
