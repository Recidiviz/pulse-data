# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Constants related to a bond entity."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings


class BondType(object):
    BOND_DENIED = enum_strings.bond_type_denied
    CASH = enum_strings.bond_type_cash
    NO_BOND = enum_strings.bond_type_no_bond
    SECURED = enum_strings.bond_type_secured
    UNSECURED = enum_strings.bond_type_unsecured


class BondStatus(object):
    ACTIVE = enum_strings.bond_status_active
    POSTED = enum_strings.bond_status_posted


BOND_TYPE_MAP = {
    'BOND DENIED': BondType.BOND_DENIED,
    'CASH': BondType.CASH,
    'NO BOND': BondType.NO_BOND,
    'SECURED': BondType.SECURED,
    'UNSECURED': BondType.UNSECURED,
}


BOND_STATUS_MAP = {
    'ACTIVE': BondStatus.ACTIVE,
    'POSTED': BondStatus.POSTED,
}
