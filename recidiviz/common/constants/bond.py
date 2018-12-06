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

class BondType(object):
    SECURED = 'SECURED'
    UNSECURED = 'UNSECURED'
    CASH = 'CASH'
    NO_BOND = 'NO BOND'
    BOND_DENIED = 'BOND DENIED'
    UNKNOWN = 'UNKNOWN'


class BondStatus(object):
    # TODO: resolve inconsistency between Python and DB enum values
    ACTIVE = 'Active'
    POSTED = 'POSTED'
    UNKNOWN = 'UNKNOWN'


BOND_TYPE_MAP = {
    'SECURED': BondType.SECURED,
    'UNSECURED': BondType.UNSECURED,
    'CASH': BondType.CASH,
    'NO BOND': BondType.NO_BOND,
    'BOND DENIED': BondType.BOND_DENIED,
    'UNKNOWN': BondType.UNKNOWN
}
BOND_STATUS_MAP = {
    'ACTIVE': BondStatus.ACTIVE,
    'POSTED': BondStatus.POSTED,
    'UNKNOWN': BondStatus.UNKNOWN
}
