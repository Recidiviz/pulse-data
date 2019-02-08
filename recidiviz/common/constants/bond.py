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
from recidiviz.common.constants.mappable_enum import MappableEnum


class BondType(MappableEnum):
    CASH = enum_strings.bond_type_cash
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    NO_BOND = enum_strings.bond_type_no_bond
    SECURED = enum_strings.bond_type_secured
    UNKNOWN_REMOVED_FROM_SOURCE = enum_strings.unknown_removed_from_source
    UNSECURED = enum_strings.bond_type_unsecured

    @staticmethod
    def _get_default_map():
        return BOND_TYPE_MAP


class BondStatus(MappableEnum):
    DENIED = enum_strings.bond_status_denied
    INFERRED_SET = enum_strings.bond_status_inferred_set
    NOT_REQUIRED = enum_strings.bond_status_not_required
    PENDING = enum_strings.bond_status_pending
    POSTED = enum_strings.bond_status_posted
    REVOKED = enum_strings.bond_status_revoked
    SET = enum_strings.bond_status_set
    UNKNOWN_FOUND_IN_SOURCE = enum_strings.unknown_found_in_source
    UNKNOWN_REMOVED_FROM_SOURCE = enum_strings.unknown_removed_from_source

    @staticmethod
    def _get_default_map():
        return BOND_STATUS_MAP


# Not marked as private so it can be used in
# persistence/converter/converter_utils
BOND_TYPE_MAP = {
    'BOND DENIED': BondType.NO_BOND,
    'HOLD WITHOUT BAIL': BondType.NO_BOND,
    'CASH': BondType.CASH,
    'CASH BOND': BondType.CASH,
    'PURGE PAYMENT': BondType.CASH,
    'U.S. CURRENCY': BondType.CASH,
    'N/A': BondType.NO_BOND,
    'NO BOND': BondType.NO_BOND,
    'NO BOND ALLOWED': BondType.NO_BOND,
    'NONE SET': BondType.NO_BOND,
    'OTHER': BondType.NO_BOND,
    'RELEASE ON RECOGNIZANCE': BondType.NO_BOND,
    'RELEASED BY COURT': BondType.NO_BOND,
    'RELEASED ON OWN RECOGNIZANCE': BondType.NO_BOND,
    'ROR': BondType.NO_BOND,
    'WRITTEN PROMISE': BondType.NO_BOND,
    'WRITTEN PROMISE TO APPEAR': BondType.NO_BOND,
    'SECURED': BondType.SECURED,
    'SECURE BOND': BondType.SECURED,
    'SECURED BOND': BondType.SECURED,
    'CASH, SURETY': BondType.SECURED,
    'SURETY': BondType.SECURED,
    'SURETY BOND': BondType.SECURED,
    'UNKNOWN': BondType.EXTERNAL_UNKNOWN,
    'UNSECURE BOND': BondType.UNSECURED,
    'UNSECURED': BondType.UNSECURED,
}

# Not marked as private so it can be used in
# persistence/converter/converter_utils
BOND_STATUS_MAP = {
    'ACTIVE': BondStatus.SET,
    'BOND DENIED': BondStatus.DENIED,
    'DENIED': BondStatus.DENIED,
    'HOLD WITHOUT BAIL': BondStatus.DENIED,
    'NO BOND ALLOWED': BondStatus.DENIED,
    'NONE SET': BondStatus.NOT_REQUIRED,
    'NOT REQUIRED': BondStatus.NOT_REQUIRED,
    'PENDING': BondStatus.PENDING,
    'POSTED': BondStatus.POSTED,
    'REVOKED': BondStatus.REVOKED,
    'SET': BondStatus.SET,
}
