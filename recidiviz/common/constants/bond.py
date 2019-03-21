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
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class BondType(EntityEnum, metaclass=EntityEnumMeta):
    CASH = enum_strings.bond_type_cash
    DENIED = enum_strings.bond_type_denied
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    NOT_REQUIRED = enum_strings.bond_type_not_required
    PARTIAL_CASH = enum_strings.bond_type_partial_cash
    SECURED = enum_strings.bond_type_secured
    UNSECURED = enum_strings.bond_type_unsecured

    @staticmethod
    def _get_default_map():
        return BOND_TYPE_MAP


class BondStatus(EntityEnum, metaclass=EntityEnumMeta):
    PENDING = enum_strings.bond_status_pending
    POSTED = enum_strings.bond_status_posted
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    REMOVED_WITHOUT_INFO = enum_strings.removed_without_info
    REVOKED = enum_strings.bond_status_revoked
    SET = enum_strings.bond_status_set

    @staticmethod
    def _get_default_map():
        return BOND_STATUS_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# Not marked as private so it can be used in
# persistence/converter/converter_utils
BOND_TYPE_MAP = {
    '10': BondType.PARTIAL_CASH,
    '10 BOND': BondType.PARTIAL_CASH,
    '10 NO CONTACT': BondType.PARTIAL_CASH,
    '10 PS': BondType.PARTIAL_CASH,
    '10 W BAIL CREDIT': BondType.PARTIAL_CASH,
    'APPROVED SURETY': BondType.SECURED,
    'BAIL DENIED': BondType.DENIED,
    'BENCH WARRANT': None,
    'BOND DENIED': BondType.DENIED,
    'BOND AT 10': BondType.PARTIAL_CASH,
    'BOND AT 20': BondType.PARTIAL_CASH,
    'BONDING COMPANY': BondType.SECURED,
    'CA': BondType.CASH,
    'CASH': BondType.CASH,
    'CASH 10': BondType.CASH,
    'CASH AT 10': BondType.PARTIAL_CASH,
    'CASH BOND': BondType.CASH,
    'CASH NO CONTACT': BondType.CASH,
    'CASH NON REFUNDABLE': BondType.CASH,
    'CASH NO SURETY': BondType.CASH,
    'CASH OR PROPERTY': BondType.SECURED,
    'CASH OR SURETY': BondType.SECURED,
    'CASH PAY OUT': BondType.CASH,
    'CASH SURETY': BondType.SECURED,
    'CASH W BAIL CREDIT': BondType.CASH,
    'COURT ORDER': None,
    'COURT RELEASE': None,
    'DENIED': BondType.DENIED,
    'FINES AND COST': BondType.CASH,
    'FULL CASH': BondType.CASH,
    'FULL CASH NO SURETY': BondType.CASH,
    'HOLD WITHOUT BAIL': BondType.DENIED,
    'N A': BondType.NOT_REQUIRED,
    'NO BAIL': BondType.DENIED,
    'NO BOND': BondType.DENIED,
    'NO BOND ALLOWED': BondType.DENIED,
    'NO BOND SET': BondType.DENIED,
    'NONE SET': BondType.NOT_REQUIRED,
    'NONE': BondType.DENIED,
    'NOT REQUIRED': BondType.NOT_REQUIRED,
    'O R': BondType.NOT_REQUIRED,
    'OR BOND': BondType.NOT_REQUIRED,
    'OTHER': None,
    'OWN RECOG': BondType.NOT_REQUIRED,
    'OWN RECOGNIZANCE': BondType.NOT_REQUIRED,
    'OWN RECOGNIZANCE SIGNATURE BOND': BondType.NOT_REQUIRED,
    'PARTIAL CASH': BondType.PARTIAL_CASH,
    'PARTIALLY SECURE': BondType.SECURED,
    'PARTIALLY SECURE 10': BondType.SECURED,
    'PARTIALLY SECURED': BondType.SECURED,
    'PAY AND RELEASE': BondType.CASH,
    'PAY OR SERVE': BondType.CASH,
    'PAY OR STAY': BondType.CASH,
    'PAY STAY': BondType.CASH,
    # TODO(990): remap 'PENDING' values to BondStatus.PENDING
    'PENDING': None,
    'PENDING PRE TRIAL': None,
    'PERCENT': BondType.PARTIAL_CASH,
    'PERSONAL RECOGNIZANCE': BondType.NOT_REQUIRED,
    'PROBATION VIOLATION HOLD': BondType.DENIED,
    'PROPERTY': BondType.SECURED,
    'PROPERTY BOND': BondType.SECURED,
    'PURGE': BondType.CASH,
    'PURGE PAYMENT': BondType.CASH,
    'RELEASE ON RECOGNIZANCE': BondType.NOT_REQUIRED,
    'RELEASED BY COURT': BondType.NOT_REQUIRED,
    'RELEASED ON OWN RECOGNIZANCE': BondType.NOT_REQUIRED,
    'RELEASED ON RECOGNIZANCE': BondType.NOT_REQUIRED,
    'ROR': BondType.NOT_REQUIRED,
    'SECURED': BondType.SECURED,
    'SECURE BOND': BondType.SECURED,
    'SECURED BOND': BondType.SECURED,
    'SEE ABOVE': None,
    'SEE COMMENTS': None,
    'SEE MEMO': None,
    # TODO(990): remap 'SERVING' values to ChargeStatus.SENTENCED
    'SERVING FELONY TIME': None,
    'SERVING MISDEMEANOR TIME': None,
    'SERVE TIME': None,
    # TODO(990): remap 'SET BY COURT' to BondStatus.SET
    'SET BY COURT': None,
    'SPLIT BONDS': BondType.CASH,
    # TODO(990): remap 'STATE INMATE' to ChargeStatus.SENTENCED
    'STATE INMATE': None,
    'SURETY': BondType.SECURED,
    'SURETY BOND': BondType.SECURED,
    'TOTAL BOND PER COURT': BondType.CASH,
    'U S CURRENCY': BondType.CASH,
    'UNKNOWN': BondType.EXTERNAL_UNKNOWN,
    'UNSECURE': BondType.UNSECURED,
    'UNSECURE BOND': BondType.UNSECURED,
    'UNSECURED': BondType.UNSECURED,
    'UNSECURED BOND': BondType.UNSECURED,
    'USB': BondType.UNSECURED,
    'W SURETY': BondType.SECURED,
    'WRITTEN PROMISE': BondType.NOT_REQUIRED,
    'WRITTEN PROMISE TO APPEAR': BondType.NOT_REQUIRED,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# Not marked as private so it can be used in
# persistence/converter/converter_utils
BOND_STATUS_MAP = {
    'ACTIVE': BondStatus.SET,
    'BOND REVOCATION': BondStatus.REVOKED,
    'BOND REVOKED': BondStatus.REVOKED,
    'PENDING': BondStatus.PENDING,
    'POSTED': BondStatus.POSTED,
    'REVOKED': BondStatus.REVOKED,
    'REVOKED BOND': BondStatus.REVOKED,
    'SET': BondStatus.SET,
}
