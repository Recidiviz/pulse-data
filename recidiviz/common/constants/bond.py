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
    '10 BAIL CREDIT': BondType.PARTIAL_CASH,
    '10 BOND': BondType.PARTIAL_CASH,
    '10 CASH': BondType.PARTIAL_CASH,
    '10 NO CONTACT': BondType.PARTIAL_CASH,
    '10 PS': BondType.PARTIAL_CASH,
    '10 SURETY': BondType.PARTIAL_CASH,
    '10 W BAIL CREDIT': BondType.PARTIAL_CASH,
    '17 ALT': None,
    'APPROVED SURETY': BondType.SECURED,
    'AR': BondType.NOT_REQUIRED,  # Administrative Release
    'AT 10': BondType.PARTIAL_CASH,
    'BAIL CREDIT': BondType.CASH,
    'BAIL CREDIT SERVED': BondType.NOT_REQUIRED,
    'BAIL CREDIT W CASH': BondType.CASH,
    'BAIL DENIED': BondType.DENIED,
    'BED DATE': None,
    'BENCH WARRANT': None,
    'BOND': BondType.CASH,
    'BOND AT 10': BondType.PARTIAL_CASH,
    'BOND AT 20': BondType.PARTIAL_CASH,
    'BOND DENIED': BondType.DENIED,
    'BONDING COMPANY': BondType.SECURED,
    'CA': BondType.CASH,
    'CALENDAR': None,
    'CASH': BondType.CASH,
    'CASH 10': BondType.CASH,
    'CASH AT 10': BondType.PARTIAL_CASH,
    'CASH BOND': BondType.CASH,
    'CASH OR APP SURETY': BondType.SECURED,
    'CASH OR BOND': BondType.SECURED,
    'CASH NO CONTACT': BondType.CASH,
    'CASH NON REFUNDABLE': BondType.CASH,
    'CASH NO SURETY': BondType.CASH,
    'CASH ONLY': BondType.CASH,
    'CASH OR PROPERTY': BondType.SECURED,
    'CASH OR SURETY': BondType.SECURED,
    'CASH PAY OUT': BondType.CASH,
    'CASH PROP': BondType.SECURED,
    'CASH PURGE': BondType.CASH,
    'CASH SURETY': BondType.SECURED,
    'CASH W BAIL CREDIT': BondType.CASH,
    'CHILD SUPPORT': BondType.CASH,
    'CORPORATE BOND': BondType.SECURED,
    'COURT ORDER': None,
    'COURT ORDERED RELEASE': None,
    'COURT RELEASE': None,
    'DENIED': BondType.DENIED,
    'FINES AND COST': BondType.CASH,
    'FULL CASH': BondType.CASH,
    'FULL CASH NO SURETY': BondType.CASH,
    'FULL CASH WITH BAIL CREDIT': BondType.CASH,
    'HOLD WITHOUT BAIL': BondType.DENIED,
    'MH COURT': None,
    'MINIMUM EXPIRATION': None,
    'N A': BondType.NOT_REQUIRED,
    'NO 10 NO SURETY': BondType.CASH,
    'NO BAIL': BondType.DENIED,
    'NO BOND': BondType.DENIED,
    'NO BOND ALLOWED': BondType.DENIED,
    'NO BOND SET': BondType.DENIED,
    'NO SURETY NO CC NO 10': BondType.CASH,
    'NON REFUNDABLE': BondType.CASH,
    'NONE SET': BondType.NOT_REQUIRED,
    'NONE': BondType.DENIED,
    'NOT REQUIRED': BondType.NOT_REQUIRED,
    'O R': BondType.NOT_REQUIRED,
    'O RD': BondType.NOT_REQUIRED,
    'OPEN': None,
    'OR': BondType.NOT_REQUIRED,
    'OR BOND': BondType.NOT_REQUIRED,
    'OR D': BondType.NOT_REQUIRED,
    'OTHER': None,
    'OWN RECOG': BondType.NOT_REQUIRED,
    'OWN RECOGNIZANCE': BondType.NOT_REQUIRED,
    'OWN RECOGNIZANCE SIGNATURE BOND': BondType.NOT_REQUIRED,
    'PARTIAL CASH': BondType.PARTIAL_CASH,
    'PARTIALLY SECURE': BondType.SECURED,
    'PARTIALLY SECURE 10': BondType.SECURED,
    'PARTIALLY SECURED': BondType.SECURED,
    'PAY AND RELEASE': BondType.CASH,
    'PAY FINE': BondType.CASH,
    'PAY OR SERVE': BondType.CASH,
    'PAY OR STAY': BondType.CASH,
    'PAY STAY': BondType.CASH,
    # TODO(990): remap 'PENDING' values to BondStatus.PENDING
    'PENDING': None,
    'PENDING PRE TRIAL': None,
    'PER COMMITMENT': None,
    'PER STATUE': None,
    'PERCENT': BondType.PARTIAL_CASH,
    'PERSONAL RECOGNIZANCE': BondType.NOT_REQUIRED,
    'PROBATION VIOLATION HOLD': BondType.DENIED,
    'PROPERTY': BondType.SECURED,
    'PROPERTY BOND': BondType.SECURED,
    'PURGE': BondType.CASH,
    'PURGE PAYMENT': BondType.CASH,
    'RELEASE': BondType.NOT_REQUIRED,
    'RELEASE FR CUST': BondType.NOT_REQUIRED,
    'RELEASE FR CUSTODY': BondType.NOT_REQUIRED,
    'RELEASE FROM CUSTODY': BondType.NOT_REQUIRED,
    'RELEASE ON RECOGNIZANCE': BondType.NOT_REQUIRED,
    'RELEASE TO RESPONSIBLE ADULT': BondType.NOT_REQUIRED,
    'RELEASED BY COURT': BondType.NOT_REQUIRED,
    'RELEASED BY JU': BondType.NOT_REQUIRED,
    'RELEASED ON OWN RECOGNIZANCE': BondType.NOT_REQUIRED,
    'RELEASED ON RECOGNIZANCE': BondType.NOT_REQUIRED,
    'RELEASED OR': BondType.NOT_REQUIRED,
    'ROR': BondType.NOT_REQUIRED,
    'ROR BOND': BondType.NOT_REQUIRED,
    'SECURE BOND': BondType.SECURED,
    'SECURED': BondType.SECURED,
    'SECURED APPEARANCE BOND WITH A CASH OR SURETY DEPOSIT': BondType.SECURED,
    'SECURED BOND': BondType.SECURED,
    'SEE ABOVE': None,
    'SEE COMM': None,
    'SEE COMMENTS': None,
    'SEE MEMO': None,
    # TODO(990): remap 'SERVING' values to ChargeStatus.SENTENCED
    'SERVING FELONY TIME': None,
    'SERVING MISDEMEANOR TIME': None,
    'SERVE 10': BondType.CASH,
    'SERVE TIME': None,
    # TODO(990): remap 'SET BY COURT' to BondStatus.SET
    'SET BY COURT': None,
    'SPLIT BONDS': BondType.CASH,
    # TODO(990): remap 'STATE INMATE' to ChargeStatus.SENTENCED
    'STATE INMATE': None,
    'SURETY': BondType.SECURED,
    'SURETY BOND': BondType.SECURED,
    'THIS CHARG ONLY': None,
    'TIME EXPIRED': BondType.NOT_REQUIRED,
    'TIME SERVED BAIL CREDIT': BondType.NOT_REQUIRED,
    'TIME SERVED WITH JAIL CREDIT': BondType.NOT_REQUIRED,
    'TOTAL BOND PER COURT': BondType.CASH,
    'U S CURRENCY': BondType.CASH,
    'UNKNOWN': BondType.EXTERNAL_UNKNOWN,
    'UNKNOWN CODE': BondType.EXTERNAL_UNKNOWN,
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
    'BR': BondStatus.REVOKED,
    'BOND POSTED': BondStatus.POSTED,
    'BOND REVOCATION': BondStatus.REVOKED,
    'BOND REVOKED': BondStatus.REVOKED,
    'PENDING': BondStatus.PENDING,
    'POSTED': BondStatus.POSTED,
    'REVOKED': BondStatus.REVOKED,
    'REVOKED BOND': BondStatus.REVOKED,
    'SET': BondStatus.SET,
}
