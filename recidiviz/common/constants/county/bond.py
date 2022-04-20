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

"""
Constants related to a bond shared between county and state schemas.
"""
from typing import Dict, Optional

import recidiviz.common.constants.county.enum_canonical_strings as enum_strings
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
    def _get_default_map() -> Dict[str, Optional["BondType"]]:
        return BOND_TYPE_MAP


class BondStatus(EntityEnum, metaclass=EntityEnumMeta):
    PENDING = enum_strings.bond_status_pending
    POSTED = enum_strings.bond_status_posted
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    REMOVED_WITHOUT_INFO = enum_strings.removed_without_info
    REVOKED = enum_strings.bond_status_revoked
    SET = enum_strings.bond_status_set

    @staticmethod
    def _get_default_map() -> Dict[str, "BondStatus"]:
        return BOND_STATUS_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# Not marked as private so it can be used in
# persistence/ingest_info_converter/converter_utils
BOND_TYPE_MAP = {
    "10": BondType.PARTIAL_CASH,
    "10 BAIL": BondType.PARTIAL_CASH,
    "10 BAIL CREDIT": BondType.PARTIAL_CASH,
    "10 BOND": BondType.PARTIAL_CASH,
    "10 CASH": BondType.PARTIAL_CASH,
    "10 DEPOSIT BOND": BondType.PARTIAL_CASH,
    "10 NO CONTACT": BondType.PARTIAL_CASH,
    "10 NO SURETY": BondType.PARTIAL_CASH,
    "10 NO SURETY ALLOWED": BondType.PARTIAL_CASH,
    "10 PS": BondType.PARTIAL_CASH,
    "10 SURETY": BondType.PARTIAL_CASH,
    "10 W BAIL CREDIT": BondType.PARTIAL_CASH,
    "17 ALT": None,
    "20 BOND": BondType.PARTIAL_CASH,
    "APPEARANCE": BondType.CASH,
    "APPROVED SURETY": BondType.SECURED,
    "AR": BondType.NOT_REQUIRED,  # Administrative Release
    "AT 10": BondType.PARTIAL_CASH,
    "B W FEE": BondType.CASH,
    "BAIL CREDIT": BondType.CASH,
    "BAIL CREDIT SERVED": BondType.NOT_REQUIRED,
    "BAIL CREDIT W CASH": BondType.CASH,
    "BAIL DENIED": BondType.DENIED,
    "BED DATE": None,
    "BENCH WARRANT": None,
    "BOND": BondType.CASH,
    "BOND AT 10": BondType.PARTIAL_CASH,
    "BOND AT 20": BondType.PARTIAL_CASH,
    "BOND DENIED": BondType.DENIED,
    "BONDING COMPANY": BondType.SECURED,
    "BONDS MAN": BondType.SECURED,
    "CA": BondType.CASH,
    "CALENDAR": None,
    "CASH": BondType.CASH,
    "CASH 10": BondType.PARTIAL_CASH,
    "CASH 10 H I": BondType.PARTIAL_CASH,
    "CASH AND SURETY BOND": BondType.SECURED,
    "CASH AT 10": BondType.PARTIAL_CASH,
    "CASH BAIL BOND": BondType.CASH,
    "CASH BAIL CREDIT": BondType.UNSECURED,
    "CASH BOND": BondType.CASH,
    "CASH HOME INC": BondType.CASH,
    "CASH MCR PROGRAM": BondType.CASH,
    "CASH NO CONTACT": BondType.CASH,
    "CASH NON REFUNDABLE": BondType.CASH,
    "CASH NO SURETY": BondType.CASH,
    "CASH ONLY": BondType.CASH,
    "CASH OR 2X PROPERTY": BondType.SECURED,
    "CASH OR APP SURETY": BondType.SECURED,
    "CASH OR BOND": BondType.SECURED,
    "CASH OR CORPORATE": BondType.SECURED,
    "CASH OR PROPERY": BondType.SECURED,
    "CASH OR PROPERTY": BondType.SECURED,
    "CASH OR SURETY": BondType.SECURED,
    "CASH PAY OUT": BondType.CASH,
    "CASH PROP": BondType.SECURED,
    "CASH PROPERTY": BondType.SECURED,
    "CASH PROPERTY SURETY": BondType.SECURED,
    "CASH PURGE": BondType.CASH,
    "CASH SURETY": BondType.SECURED,
    "CASH SURETY OR 10 CASH": BondType.PARTIAL_CASH,
    "CASH W BAIL CREDIT": BondType.CASH,
    "C B": BondType.CASH,
    "CHILD SUPPORT": BondType.CASH,
    "CHILD SUPPORT CASH NON REFUNDABLE": BondType.CASH,
    "CIRCUIT COURT SURETY": BondType.SECURED,
    "CORPORATE BOND": BondType.SECURED,
    "COURT ORDER": BondType.NOT_REQUIRED,
    "COURT ORDERED RELEASE": BondType.NOT_REQUIRED,
    "COURT RELEASE": BondType.NOT_REQUIRED,
    "C S 10": BondType.PARTIAL_CASH,
    "DENIED": BondType.DENIED,
    "DENIED BOND": BondType.DENIED,
    "DO NOT RELEASE": BondType.DENIED,
    "DOC": None,
    "FINES": BondType.CASH,
    "FINES AND COST": BondType.CASH,
    "FULL CASH": BondType.CASH,
    "FULL CASH NO SURETY": BondType.CASH,
    "FULL CASH WITH BAIL CREDIT": BondType.CASH,
    "FULLY SECURED": BondType.SECURED,
    "FULLY SECURED OR DBL PROPERTY OR 10": BondType.PARTIAL_CASH,
    "HOLD WITHOUT BAIL": BondType.DENIED,
    "INCLUDED": None,
    "JAIL CREDIT": BondType.CASH,
    "MH COURT": None,
    "MINIMUM EXPIRATION": None,
    "N A": BondType.NOT_REQUIRED,
    "NO 10 NO SURETY": BondType.CASH,
    "NO BAIL": BondType.DENIED,
    "NO BOND": BondType.DENIED,
    "NO BOND ALLOWED": BondType.DENIED,
    "NO BOND NEEDED": BondType.NOT_REQUIRED,
    "NO BOND REQUIRED": BondType.NOT_REQUIRED,
    "NO BOND SET": BondType.DENIED,
    "NO BOND UNTIL DRUG SCREEN TAKEN": BondType.DENIED,
    "NO INFO": None,
    "NO SURETY NO CC NO 10": BondType.CASH,
    "NOBOND": BondType.DENIED,
    "NON REFUNDABLE": BondType.CASH,
    "NONE SET": BondType.NOT_REQUIRED,
    "NONE": BondType.DENIED,
    "NOT BONDABLE": BondType.DENIED,
    "NOT REQUIRED": BondType.NOT_REQUIRED,
    "O R": BondType.NOT_REQUIRED,
    "O R BOND": BondType.NOT_REQUIRED,
    "O RD": BondType.NOT_REQUIRED,
    "OPEN": None,
    "OR": BondType.NOT_REQUIRED,
    "OR BOND": BondType.NOT_REQUIRED,
    "OR D": BondType.NOT_REQUIRED,
    "OTHER": None,
    "OTHER BOND TYPE": None,
    "OWN RECOG": BondType.NOT_REQUIRED,
    "OWN RECOGNIZANCE": BondType.NOT_REQUIRED,
    "OWN RECOGNIZANCE SIGNATURE BOND": BondType.NOT_REQUIRED,
    "P OR S": BondType.CASH,
    "PAID": BondType.CASH,
    "PARTIAL CASH": BondType.PARTIAL_CASH,
    "PARTIALLY SECURE": BondType.SECURED,
    "PARTIALLY SECURE 10": BondType.SECURED,
    "PARTIALLY SECURED": BondType.SECURED,
    "PARTIALLY SECURED BOND": BondType.SECURED,
    "PAY AND RELEASE": BondType.CASH,
    "PAY AND STAY": BondType.CASH,
    "PAY FINE": BondType.CASH,
    "PAY OR SERVE": BondType.CASH,
    "PAY OR STAY": BondType.CASH,
    "PAY STAY": BondType.CASH,
    "PAYMENT OF FINES": BondType.CASH,
    # TODO(#10481): remap 'PENDING' values to BondStatus.PENDING
    "PENDING": None,
    "PENDING PRE TRIAL": None,
    "PER COMMITMENT": None,
    "PER STATUE": None,
    "PERCENT": BondType.PARTIAL_CASH,
    "PERCENT BOND": BondType.PARTIAL_CASH,
    "PERSONAL BOND": BondType.NOT_REQUIRED,
    "PERSONAL RECOGNIZANCE": BondType.NOT_REQUIRED,
    "PERSONAL RECOGNIZANCE BOND": BondType.NOT_REQUIRED,
    "PERSONAL RECONGNIZANCE BOND": BondType.NOT_REQUIRED,
    "PR": BondType.NOT_REQUIRED,
    "PRE TRIAL BOND NOT SET": BondType.DENIED,
    "PROBATION VIOLATION HOLD": BondType.DENIED,
    "PROPERTY": BondType.SECURED,
    "PROPERTY BOND": BondType.SECURED,
    "PURGE": BondType.CASH,
    "PURGE PAYMENT": BondType.CASH,
    "RELEASE": BondType.NOT_REQUIRED,
    "RELEASE FR CUST": BondType.NOT_REQUIRED,
    "RELEASE FR CUSTODY": BondType.NOT_REQUIRED,
    "RELEASE FROM CUSTODY": BondType.NOT_REQUIRED,
    "RELEASE ON RECOGNIZANCE": BondType.NOT_REQUIRED,
    "RELEASE ON RECOGNIZENCE": BondType.NOT_REQUIRED,
    "RELEASE TO RESPONSIBLE ADULT": BondType.NOT_REQUIRED,
    "RELEASED BY COURT": BondType.NOT_REQUIRED,
    "RELEASED BY JU": BondType.NOT_REQUIRED,
    "RELEASED ON OWN RECOGNIZANCE": BondType.NOT_REQUIRED,
    "RELEASED ON RECOGNIZANCE": BondType.NOT_REQUIRED,
    "RELEASED OR": BondType.NOT_REQUIRED,
    "RELEASED OWN PR": BondType.NOT_REQUIRED,
    "RESTITUTION CASH NON REFUNDABLE": BondType.CASH,
    "ROP": BondType.NOT_REQUIRED,
    "ROR": BondType.NOT_REQUIRED,
    "ROR BOND": BondType.NOT_REQUIRED,
    "SEC A": BondType.SECURED,
    "SECURE BOND": BondType.SECURED,
    "SECURED": BondType.SECURED,
    "SECURED APPEARANCE BOND WITH A CASH OR SURETY DEPOSIT": BondType.SECURED,
    "SECURED BOND": BondType.SECURED,
    "SEE ABOVE": None,
    "SEE COMM": None,
    "SEE COMMENTS": None,
    "SEE MEMO": None,
    # TODO(#10481): remap 'SERVING' values to ChargeStatus.SENTENCED
    "SERVING FELONY TIME": None,
    "SERVING MISDEMEANOR TIME": None,
    "SERVE 10": BondType.CASH,
    "SERVE TIME": None,
    # TODO(#10481): remap 'SET BY COURT' to BondStatus.SET
    "SET BY COURT": None,
    "SIGNATURE BOND": BondType.NOT_REQUIRED,
    "SIGNATURE BOND W COSIGNER": BondType.NOT_REQUIRED,
    "SPLIT BONDS": BondType.CASH,
    "SPLIT SENTENCE": BondType.CASH,
    # TODO(#10481): remap 'STATE INMATE' to ChargeStatus.SENTENCED
    "STATE INMATE": None,
    "SURETY": BondType.SECURED,
    "SURETY BOND": BondType.SECURED,
    "SURETY BOND 10": BondType.PARTIAL_CASH,
    "SURETY BOND OR 10": BondType.PARTIAL_CASH,
    "SURETY CASH": BondType.SECURED,
    "SURETY ONLY": BondType.SECURED,
    "THIS CHARG ONLY": None,
    "TIME EXPIRED": BondType.NOT_REQUIRED,
    "TIME SERVED BAIL CREDIT": BondType.NOT_REQUIRED,
    "TIME SERVED WITH JAIL CREDIT": BondType.NOT_REQUIRED,
    "TOTAL BOND PER COURT": BondType.CASH,
    "U S CURRENCY": BondType.CASH,
    "UNKNOWN": BondType.EXTERNAL_UNKNOWN,
    "UNKNOWN CODE": BondType.EXTERNAL_UNKNOWN,
    "UNSC": BondType.UNSECURED,
    "UNSC T P": BondType.UNSECURED,
    "UNSECURE": BondType.UNSECURED,
    "UNSECURE BOND": BondType.UNSECURED,
    "UNSECURED": BondType.UNSECURED,
    "UNSECURED BOND": BondType.UNSECURED,
    "UNSECURED 3RD PARTY": BondType.UNSECURED,
    "USB": BondType.UNSECURED,
    "W SURETY": BondType.SECURED,
    "WRITTEN PROMISE": BondType.NOT_REQUIRED,
    "WRITTEN PR": BondType.NOT_REQUIRED,
    "WRITTEN PROMISE TO APPEAR": BondType.NOT_REQUIRED,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# Not marked as private so it can be used in
# persistence/ingest_info_converter/converter_utils
BOND_STATUS_MAP = {
    "ACTIVE": BondStatus.SET,
    "BR": BondStatus.REVOKED,
    "BOND PAID SEE ATTACHMENTS": BondStatus.POSTED,
    "BOND POSTED": BondStatus.POSTED,
    "BOND REVOCATION": BondStatus.REVOKED,
    "BOND REVOKED": BondStatus.REVOKED,
    "PAID": BondStatus.POSTED,
    "PAID BOND": BondStatus.POSTED,
    "PENDING": BondStatus.PENDING,
    "POSTED": BondStatus.POSTED,
    "REVOKED": BondStatus.REVOKED,
    "REVOKED BOND": BondStatus.REVOKED,
    "SET": BondStatus.SET,
}
