# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Custom parsers for identity mappings that clean and validate contact info.

IdentityPhoneNumber and IdentityEmail validate their values strictly, so junk
contact info would crash entity construction. Each parser here runs the
entity's own validator and returns the cleaned value when it passes, or None
when it does not, so a mapping can omit junk contact info rather than fail.
Because the parsers reuse the entity validators, the check can never drift
from what the entity will accept.
"""
import re

from recidiviz.common.attr_validator_checks import passes_validator
from recidiviz.common.attr_validators import is_valid_email, is_valid_phone_number


def normalize_phone_or_null(phone: str) -> str | None:
    """Returns the phone number as bare ten NANP digits if it is a valid phone
    number once formatting and any leading country-code 1 are stripped, or None
    otherwise. The same number written with and without the country code
    normalizes to the same string."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if passes_validator(is_valid_phone_number, digits) else None


def validate_email_or_null(email: str) -> str | None:
    """Returns the email address if it is valid, or None otherwise."""
    if not email:
        return None
    return email if passes_validator(is_valid_email, email) else None
