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
"""Hashing of user email addresses, shared by the auth endpoints and the
Identity Service.

Keep this module dependency-free. The Identity Service's source-visibility test
forbids importing recidiviz.auth.helpers (Flask, the case triage schema), so
anything the service and the auth endpoints must hash the same way lives here.
The two must produce identical hashes for their values to compare, so both import
from this module.
"""
import base64
import hashlib


def replace_char_0_slash(user_hash: str) -> str:
    """Returns the hash with a leading slash replaced by an underscore, so the
    value is safe to use in URL paths."""
    return user_hash[:1].replace("/", "_") + user_hash[1:]


def generate_user_hash(email: str) -> str:
    """Returns the base64-encoded SHA-256 hash of the given email address."""
    user_hash = base64.b64encode(hashlib.sha256(email.encode("utf-8")).digest()).decode(
        "utf-8"
    )
    return replace_char_0_slash(user_hash)


def normalize_email(address: str) -> str:
    """Returns the address normalized for hashing and comparison, so that
    addresses that differ only in surrounding whitespace or case are treated as
    the same address."""
    return address.strip().casefold()


def normalized_email_hash(address: str) -> str:
    """Returns generate_user_hash of the address after normalizing it with
    normalize_email, so equivalent addresses always hash to the same value."""
    return generate_user_hash(normalize_email(address))
