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
"""Exceptions raised by the Identity Service."""


class UnknownCallerError(ValueError):
    """Raised when an authenticated caller is not in the source-app mapping."""


class IdentityHistoryIntegrityException(ValueError):
    """Raised when stored identity data violates an invariant the merge/split
    audit trail is supposed to guarantee -- e.g. a merged_into chain that
    references a nonexistent record, or a cycle in that chain. Indicates
    corrupt data requiring investigation, not a caller error."""
