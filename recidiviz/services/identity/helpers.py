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
"""Helpers for resolving metadata about Identity Service requests."""
from recidiviz.common.constants.identity import ProductApp
from recidiviz.services.identity.constants import PRODUCT_APP_BY_SERVICE_ACCOUNT
from recidiviz.services.identity.exceptions import UnknownCallerError


def get_source_product_app(caller_email: str | None) -> ProductApp | None:
    """Returns the ProductApp the given IAP caller writes its records under.

    A return value of None means the caller is permitted but does not write
    PRODUCT_APP-sourced attributes (e.g. the batch identity clustering DAG).
    Raises UnknownCallerError if the caller's email is not present in the
    static mapping.
    """
    if caller_email not in PRODUCT_APP_BY_SERVICE_ACCOUNT:
        raise UnknownCallerError(f"Unknown caller [{caller_email}]")
    return PRODUCT_APP_BY_SERVICE_ACCOUNT[caller_email]
