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
"""Test helpers for the Identity Service."""
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import patch

from recidiviz.common.constants.identity import ProductApp
from recidiviz.services.identity.constants import DEV_CALLER_SERVICE_ACCOUNT

TEST_PRODUCT_APP_BY_SERVICE_ACCOUNT: dict[str, ProductApp | None] = {
    DEV_CALLER_SERVICE_ACCOUNT: ProductApp.ADMIN_PANEL,
}

STRANGER_SERVICE_ACCOUNT = "stranger_service_account"
NO_APP_SERVICE_ACCOUNT = "no-app-caller_service_account"
ADMIN_PANEL_SERVICE_ACCOUNT = "admin-panel-caller_service_account"
MAPPED_SERVICE_ACCOUNT = "mapped_service_account"
DEFAULT_MAPPING = {MAPPED_SERVICE_ACCOUNT: ProductApp.ADMIN_PANEL}


@contextmanager
def mock_iap_environment(
    mapping: Mapping[str, ProductApp | None] | None = None,
    authenticated_as: str | None = None,
    invalid_jwt: bool = False,
    in_development: bool = False,
) -> Iterator[None]:
    """Patch the Identity Service IAP auth dependencies for the duration of the block.

    `mapping` replaces the static caller-to-source_product_app mapping.
    `authenticated_as` makes the JWT validator report that email as the
    authenticated caller. `invalid_jwt` makes the validator reject any header.
    `in_development` activates the decorator's dev bypass (no JWT validation)
    and the server's dev caller-email default.
    """
    if authenticated_as is not None and invalid_jwt:
        raise ValueError("authenticated_as and invalid_jwt are mutually exclusive")

    if invalid_jwt:
        jwt_return: tuple[str | None, str | None, str | None] = (
            None,
            None,
            "INVALID TOKEN",
        )
    else:
        jwt_return = ("sub-id", authenticated_as, None)

    with patch(
        "recidiviz.utils.auth.gce.in_development",
        return_value=in_development,
    ), patch(
        "recidiviz.services.identity.server.in_development",
        return_value=in_development,
    ), patch(
        "recidiviz.utils.metadata.project_number",
        return_value="123456789",
    ), patch(
        "recidiviz.utils.auth.gce.get_secret",
        return_value="987654321",
    ), patch(
        "recidiviz.utils.validate_jwt.validate_iap_jwt_from_compute_engine",
        return_value=jwt_return,
    ), patch.dict(
        "recidiviz.services.identity.helpers.PRODUCT_APP_BY_SERVICE_ACCOUNT",
        mapping or TEST_PRODUCT_APP_BY_SERVICE_ACCOUNT,
        clear=True,
    ):
        yield
