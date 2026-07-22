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
"""Caller authorization for the Identity Service.

Maps an authenticated caller's service account email to the role that governs
which endpoints it may call and, for product callers, the product app it writes
its records under.
"""
import enum

import attr

from recidiviz.common.constants.identity import ProductApp
from recidiviz.services.identity.constants import (
    DEV_EDITOR_SERVICE_ACCOUNT,
    DEV_IMPORTER_SERVICE_ACCOUNT,
    DEV_READER_SERVICE_ACCOUNT,
)
from recidiviz.services.identity.exceptions import UnknownCallerError
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.service_accounts import (
    get_default_compute_engine_service_account_email,
)


class CallerRole(enum.Enum):
    """Authorization role granted to an Identity Service caller, determining
    which endpoints it may call."""

    READER = "READER"
    """May read identities but not modify them."""

    EDITOR = "EDITOR"
    """May create and modify identities."""

    IMPORTER = "IMPORTER"
    """May call POST /trigger_import (the batch identity clustering DAG)."""


@attr.define(frozen=True, kw_only=True)
class IdentityServiceCaller:
    """Authorization config for an authenticated Identity Service caller."""

    role: CallerRole = attr.ib(validator=attr.validators.in_(CallerRole))
    """The role governing which endpoints this caller may call."""

    source_product_app: ProductApp | None = attr.ib(
        validator=attr.validators.optional(attr.validators.in_(ProductApp))
    )
    """The product app this caller writes its records under. Set only for EDITOR
    callers, which contribute PRODUCT_APP-sourced attributes; None for roles that
    do not write product-app attributes (the importer writes EXTERNAL_DATA_SYSTEM
    records)."""

    def __attrs_post_init__(self) -> None:
        if self.role is CallerRole.EDITOR and self.source_product_app is None:
            raise ValueError(
                "Editor callers contribute product-app-sourced attributes and "
                "must specify a source_product_app to attribute their writes to."
            )
        if self.role is not CallerRole.EDITOR and self.source_product_app is not None:
            raise ValueError(
                f"Only editor callers contribute product-app-sourced attributes, "
                f"but role [{self.role}] was given source_product_app "
                f"[{self.source_product_app}]."
            )


# Maps a caller's authenticated service account email (extracted from the
# `email` claim of the IAP JWT) to its authorization config. Callers whose email
# is not in this mapping are rejected with 403.
CALLER_BY_SERVICE_ACCOUNT: dict[str, IdentityServiceCaller] = {
    DEV_READER_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.READER, source_product_app=None
    ),
    DEV_EDITOR_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.EDITOR, source_product_app=ProductApp.ADMIN_PANEL
    ),
    DEV_IMPORTER_SERVICE_ACCOUNT: IdentityServiceCaller(
        role=CallerRole.IMPORTER, source_product_app=None
    ),
    # The Cloud Composer environment (and thus the batch identity clustering
    # DAG) runs as the project's default Compute Engine service account, in both
    # staging and production.
    get_default_compute_engine_service_account_email(
        GCP_PROJECT_STAGING
    ): IdentityServiceCaller(role=CallerRole.IMPORTER, source_product_app=None),
    get_default_compute_engine_service_account_email(
        GCP_PROJECT_PRODUCTION
    ): IdentityServiceCaller(role=CallerRole.IMPORTER, source_product_app=None),
}


def get_caller(caller_email: str | None) -> IdentityServiceCaller:
    """Returns the authorization config for the given IAP caller.

    Raises UnknownCallerError if the caller's email is not present in the static
    mapping.
    """
    if caller_email not in CALLER_BY_SERVICE_ACCOUNT:
        raise UnknownCallerError(f"Unknown caller [{caller_email}]")
    return CALLER_BY_SERVICE_ACCOUNT[caller_email]
