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
"""Resolves per-state eOMIS credentials.

Each of the username and password resolves independently, in order: an
explicit override (attended runs), an environment variable (local runs), then
Google Secret Manager (scheduled production runs). Credentials are scoped to
one state and must never be shared across states or logged.
"""
from __future__ import annotations

import os

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.secrets import get_secret


@attr.define(frozen=True, kw_only=True)
class EomisCredentials:
    """A username/password pair for one state's eOMIS instance. Both fields
    are excluded from repr so instances can never leak secrets into logs or
    error messages."""

    username: str = attr.ib(validator=attr_validators.is_non_empty_str, repr=False)
    """The eOMIS service account username."""

    password: str = attr.ib(validator=attr_validators.is_non_empty_str, repr=False)
    """The eOMIS service account password."""


def eomis_env_var_name(state_code: StateCode, field: str) -> str:
    """Returns the environment variable holding the given credential field,
    e.g. EOMIS_AR_USERNAME for (US_AR, "username")."""
    return f"EOMIS_{state_code.value.removeprefix('US_')}_{field.upper()}"


def eomis_secret_id(state_code: StateCode, field: str) -> str:
    """Returns the Google Secret Manager secret id holding the given
    credential field, e.g. eomis_us_ar_username for (US_AR, "username")."""
    return f"eomis_{state_code.value.lower()}_{field.lower()}"


def _resolve_field(state_code: StateCode, field: str, override: str | None) -> str:
    if override:
        return override
    if from_env := os.environ.get(eomis_env_var_name(state_code, field)):
        return from_env
    if from_secret := get_secret(eomis_secret_id(state_code, field)):
        return from_secret
    raise ValueError(
        f"Unable to resolve the eOMIS {field} for [{state_code.value}]: "
        f"set [{eomis_env_var_name(state_code, field)}] or configure the "
        f"[{eomis_secret_id(state_code, field)}] secret."
    )


def resolve_eomis_credentials(
    state_code: StateCode,
    *,
    username_override: str | None,
    password_override: str | None,
) -> EomisCredentials:
    """Returns the eOMIS credentials for |state_code|, resolving the username
    and password each in order: explicit override, environment variable,
    Google Secret Manager. Raises if either cannot be resolved."""
    return EomisCredentials(
        username=_resolve_field(state_code, "username", username_override),
        password=_resolve_field(state_code, "password", password_override),
    )
