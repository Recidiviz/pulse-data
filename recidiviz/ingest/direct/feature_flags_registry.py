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
"""Registry of feature flags that can be used to gate ingest logic.

Each flag is a named function that answers "is this feature enabled for the
given project?". Gating code can call a flag function directly (e.g. to pick
which raw data table a view query should read from), and every registered
flag is additionally exposed to ingest mapping YAML as a `feature__`-prefixed
`$env` property -- see `ingest_mappings/env_property_utils.py`.

IMPORTANT: a flag function must be a pure function of the `project_id` it is
handed. It must not consult ambient state such as `metadata.project_id()` or
`environment.in_gcp_production()`. Callers ask what is enabled *for a given
project*, and that project is not always the one the current process is running
in.
"""
from typing import Callable


def is_tomis_2_0_enabled(
    project_id: str,  # pylint: disable=unused-argument
) -> bool:
    """Returns whether the data platform should read TOMIS 2.0 data (rather than
    TOMIS 1.0 data) for the given project.

    TODO(TN-1892): Delete this flag, and any logic gated on it, once TOMIS
    2.0 has fully rolled out in production.
    """
    return False


# All feature flags available to gate ingest logic. Keys are bare, snake_case
# names and values are pure functions of the project the logic is being
# evaluated for. By convention a flag keyed `foo_enabled` is backed by a
# function named `is_foo_enabled`.
_INGEST_FEATURE_FLAGS: dict[str, Callable[[str], bool]] = {
    "tomis_2_0_enabled": is_tomis_2_0_enabled,
}


def all_feature_flag_names() -> list[str]:
    """Returns the sorted names of every registered feature flag."""
    return sorted(_INGEST_FEATURE_FLAGS)


def resolve_ingest_feature_flags(project_id: str) -> dict[str, bool]:
    """Returns the value of every registered feature flag, evaluated for the
    given project.
    """
    return {
        flag_name: is_enabled_fn(project_id)
        for flag_name, is_enabled_fn in _INGEST_FEATURE_FLAGS.items()
    }
