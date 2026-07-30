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
"""Helpers defining the `$env` properties an ingest mapping may reference.

There are two flavors of `$env` property: the built-in environment properties
(is_local/is_staging/is_production), and one property per registered ingest
feature flag, exposed with a `feature__` prefix. For example, a flag keyed
`tomis_2_0_enabled` in `feature_flags_registry.py` can gate whether an ingest
view launches at all:

    launch_env:
      $env: feature__tomis_2_0_enabled

... or the inverse, via the standard boolean combinators:

    launch_env:
      $not:
        $env: feature__tomis_2_0_enabled

... or an individual field within the mapping's `output`:

    supervision_level:
      $conditional:
        - $if:
            $env: feature__tomis_2_0_enabled
          $then: new_supervision_level_col
        - $else: legacy_supervision_level_col

Adding a flag to the registry is all that is required to make it usable here.
This module deliberately knows nothing about which flags exist -- it only
understands the shape of an `$env` property name. That a mapping references a
*registered* flag is enforced by
`direct_ingest_region_structure_test.py`, which checks every real
mapping in every state against the registry.
"""
from typing import Type

# Prefix distinguishing a feature flag from the built-in `$env` properties in
# ingest mapping YAML.
FEATURE_FLAG_ENV_PROPERTY_PREFIX = "feature__"

# Supported non-feature-flag $env properties.
IS_LOCAL_PROPERTY_NAME = "is_local"
IS_STAGING_PROPERTY_NAME = "is_staging"
IS_PRODUCTION_PROPERTY_NAME = "is_production"

BUILT_IN_ENV_PROPERTY_NAMES = (
    IS_LOCAL_PROPERTY_NAME,
    IS_STAGING_PROPERTY_NAME,
    IS_PRODUCTION_PROPERTY_NAME,
)


def env_property_name_for_flag(flag_name: str) -> str:
    """Returns the `$env` property name that exposes the given feature flag
    to ingest mapping YAML.
    """
    return f"{FEATURE_FLAG_ENV_PROPERTY_PREFIX}{flag_name}"


def feature_flag_for_env_property(property_name: str) -> str | None:
    """Returns the name of the feature flag referenced by the given `$env`
    property name, or None if that property is not a feature flag reference.

    Raises if the property carries the feature flag prefix but names no
    flag after it. Does not check that the returned flag is one that
    actually exists.
    """
    if not property_name.startswith(FEATURE_FLAG_ENV_PROPERTY_PREFIX):
        return None
    if not (flag_name := property_name[len(FEATURE_FLAG_ENV_PROPERTY_PREFIX) :]):
        raise ValueError(
            f"Found `$env` property [{property_name}] with the feature flag "
            f"prefix [{FEATURE_FLAG_ENV_PROPERTY_PREFIX}] but no flag name "
            f"after it."
        )
    return flag_name


def is_feature_flag_env_property(property_name: str) -> bool:
    """Returns whether the given `$env` property name refers to a feature flag.

    Raises if the property carries the feature flag prefix but names no
    flag after it. Does not check that the referenced flag is one that
    actually exists.
    """
    return feature_flag_for_env_property(property_name) is not None


def env_property_type(property_name: str) -> Type:
    """Returns the expected value type for the given `$env` property, raising if
    the property is not one an ingest mapping may reference.
    """
    if property_name in BUILT_IN_ENV_PROPERTY_NAMES:
        return bool

    if is_feature_flag_env_property(property_name):
        return bool

    raise ValueError(f"Unexpected environment property: [{property_name}]")
