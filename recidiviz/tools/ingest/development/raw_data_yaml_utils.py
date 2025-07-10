#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Utility functions to support yaml serialization for raw data config files"""
from typing import Any

import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    EXCLUDE_FROM_YAML,
    DirectIngestRawFileDefaultConfig,
)

DEFAULT_CONFIG_ATTRIBUTE_PREFIX = "default_"


def raw_data_yaml_attribute_filter(
    attribute: attr.Attribute,
    value: Any,
    *,
    default_region_config: DirectIngestRawFileDefaultConfig,
) -> bool:
    """
    Determines whether a field should be included in the YAML output
    based on metadata, defaults, and falsey-ness.
    """
    if attribute.metadata.get(EXCLUDE_FROM_YAML) is True:
        return False

    if not isinstance(value, bool) and not value:
        return False

    if attribute.default is not attr.NOTHING:
        default_val = (
            attribute.default() if callable(attribute.default) else attribute.default
        )
        if value == default_val:
            return False

    default_config_value_name = f"{DEFAULT_CONFIG_ATTRIBUTE_PREFIX}{attribute.name}"
    if hasattr(default_region_config, default_config_value_name) and value == getattr(
        default_region_config, default_config_value_name
    ):
        return False

    return True
