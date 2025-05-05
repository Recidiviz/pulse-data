# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Utils for inspecting / comparing BigQuery ExternalConfig objects."""
from typing import Any

from google.cloud.bigquery import ExternalConfig


def _external_config_no_schema(external_config: ExternalConfig) -> ExternalConfig:
    """Returns a copy of the given external_config, with all schema info cleared."""
    desired_data_config_dict = external_config.to_api_repr()
    desired_data_config_dict.pop("schema", None)
    return ExternalConfig.from_api_repr(desired_data_config_dict)


def get_external_config_non_schema_updates(
    current_table_external_config: ExternalConfig,
    desired_external_config: ExternalConfig,
) -> dict[str, tuple[Any, Any]]:
    """Returns a dictionary where each key is a field that updated between
    |current_table_external_config| and |desired_external_config| and values are a tuple
    of (old value, new value). Ignores all schema updates.
    """
    old_config = _external_config_no_schema(current_table_external_config).to_api_repr()
    new_config = _external_config_no_schema(desired_external_config).to_api_repr()
    differences = {}
    for key in sorted(set(old_config) | set(new_config)):
        old_val = old_config.get(key, None)
        new_val = new_config.get(key, None)
        if old_val != new_val:
            differences[key] = (old_val, new_val)
    return differences


def external_config_has_non_schema_updates(
    current_table_external_config: ExternalConfig,
    desired_external_config: ExternalConfig,
) -> bool:
    """Returns True if the |desired_external_config| has updates as compared to
    |current_table_external_config|, ignoring all schema updates.
    """
    return bool(
        get_external_config_non_schema_updates(
            current_table_external_config=current_table_external_config,
            desired_external_config=desired_external_config,
        )
    )
