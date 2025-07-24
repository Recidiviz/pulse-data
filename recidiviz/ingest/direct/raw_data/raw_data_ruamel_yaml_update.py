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
"""Module to manage and upgrade YAML configurations, preserving original formatting and comments"""
from functools import partial
from pathlib import Path
from typing import Any

import attr
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileDefaultConfig,
)
from recidiviz.tools.ingest.development.raw_data_yaml_utils import (
    raw_data_yaml_attribute_filter,
)

COLUMNS_ATTRIBUTE_NAME = "_columns"
COLUMNS_YAML_KEY = COLUMNS_ATTRIBUTE_NAME.lstrip("_")


def load_ruaml_yaml(file_path: str | Path) -> CommentedMap:
    """loads yaml object based on file path"""
    yaml = YAML()
    yaml.preserve_quotes = True
    file_path = Path(file_path)
    with file_path.open("r", encoding="utf-8") as f:
        return yaml.load(f)


def convert_raw_file_config_to_dict(
    file_config: DirectIngestRawFileConfig,
    default_region_config: DirectIngestRawFileDefaultConfig,
) -> dict[str, Any]:
    """Converts a DirectIngestRawFileConfig object to a dictionary for YAML serialization by filtering out default values"""
    filtered = attr.asdict(
        file_config,
        recurse=True,
        filter=partial(
            raw_data_yaml_attribute_filter, default_region_config=default_region_config
        ),
    )

    # _columns attribute is private to avoid accidentally referencing deleted columns
    # but all columns including deleted columns are found in the raw file config yaml
    # with the key `columns`, so the key needs to be updated here
    filtered[COLUMNS_YAML_KEY] = filtered.pop(COLUMNS_ATTRIBUTE_NAME)

    return filtered


def update_ruamel_dict(
    ruamel_data: dict[str, Any],
    original_dict: dict[str, Any],
    updated_dict: dict[str, Any],
) -> None:
    """Recursively updates a ruamel dict in place by comparing changes between updated_dict and original_dict"""
    for key, updated_val in updated_dict.items():
        original_val = original_dict.get(key)
        if updated_val == original_val:
            continue
        if isinstance(updated_val, dict) and isinstance(original_val, dict):
            update_ruamel_dict(ruamel_data[key], original_val, updated_val)
        # TODO(#44663) Handle list case
        # elif isinstance(updated_val, list) and isinstance(original_val, list):
        #     update_ruamel_list(ruamel_data[key], original_val, updated_val)
        else:
            ruamel_data[key] = updated_val
    for key in original_dict:
        if key not in updated_dict:
            ruamel_data.pop(key, None)


# def update_ruamel_list(
#     ruamel_list: list[Any], original_list: list[Any], updated_list: list[Any]
# ) -> None:
#     """Recursively updates a ruamel YAML list in place. TODO(#44663) Implement this function."""


def update_ruamel_for_raw_file(
    ruamel_yaml: CommentedMap,
    original_config_dict: dict[str, DirectIngestRawFileConfig],
    updated_config_dict: dict[str, DirectIngestRawFileConfig],
) -> CommentedMap:
    """Updates a ruamel yaml object in place by applying differences identified between original and updated raw file configurations"""
    update_ruamel_dict(ruamel_yaml, original_config_dict, updated_config_dict)
    return ruamel_yaml
