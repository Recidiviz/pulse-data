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
"""Synchronizes an in-memory raw data file configuration object with its on-disk YAML representations"""

import copy
import logging
from pathlib import Path
from typing import Any, Callable, Optional

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from recidiviz.ingest.direct.raw_data.raw_data_ruamel_yaml_update import (
    convert_raw_file_config_to_dict,
    load_ruaml_yaml,
    update_ruamel_for_raw_file,
    write_ruamel_yaml_to_file,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)


def update_ruaml_for_raw_file_and_persist(
    ruamel_yaml: CommentedMap,
    output_file_path: Path,
    original_config_dict: dict[str, DirectIngestRawFileConfig],
    updated_config_dict: dict[str, DirectIngestRawFileConfig],
) -> None:
    """Applies changes to an existing ruamel.yaml object and persists it to disk. It updates ruamel_yaml with differences between original_config_dict and updated_config_dict, writing to output_file_path only if actual changes are present."""
    if original_config_dict != updated_config_dict:
        updated_yaml = update_ruamel_for_raw_file(
            ruamel_yaml,
            original_config_dict,
            updated_config_dict,
        )
        write_ruamel_yaml_to_file(output_file_path, updated_yaml)
    else:
        logging.debug(
            "No changes detected for %s, skipping file write.", output_file_path.name
        )


def update_region_raw_file_yamls(
    region_raw_file_config: DirectIngestRegionRawFileConfig,
    config_updater_fn: Callable[
        [DirectIngestRegionRawFileConfig], DirectIngestRegionRawFileConfig
    ],
) -> None:
    """Manages the update process for all raw data YAML files within a specific region."""
    original_file_configs_by_tag: dict[str, DirectIngestRawFileConfig] = copy.deepcopy(
        region_raw_file_config.raw_file_configs
    )
    original_dicts_by_file_tag: dict[str, dict[str, Any]] = {
        file_tag: convert_raw_file_config_to_dict(
            file_config, region_raw_file_config.default_config()
        )
        for file_tag, file_config in original_file_configs_by_tag.items()
    }
    original_ruamls_by_file_tag: dict[str, Optional[CommentedMap]] = {
        file_tag: load_ruaml_yaml(file_config.file_path)
        for file_tag, file_config in original_file_configs_by_tag.items()
    }

    updated_region_raw_file_config = config_updater_fn(region_raw_file_config)
    updated_file_configs_by_tag: dict[
        str, DirectIngestRawFileConfig
    ] = updated_region_raw_file_config.raw_file_configs
    updated_dicts_by_file_tag: dict[str, dict[str, Any]] = {
        file_tag: convert_raw_file_config_to_dict(
            file_config, region_raw_file_config.default_config()
        )
        for file_tag, file_config in updated_file_configs_by_tag.items()
    }

    all_file_tags: set[str] = set(updated_dicts_by_file_tag.keys()) | set(
        original_dicts_by_file_tag.keys()
    )
    for file_tag in all_file_tags:
        original_repr = original_dicts_by_file_tag.get(file_tag)
        updated_repr = updated_dicts_by_file_tag.get(file_tag)
        file_path_for_tag: Path
        if file_tag in updated_file_configs_by_tag:
            file_path_for_tag = Path(updated_file_configs_by_tag[file_tag].file_path)
        elif file_tag in original_file_configs_by_tag:
            file_path_for_tag = Path(original_file_configs_by_tag[file_tag].file_path)
        else:
            raise ValueError(
                f"No explicit file path found for tag '{file_tag}'. Cannot proceed with file management."
            )

        if original_repr is None and updated_repr is not None:
            logging.debug("Handling new file: %s", file_tag)
            new_ruamel_data = YAML().map()
            update_ruaml_for_raw_file_and_persist(
                ruamel_yaml=new_ruamel_data,
                output_file_path=file_path_for_tag,
                original_config_dict={},
                updated_config_dict=updated_repr,
            )

        elif original_repr is not None and updated_repr is None:
            logging.debug("Handling deleted file: %s", file_tag)
            if file_path_for_tag.exists():
                file_path_for_tag.unlink()
                logging.info("Deleted raw file YAML: %s", file_path_for_tag)
            else:
                logging.info(
                    "Warning: Attempted to delete non-existent file for tag '%s': %s",
                    file_tag,
                    file_path_for_tag,
                )

        elif original_repr is not None and updated_repr is not None:
            logging.debug("Handling existing file: %s", file_tag)
            current_ruamel_data = original_ruamls_by_file_tag[file_tag]
            if current_ruamel_data is not None:
                update_ruaml_for_raw_file_and_persist(
                    current_ruamel_data,
                    file_path_for_tag,
                    original_repr,
                    updated_repr,
                )
