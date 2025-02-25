# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Alphabetize RAW_DATA_REFERENCES_YAML.
"""

from typing import Dict, Set

from ruamel.yaml import YAML

from recidiviz.tools.raw_data_reference_reasons_yaml_loader import (
    RAW_DATA_REFERENCES_YAML_PATH,
    RawDataReferenceReasonsYamlLoader,
)


def sort_yaml_file(yaml_path: str = RAW_DATA_REFERENCES_YAML_PATH) -> None:
    raw_data = RawDataReferenceReasonsYamlLoader.get_raw_yaml_data()
    if not is_sorted(raw_data):
        sorted_data = _sort_dict(raw_data)
        with open(yaml_path, "w", encoding="utf-8") as file:
            YAML().dump(sorted_data, file)


def _sort_dict(d: Dict) -> Dict:
    sorted_dict = {}
    for key in sorted(d):
        value = d[key]
        sorted_dict[key] = _sort_dict(value) if isinstance(value, dict) else value
    return sorted_dict


def is_sorted(references: Dict[str, Dict[str, Set[str]]]) -> bool:
    state_codes = references.keys()
    if list(state_codes) != sorted(state_codes):
        return False
    for file_tag_to_addresses in references.values():
        file_tags = file_tag_to_addresses.keys()
        if list(file_tags) != sorted(file_tags):
            return False
        for addresses in file_tag_to_addresses.values():
            if list(addresses) != sorted(addresses):
                return False
    return True


if __name__ == "__main__":
    sort_yaml_file()
