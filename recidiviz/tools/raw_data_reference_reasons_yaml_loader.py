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
"""Load and parse RAW_DATA_REFERENCES_YAML."""

import os
from enum import Enum
from typing import Dict, Set

from ruamel.yaml import YAML

import recidiviz
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.utils import environment

RAW_DATA_REFERENCES_YAML = "view_registry/raw_data_reference_reasons.yaml"
RAW_DATA_REFERENCES_YAML_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    RAW_DATA_REFERENCES_YAML,
)

VARIANTS_KEY = "variants"


class RawDataReferenceVariant(Enum):
    """The flavor of raw data a downstream view reads for a given file tag."""

    # The `us_xx_raw_data_views.<file_tag>_all` view (all non-deleted rows).
    ALL = "ALL"
    # The `us_xx_raw_data_up_to_date_views.<file_tag>_latest` view (most recent rows).
    LATEST = "LATEST"
    # The `us_xx_raw_data.<file_tag>` raw data table directly.
    TABLE = "TABLE"


class RawDataReferenceReasonsYamlLoader:
    """Class responsible for loading and parsing RAW_DATA_REFERENCES_YAML."""

    _yaml_data: Dict[
        StateCode, Dict[str, Dict[BigQueryAddress, Set[RawDataReferenceVariant]]]
    ] = {}
    _raw_yaml_data: Dict[str, Dict[str, Dict[str, Dict]]] = {}

    @classmethod
    def get_raw_yaml_data(cls) -> Dict[str, Dict[str, Dict[str, Dict]]]:
        if not cls._raw_yaml_data:
            cls._load_yaml()
        return cls._raw_yaml_data

    @classmethod
    def get_yaml_data(
        cls,
    ) -> Dict[
        StateCode, Dict[str, Dict[BigQueryAddress, Set[RawDataReferenceVariant]]]
    ]:
        if not cls._yaml_data:
            cls._yaml_data = cls._convert_raw_yaml_data_to_objs(cls.get_raw_yaml_data())
        return cls._yaml_data

    @classmethod
    @environment.test_only
    def reset_data(cls) -> None:
        cls._yaml_data = {}
        cls._raw_yaml_data = {}

    @classmethod
    def get_downstream_referencing_views(
        cls, state_code: StateCode
    ) -> Dict[str, Set[BigQueryAddress]]:
        """Get raw data filetags and the downstream views that reference them for a
        given region code."""
        return {
            file_tag: set(references)
            for file_tag, references in cls.get_yaml_data().get(state_code, {}).items()
        }

    @classmethod
    def get_referenced_variants_by_file_tag(
        cls, state_code: StateCode
    ) -> Dict[str, Set[RawDataReferenceVariant]]:
        """Get, per raw data file tag, the set of variants (`_all`, `_latest`, raw
        table) read by any downstream view for a given region code."""
        return {
            file_tag: set().union(*references.values())
            for file_tag, references in cls.get_yaml_data().get(state_code, {}).items()
        }

    @classmethod
    def _load_yaml(cls, yaml_path: str = RAW_DATA_REFERENCES_YAML_PATH) -> None:
        try:
            with open(yaml_path, "r", encoding="utf-8") as yaml_file:
                yaml = YAML()
                cls._raw_yaml_data = yaml.load(yaml_file)
        except Exception as e:
            raise RuntimeError(
                f"Failed to load or parse YAML data from {yaml_path}: {e}"
            ) from e

    @staticmethod
    def _convert_raw_yaml_data_to_objs(
        references: Dict[str, Dict[str, Dict[str, Dict]]],
    ) -> Dict[
        StateCode, Dict[str, Dict[BigQueryAddress, Set[RawDataReferenceVariant]]]
    ]:
        return {
            StateCode(state_code): {
                file_tag: {
                    BigQueryAddress.from_str(view): {
                        RawDataReferenceVariant(variant)
                        for variant in reference[VARIANTS_KEY]
                    }
                    for view, reference in views.items()
                }
                for file_tag, views in file_tags.items()
            }
            for state_code, file_tags in references.items()
        }
