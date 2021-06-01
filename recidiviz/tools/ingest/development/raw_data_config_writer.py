# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Functionality for generating raw data config YAML contents."""
import os
from typing import List

from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.tests.ingest.direct.direct_ingest_util import PLACEHOLDER_TO_DO_STRING


class RawDataConfigWriter:
    """A class for generating yaml files for our DirectIngestRawFileConfigs."""

    @staticmethod
    def _get_primary_key_config_string(
        raw_table_config: DirectIngestRawFileConfig,
    ) -> str:
        if raw_table_config.primary_key_cols:
            return "\n  - " + "\n  - ".join(raw_table_config.primary_key_cols)
        return " []"

    @staticmethod
    def _get_known_values_config_string(column: RawTableColumnInfo) -> str:
        if not column.is_enum:
            raise ValueError(f"Column [{column.name}] is not an enum column.")

        if not column.known_values_nonnull:
            known_values_string = "\n    known_values: []"
        else:
            known_values_string = "\n    known_values:"
            for enum in column.known_values_nonnull:
                known_values_string += (
                    f"\n      - value: {enum.value}"
                    f"\n        description: {enum.description if enum.description else PLACEHOLDER_TO_DO_STRING}"
                )
        return known_values_string

    def _generate_individual_column_string(self, column: RawTableColumnInfo) -> str:
        if column is None or column.name is None:
            return ""

        column_string = f"  - name: {column.name}"
        if column.is_datetime:
            column_string += "\n    is_datetime: True"
        if column.description:
            column_description_string = "\n      ".join(column.description.splitlines())
            column_string += f"\n    description: |-\n      {column_description_string}"
        if column.is_enum:
            column_string += self._get_known_values_config_string(column)
        return column_string

    def _generate_columns_string(self, columns: List[RawTableColumnInfo]) -> str:
        columns_string = "columns:"
        if not columns:
            return columns_string + " []"
        columns_string += "\n"
        return columns_string + "\n".join(
            [self._generate_individual_column_string(column) for column in columns]
        )

    def output_to_file(
        self,
        raw_file_config: DirectIngestRawFileConfig,
        output_path: str,
        default_encoding: str,
        default_separator: str,
    ) -> None:
        """Writes a yaml config file to the given path for a given raw file config"""
        file_description_string = "\n  ".join(
            raw_file_config.file_description.splitlines()
        )
        config = (
            f"file_tag: {raw_file_config.file_tag}\n"
            "file_description: |-\n"
            f"  {file_description_string}\n"
            "primary_key_cols:"
            f"{self._get_primary_key_config_string(raw_file_config)}\n"
            f"{self._generate_columns_string(raw_file_config.columns)}\n"
        )

        if raw_file_config.supplemental_order_by_clause:
            config += "supplemental_order_by_clause: True\n"
        if raw_file_config.ignore_quotes:
            config += "ignore_quotes: True\n"
        if raw_file_config.always_historical_export:
            config += "always_historical_export: True\n"

        # If an encoding is not the default, we need to include it in the config
        if raw_file_config.encoding != default_encoding:
            config += f"encoding: {raw_file_config.encoding}\n"
        if raw_file_config.separator != default_separator:
            config += f"separator: '{raw_file_config.separator}'\n"
        if raw_file_config.custom_line_terminator:
            config += (
                f"custom_line_terminator: '{raw_file_config.custom_line_terminator}'\n"
            )

        prior_config = None
        if os.path.exists(output_path):
            with open(output_path, "r") as raw_data_config_file:
                prior_config = raw_data_config_file.read()

        if prior_config != config:
            with open(output_path, "w") as raw_data_config_file:
                raw_data_config_file.write(config)
