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
from typing import List, Optional

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    ImportBlockingValidationExemption,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data.raw_table_relationship_info import (
    RawDataJoinCardinality,
)
from recidiviz.tools.docs.utils import PLACEHOLDER_TO_DO_STRING


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
        """Generates a string for a single column in the yaml file."""
        if column is None or column.name is None:
            return ""

        column_string = f"  - name: {column.name}"
        if column.description:
            column_description_string = "\n      ".join(column.description.splitlines())
            column_string += f"\n    description: |-\n      {column_description_string}"

        if column.field_type != RawTableColumnFieldType.STRING:
            column_string += f"\n    field_type: {column.field_type.value}"
        if column.external_id_type:
            column_string += f"\n    external_id_type: {column.external_id_type}"
        if column.is_primary_for_external_id_type:
            column_string += f"\n    is_primary_for_external_id_type: {column.is_primary_for_external_id_type}"
        if column.datetime_sql_parsers:
            column_string += "\n    datetime_sql_parsers:"
            for parser in column.datetime_sql_parsers:
                column_datetime_sql_str = parser.replace("\\", "\\\\")
                column_string += f'\n      - "{column_datetime_sql_str}"'
        if column.is_pii:
            column_string += "\n    is_pii: True"
        if column.is_enum:
            column_string += self._get_known_values_config_string(column)
        if column.import_blocking_column_validation_exemptions:
            column_string += "\n    import_blocking_column_validation_exemptions:"
            for exemption in column.import_blocking_column_validation_exemptions:
                column_string += (
                    f"\n      - validation_type: {exemption.validation_type.value}"
                )
                column_string += (
                    f"\n        exemption_reason: {exemption.exemption_reason}"
                )
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
        default_ignore_quotes: bool,
        default_always_historical_export: bool,
        default_no_valid_primary_keys: bool,
        default_line_terminator: Optional[str],
        default_update_cadence: Optional[RawDataFileUpdateCadence],
        default_import_blocking_validation_exemptions: Optional[
            List[ImportBlockingValidationExemption]
        ],
    ) -> None:
        """Writes a yaml config file to the given path for a given raw file config"""
        file_description_string = "\n  ".join(
            raw_file_config.file_description.splitlines()
        )
        config = (
            "# yaml-language-server: $schema=./../../../raw_data/yaml_schema/schema.json\n"
            f"file_tag: {raw_file_config.file_tag}\n"
            "file_description: |-\n"
            f"  {file_description_string}\n"
            f"data_classification: {raw_file_config.data_classification.value}\n"
        )
        # If whether to treat raw files as having valid primary keys is not the default,
        # we need to include it in the config
        if raw_file_config.no_valid_primary_keys != default_no_valid_primary_keys:
            config += (
                f"no_valid_primary_keys: {raw_file_config.no_valid_primary_keys}\n"
            )
        config += (
            "primary_key_cols:"
            f"{self._get_primary_key_config_string(raw_file_config)}\n"
            f"{self._generate_columns_string(raw_file_config.columns)}\n"
        )
        if raw_file_config.is_primary_person_table:
            config += "is_primary_person_table: True\n"
        if raw_file_config.supplemental_order_by_clause:
            config += "supplemental_order_by_clause: True\n"
        if raw_file_config.infer_columns_from_config:
            config += "infer_columns_from_config: True\n"

        # If an encoding is not the default, we need to include it in the config
        if raw_file_config.encoding != default_encoding:
            config += f"encoding: {raw_file_config.encoding}\n"
        # If a separator is not the default, we need to include it in the config
        if raw_file_config.separator != default_separator:
            config += f'separator: "{raw_file_config.separator}"\n'
        # If whether to ignore quotes is not the default, we need to include it in the config
        if raw_file_config.ignore_quotes != default_ignore_quotes:
            config += f"ignore_quotes: {raw_file_config.ignore_quotes}\n"
        # If whether to always treat raw files as historical exports is not the default,
        # we need to include it in the config
        if raw_file_config.always_historical_export != default_always_historical_export:
            config += f"always_historical_export: {raw_file_config.always_historical_export}\n"
        if raw_file_config.custom_line_terminator != default_line_terminator:
            # Convert newline, etc. to escape sequences
            custom_line_terminator_for_yaml = repr(
                raw_file_config.custom_line_terminator
            ).strip("'")
            config += f'custom_line_terminator: "{custom_line_terminator_for_yaml}"\n'
        if raw_file_config.update_cadence != default_update_cadence:
            config += f"update_cadence: {raw_file_config.update_cadence.value}\n"
        if raw_file_config.is_code_file:
            config += "is_code_file: True\n"
        if raw_file_config.is_chunked_file:
            config += "is_chunked_file: True\n"
        if raw_file_config.expected_number_of_chunks:
            config += f"expected_number_of_chunks: {raw_file_config.expected_number_of_chunks}\n"

        if raw_file_config.table_relationships:
            table_relationships_lines = ["table_relationships:"]
            for relationship in raw_file_config.table_relationships:
                relationship_lines = [
                    f"  - foreign_table: {relationship.foreign_table}",
                ]
                if relationship.cardinality != RawDataJoinCardinality.MANY_TO_MANY:
                    relationship_lines.append(
                        f"    cardinality: {relationship.cardinality.value}"
                    )

                join_list_str = "\n".join(
                    f"      - {c.to_sql()}" for c in relationship.join_clauses
                )
                relationship_lines.append(f"    join_logic:\n{join_list_str}")

                if relationship.transforms:
                    transforms_list_str = "\n".join(
                        f"      - column: {t.column}\n        transform: {t.transformation}"
                        for t in relationship.transforms
                    )
                    relationship_lines.append(f"    transforms:\n{transforms_list_str}")

                table_relationships_lines.append("\n".join(relationship_lines))
            config += "\n".join(table_relationships_lines) + "\n"

        # only write the exemptions that weren't inherited from the default
        if raw_file_config.import_blocking_validation_exemptions:
            exemptions = [
                exemption
                for exemption in raw_file_config.import_blocking_validation_exemptions
                if not default_import_blocking_validation_exemptions
                or exemption not in default_import_blocking_validation_exemptions
            ]
            if exemptions:
                config += "import_blocking_validation_exemptions:\n"
                for exemption in exemptions:
                    config += (
                        f"  - validation_type: {exemption.validation_type.value}\n"
                    )
                    config += f"    exemption_reason: {exemption.exemption_reason}\n"

        prior_config = None
        if os.path.exists(output_path):
            with open(output_path, "r", encoding="utf-8") as raw_data_config_file:
                prior_config = raw_data_config_file.read()

        if prior_config != config:
            with open(output_path, "w", encoding="utf-8") as raw_data_config_file:
                raw_data_config_file.write(config)
