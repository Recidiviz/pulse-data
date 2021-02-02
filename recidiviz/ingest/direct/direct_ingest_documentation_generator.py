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

"""Functionality for generating documentation about our direct ingest integrations."""


from pytablewriter import MarkdownTableWriter

from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRegionRawFileConfig, \
    DirectIngestRawFileConfig


class DirectIngestDocumentationGenerator:
    """A class for generating documentation about our direct ingest integrations."""

    def generate_raw_file_docs_for_region(self, region_code: str) -> str:
        """Generates documentation for all raw file configs for the given region and returns all of it
        as a combined string."""
        region_config = DirectIngestRegionRawFileConfig(region_code=region_code)

        sorted_file_tags = sorted(region_config.raw_file_tags)
        raw_file_configs = [region_config.raw_file_configs[file_tag] for file_tag in sorted_file_tags]

        docs_per_file = [self._generate_docs_for_raw_config(config) for config in raw_file_configs]
        return '\n\n'.join(docs_per_file)

    @staticmethod
    def _generate_docs_for_raw_config(raw_file_config: DirectIngestRawFileConfig) -> str:
        """Generates documentation for the given raw file config and returns it as a string."""
        file_columns = sorted(raw_file_config.columns, key=lambda col: col.name)
        primary_key_columns = [col.upper() for col in raw_file_config.primary_key_cols]

        def _is_primary_key(column: str) -> str:
            return 'YES' if column.upper() in primary_key_columns else ''

        documentation = f"## {raw_file_config.file_tag}\n\n{raw_file_config.file_description}\n\n"

        table_matrix = [[column.name, column.description, _is_primary_key(column.name)] for column in file_columns]
        writer = MarkdownTableWriter(
            headers=["Column", "Column Description", "Part of Primary Key?"],
            value_matrix=table_matrix,
            margin=1,
        )
        documentation += writer.dumps()

        return documentation
