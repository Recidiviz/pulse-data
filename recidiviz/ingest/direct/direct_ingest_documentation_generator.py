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
from collections import defaultdict
from typing import List, Dict
import subprocess
from pytablewriter import MarkdownTableWriter
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
    DirectIngestRawFileConfig,
)
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.utils import regions


STATE_RAW_DATA_FILE_HEADER_TEMPLATE = """# {state_name} Raw Data Description

All raw data can be found in append-only tables in the dataset `{state_code_lower}_raw_data`. Views on the raw data
table that show the latest state of this table (i.e. select the most recently received row for each primary key) can be
found in `{state_code_lower}_raw_data_up_to_date_views`.

## Table of Contents
"""


class DirectIngestDocumentationGenerator:
    """A class for generating documentation about our direct ingest integrations."""

    def generate_raw_file_docs_for_region(self, region_code: str) -> str:
        """Generates documentation for all raw file configs for the given region and returns all of it
        as a combined string."""
        region_config = DirectIngestRegionRawFileConfig(region_code=region_code)

        sorted_file_tags = sorted(region_config.raw_file_tags)

        if StateCode.is_state_code(region_code):
            state_code = StateCode(region_code.upper())
            state_name = state_code.get_state()

            file_header = STATE_RAW_DATA_FILE_HEADER_TEMPLATE.format(
                state_name=state_name, state_code_lower=state_code.value.lower()
            )
        else:
            file_header = ""

        raw_file_configs = [
            region_config.raw_file_configs[file_tag] for file_tag in sorted_file_tags
        ]

        config_paths_by_file_tag = {
            file_tag: file_config.file_path
            for file_tag, file_config in region_config.raw_file_configs.items()
        }

        file_tags_with_raw_file_configs = [
            raw_file_config.file_tag for raw_file_config in raw_file_configs
        ]

        region = regions.get_region(region_code=region_code, is_direct_ingest=True)

        view_collector = DirectIngestPreProcessedIngestViewCollector(region, [])
        views_by_raw_file = self.get_referencing_views(view_collector)

        raw_file_table = self._generate_raw_file_table(
            config_paths_by_file_tag, file_tags_with_raw_file_configs, views_by_raw_file
        )

        docs_per_file = [
            self._generate_docs_for_raw_config(config) for config in raw_file_configs
        ]

        return file_header + "\n" + raw_file_table + "\n" + "\n\n".join(docs_per_file)

    @staticmethod
    def _generate_docs_for_raw_config(
        raw_file_config: DirectIngestRawFileConfig,
    ) -> str:
        """Generates documentation for the given raw file config and returns it as a string."""
        primary_key_columns = [col.upper() for col in raw_file_config.primary_key_cols]

        def _is_primary_key(column: str) -> str:
            return "YES" if column.upper() in primary_key_columns else ""

        documentation = (
            f"## {raw_file_config.file_tag}\n\n{raw_file_config.file_description}\n\n"
        )

        table_matrix = [
            [
                column.name,
                column.description or "<No documentation>",
                _is_primary_key(column.name),
            ]
            for column in raw_file_config.columns
        ]
        writer = MarkdownTableWriter(
            headers=["Column", "Column Description", "Part of Primary Key?"],
            value_matrix=table_matrix,
            margin=1,
        )
        documentation += writer.dumps()

        return documentation

    def _generate_raw_file_table(
        self,
        config_paths_by_file_tag: Dict[str, str],
        file_tags_with_raw_file_configs: List[str],
        views_by_raw_file: Dict[str, List[str]],
    ) -> str:
        """Generates a Markdown-formatted table of contents to be included in a raw file specification."""
        table_matrix = [
            [
                (
                    f"[{file_tag}](#{file_tag})"
                    if file_tag in file_tags_with_raw_file_configs
                    else f"{file_tag}"
                ),
                ",<br />".join(views_by_raw_file[file_tag]),
                self._get_last_updated(config_paths_by_file_tag[file_tag]),
                self._get_updated_by(config_paths_by_file_tag[file_tag]),
            ]
            for file_tag in sorted(config_paths_by_file_tag)
        ]
        writer = MarkdownTableWriter(
            headers=[
                "**Table**",
                "**Referencing Views**",
                "**Last Updated**",
                "**Updated By**",
            ],
            value_matrix=table_matrix,
            margin=1,
        )

        return writer.dumps()

    @staticmethod
    def _get_updated_by(path: str) -> str:
        """Returns the name of the person who last edited the file at the provided path"""
        res = subprocess.Popen(
            f'git log -1 --pretty=format:"%an" -- {path}',
            shell=True,
            stdout=subprocess.PIPE,
        )
        stdout, _stderr = res.communicate()
        return stdout.decode()

    @staticmethod
    def _get_last_updated(path: str) -> str:
        """Returns the date the file at the given path was last updated."""
        res = subprocess.Popen(
            f'git log -1 --date=short --pretty=format:"%ad" -- {path}',
            shell=True,
            stdout=subprocess.PIPE,
        )
        stdout, _stderr = res.communicate()
        return stdout.decode()

    @staticmethod
    def get_referencing_views(
        view_collector: DirectIngestPreProcessedIngestViewCollector,
    ) -> Dict[str, List[str]]:
        """Generates a dictionary mapping raw files to ingest views that reference them"""
        views_by_raw_file = defaultdict(list)

        for builder in view_collector.collect_view_builders():
            ingest_view = builder.build()
            dependency_configs = ingest_view.raw_table_dependency_configs
            for config in dependency_configs:
                views_by_raw_file[config.file_tag].append(ingest_view.file_tag)

        return views_by_raw_file
