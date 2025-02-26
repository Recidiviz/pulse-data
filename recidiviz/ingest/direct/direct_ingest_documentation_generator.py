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
import datetime
import os
import subprocess
from collections import defaultdict
from typing import Dict, List, Optional

from pytablewriter import MarkdownTableWriter

import recidiviz
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.utils.string import StrictStringFormatter

STATE_RAW_DATA_FILE_HEADER_TEMPLATE = """# {state_name} Raw Data Description

All raw data can be found in append-only tables in the dataset `{raw_tables_dataset}`. Views on the raw data
table that show the latest state of this table (i.e. select the most recently received row for each primary key) can be
found in `{latest_views_dataset}`.

## Table of Contents
"""


STATE_RAW_DATA_FILE_HEADER_PATH = "raw_data.md"

AUTOFORMAT_COMMIT_REGEX = r"\[autoformat\]"


class DirectIngestDocumentationGenerator:
    """A class for generating documentation about our direct ingest integrations."""

    def generate_raw_file_docs_for_region(self, region_code: str) -> Dict[str, str]:
        """Generates documentation for all raw file configs for the given region and
        returns all of it as a combined string.

        Returns one Markdown-formatted string per raw file, mapped to its filename, as
        well as a header file with a table of contents.
        """
        region_config = DirectIngestRegionRawFileConfig(region_code=region_code)

        sorted_file_tags = sorted(region_config.raw_file_tags)

        if StateCode.is_state_code(region_code):
            state_code = StateCode(region_code.upper())
            state_name = state_code.get_state().name
            state_code_lower = state_code.value.lower()

            file_header = StrictStringFormatter().format(
                STATE_RAW_DATA_FILE_HEADER_TEMPLATE,
                state_name=state_name,
                raw_tables_dataset=raw_tables_dataset_for_region(
                    state_code_lower, sandbox_dataset_prefix=None
                ),
                latest_views_dataset=raw_latest_views_dataset_for_region(
                    state_code_lower, sandbox_dataset_prefix=None
                ),
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

        region = direct_ingest_regions.get_direct_ingest_region(region_code=region_code)

        view_collector = DirectIngestPreProcessedIngestViewCollector(region, [])
        views_by_raw_file = self.get_referencing_views(view_collector)
        touched_configs = self._get_touched_raw_data_configs(
            region_config.yaml_config_file_dir
        )

        raw_file_table = self._generate_raw_file_table(
            config_paths_by_file_tag,
            file_tags_with_raw_file_configs,
            views_by_raw_file,
            touched_configs,
        )

        docs_per_file: Dict[str, str] = {
            f"{config.file_tag}.md": self._generate_docs_for_raw_config(config)
            for config in raw_file_configs
        }

        docs_per_file[STATE_RAW_DATA_FILE_HEADER_PATH] = (
            file_header + "\n" + raw_file_table
        )

        return docs_per_file

    @staticmethod
    def _get_touched_raw_data_configs(yaml_config_file_dir: str) -> List[str]:
        relative_config_dir_path = os.path.relpath(
            yaml_config_file_dir, recidiviz.__file__
        )
        try:
            res = subprocess.run(
                rf'git diff --cached --name-only | grep "{relative_config_dir_path}.*\.yaml"',
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            return [
                os.path.basename(filepath)
                for filepath in res.stdout.decode().splitlines()
            ]
        except subprocess.CalledProcessError:
            return []

    @staticmethod
    def _generate_docs_for_raw_config(
        raw_file_config: DirectIngestRawFileConfig,
    ) -> str:
        """Generates documentation for the given raw file config and returns it as a string."""
        primary_key_columns = [col.upper() for col in raw_file_config.primary_key_cols]

        def _is_primary_key(column: str) -> str:
            return "YES" if column.upper() in primary_key_columns else ""

        def _get_enum_bullets(known_values: Optional[List[ColumnEnumValueInfo]]) -> str:
            if known_values is None:
                return "N/A"
            if not known_values:
                return "<No documentation>"
            list_contents = ", <br/>".join(
                [
                    f"<b>{enum.value}: </b> {enum.description if enum.description else 'Unknown'}"
                    for enum in known_values
                ]
            )
            return list_contents

        documentation = (
            f"## {raw_file_config.file_tag}\n\n{raw_file_config.file_description}\n\n"
        )

        table_matrix = [
            [
                column.name,
                column.description or "<No documentation>",
                _is_primary_key(column.name),
                _get_enum_bullets(column.known_values),
                column.is_pii,
            ]
            for column in raw_file_config.columns
        ]
        writer = MarkdownTableWriter(
            headers=[
                "Column",
                "Column Description",
                "Part of Primary Key?",
                "Distinct Values",
                "Is PII?",
            ],
            value_matrix=table_matrix,
            # Margin values other than 0 have nondeterministic spacing. Do not change.
            margin=0,
        )
        documentation += writer.dumps()

        return documentation

    def _generate_raw_file_table(
        self,
        config_paths_by_file_tag: Dict[str, str],
        file_tags_with_raw_file_configs: List[str],
        views_by_raw_file: Dict[str, List[str]],
        touched_configs: List[str],
    ) -> str:
        """Generates a Markdown-formatted table of contents to be included in a raw file specification."""
        table_matrix = [
            [
                (
                    f"[{file_tag}](raw_data/{file_tag}.md)"
                    if file_tag in file_tags_with_raw_file_configs
                    else f"{file_tag}"
                ),
                ",<br />".join(views_by_raw_file[file_tag]),
                self._get_last_updated(
                    config_paths_by_file_tag[file_tag], touched_configs
                ),
                self._get_updated_by(
                    config_paths_by_file_tag[file_tag], touched_configs
                ),
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
            # Margin values other than 0 have nondeterministic spacing. Do not change.
            margin=0,
        )

        return writer.dumps()

    @staticmethod
    def _get_updated_by(path: str, touched_configs: List[str]) -> str:
        """Returns the name of the person who last edited the file at the provided path"""
        if os.path.basename(path) in touched_configs:
            res = subprocess.run(
                "git config user.name",
                shell=True,
                stdout=subprocess.PIPE,
                check=True,
            )
            return res.stdout.decode()

        res = subprocess.run(
            f'git log -1 --invert-grep --grep "{AUTOFORMAT_COMMIT_REGEX}" --pretty=format:"%an" -- {path}',
            shell=True,
            stdout=subprocess.PIPE,
            check=True,
        )
        return res.stdout.decode()

    @staticmethod
    def _get_last_updated(path: str, touched_configs: List[str]) -> str:
        """Returns the date the file at the given path was last updated."""
        if os.path.basename(path) in touched_configs:
            return datetime.datetime.today().strftime("%Y-%m-%d")
        res = subprocess.run(
            f'git log -1 --invert-grep --grep "{AUTOFORMAT_COMMIT_REGEX}" --date=short --pretty=format:"%ad" -- {path}',
            shell=True,
            stdout=subprocess.PIPE,
            check=True,
        )
        return res.stdout.decode()

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
                views_by_raw_file[config.file_tag].append(ingest_view.ingest_view_name)

        return views_by_raw_file
