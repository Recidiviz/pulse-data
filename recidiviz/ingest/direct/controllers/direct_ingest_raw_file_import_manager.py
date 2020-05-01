# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Classes for performing direct ingest raw file imports to BigQuery."""
import logging
import os
from typing import List, Dict, Any, Set

import attr
import yaml

from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import filename_parts_from_path, \
    GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.utils.regions import Region


@attr.s(frozen=True)
class DirectIngestRawFileConfig:
    """Struct for storing any configuration for raw data imports for a certain file tag."""

    file_tag: str = attr.ib(validator=attr.validators.instance_of(str))
    primary_key_cols: List[str] = attr.ib(validator=attr.validators.instance_of(list))

    @classmethod
    def from_dict(cls, file_config_dict: Dict[str, Any]) -> 'DirectIngestRawFileConfig':
        return DirectIngestRawFileConfig(
            file_tag=file_config_dict['file_tag'],
            primary_key_cols=file_config_dict['primary_key_cols']
        )


@attr.s(frozen=True)
class DirectIngestRawFileImportManager:
    """Class that parses and stores raw data import configs for a region, with functionality for executing an
    import of a specific file.
    """

    region: Region = attr.ib()
    fs: DirectIngestGCSFileSystem = attr.ib()
    ingest_directory_path: GcsfsDirectoryPath = attr.ib()

    yaml_config_file_path: str = attr.ib()

    @yaml_config_file_path.default
    def _config_file_path(self):
        return os.path.join(os.path.dirname(__file__),
                            '..',
                            'regions',
                            f'{self.region.region_code}',
                            f'{self.region.region_code}_raw_data_files.yaml')

    raw_file_configs: List[DirectIngestRawFileConfig] = attr.ib()

    @raw_file_configs.default
    def _get_raw_data_file_configs(self) -> List[DirectIngestRawFileConfig]:
        """Returns list of file tags we expect to see on raw files for this region."""
        with open(self.yaml_config_file_path, 'r') as yaml_file:
            file_contents = yaml.full_load(yaml_file)
            if not isinstance(file_contents, dict):
                raise ValueError(
                    f'File contents for [{self.yaml_config_file_path}] have unexpected type [{type(file_contents)}].')

            raw_data_configs = [DirectIngestRawFileConfig.from_dict(file_info)
                                for file_info in file_contents['raw_files']]

        return raw_data_configs

    raw_file_tags: Set[str] = attr.ib()

    @raw_file_tags.default
    def _raw_file_tags(self):
        return {config.file_tag for config in self.raw_file_configs}

    def get_unprocessed_raw_files_to_import(self) -> List[GcsfsFilePath]:
        if not self.region.are_raw_data_bq_imports_enabled_in_env():
            raise ValueError(f'Cannot import raw files for region [{self.region.region_code}]')

        unprocessed_paths = self.fs.get_unprocessed_file_paths(self.ingest_directory_path,
                                                               GcsfsDirectIngestFileType.RAW_DATA)
        paths_to_import = []
        for path in unprocessed_paths:
            parts = filename_parts_from_path(path)
            if parts.file_tag in self.raw_file_tags:
                paths_to_import.append(path)
            else:
                logging.warning('Unrecognized raw file tag [%s] for region [%s].',
                                parts.file_tag, self.region.region_code)

        return paths_to_import

    def import_raw_file_to_big_query(self, path: GcsfsFilePath) -> None:
        if not self.region.are_raw_data_bq_imports_enabled_in_env():
            raise ValueError(f'Cannot import raw files for region [{self.region.region_code}]')

        parts = filename_parts_from_path(path)
        if parts.file_tag not in self.raw_file_tags:
            raise ValueError(
                f'Attempting to import raw file with tag [{parts.file_tag}] unspecified by [{self.region.region_code}] '
                f'config.')

        if parts.file_type != GcsfsDirectIngestFileType.RAW_DATA:
            raise ValueError(f'Unexpected file type [{parts.file_type}] for path [{parts.file_tag}].')

        # TODO(3020): Implement actual BQ upload
        raise ValueError('Unimplemented!')
