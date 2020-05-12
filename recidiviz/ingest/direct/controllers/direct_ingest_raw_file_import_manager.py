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
import csv
import logging
import os
import string
from typing import List, Dict, Any, Set, Optional, Iterator, Tuple

import attr
import pandas as pd
import yaml
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.controllers.direct_ingest_file_metadata_manager import GcsfsDirectIngestFileMetadata
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystem, \
    GcsfsFileContentsHandle
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import filename_parts_from_path, \
    GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.utils.regions import Region


@attr.s(frozen=True)
class DirectIngestRawFileConfig:
    """Struct for storing any configuration for raw data imports for a certain file tag."""

    # The file tag / table name that this file will get written to
    file_tag: str = attr.ib(validator=attr.validators.instance_of(str))

    # A list of columns that constitute the primary key for this file
    primary_key_cols: List[str] = attr.ib(validator=attr.validators.instance_of(list))

    # String encoding for this file (e.g. UTF-8)
    encoding: str = attr.ib()

    # The separator character used to denote columns (e.g. ',' or '|').
    separator: str = attr.ib()

    # If true, quoted strings are ignored and separators inside of quotes are treated as column separators. This should
    # be used on any file that has free text fields where the quotes are not escaped and the separator is not common to
    # free text. For example, to handle this row from a pipe separated file that has an open quotation with no close
    # quote:
    #     123|456789|2|He said, "I will be there.|ASDF
    ignore_quotes: bool = attr.ib()

    # A comma-separated string representation of the primary keys
    primary_key_str = attr.ib()

    @primary_key_str.default
    def _primary_key_str(self):
        return ", ".join(self.primary_key_cols)

    @classmethod
    def from_dict(cls, file_config_dict: Dict[str, Any]) -> 'DirectIngestRawFileConfig':
        return DirectIngestRawFileConfig(
            file_tag=file_config_dict['file_tag'],
            primary_key_cols=file_config_dict['primary_key_cols'],
            encoding=file_config_dict['encoding'],
            separator=file_config_dict['separator'],
            ignore_quotes=file_config_dict.get('ignore_quotes', False)
        )


@attr.s
class DirectIngestRegionRawFileConfig:
    """Class that parses and stores raw data import configs for a region"""

    region_code: str = attr.ib()
    yaml_config_file_path: str = attr.ib()

    @yaml_config_file_path.default
    def _config_file_path(self):
        return os.path.join(os.path.dirname(__file__),
                            '..',
                            'regions',
                            f'{self.region_code.lower()}',
                            f'{self.region_code.lower()}_raw_data_files.yaml')

    raw_file_configs: Dict[str, DirectIngestRawFileConfig] = attr.ib()

    @raw_file_configs.default
    def _raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        return self._get_raw_data_file_configs()

    def _get_raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        """Returns list of file tags we expect to see on raw files for this region."""
        with open(self.yaml_config_file_path, 'r') as yaml_file:
            file_contents = yaml.full_load(yaml_file)
            if not isinstance(file_contents, dict):
                raise ValueError(
                    f'File contents for [{self.yaml_config_file_path}] have unexpected type [{type(file_contents)}].')

            raw_data_configs = {}
            default_encoding = file_contents['default_encoding']
            default_separator = file_contents['default_separator']
            for file_info in file_contents['raw_files']:
                file_tag = file_info['file_tag']

                if file_tag in raw_data_configs:
                    raise ValueError(f'Found duplicate file tag [{file_tag}] in [{self.yaml_config_file_path}]')

                config = {
                    'encoding': default_encoding,
                    'separator': default_separator,
                    **file_info
                }

                raw_data_configs[file_tag] = DirectIngestRawFileConfig.from_dict(config)

        return raw_data_configs

    raw_file_tags: Set[str] = attr.ib()

    @raw_file_tags.default
    def _raw_file_tags(self):
        return set(self.raw_file_configs.keys())


_FILE_ID_COL_NAME = 'file_id'
_UPDATE_DATETIME_COL_NAME = 'update_datetime'
_DEFAULT_BQ_UPLOAD_CHUNK_SIZE = 500000

class DirectIngestRawFileImportManager:
    """Class that stores raw data import configs for a region, with functionality for executing an import of a specific
    file.
    """

    def __init__(self,
                 *,
                 region: Region,
                 fs: DirectIngestGCSFileSystem,
                 ingest_directory_path: GcsfsDirectoryPath,
                 temp_output_directory_path: GcsfsDirectoryPath,
                 big_query_client: BigQueryClient,
                 region_raw_file_config: Optional[DirectIngestRegionRawFileConfig] = None,
                 upload_chunk_size: int = _DEFAULT_BQ_UPLOAD_CHUNK_SIZE):

        self.region = region
        self.fs = fs
        self.ingest_directory_path = ingest_directory_path
        self.temp_output_directory_path = temp_output_directory_path
        self.big_query_client = big_query_client
        self.region_raw_file_config = region_raw_file_config \
            if region_raw_file_config else DirectIngestRegionRawFileConfig(region_code=self.region.region_code)
        self.upload_chunk_size = upload_chunk_size

    def get_unprocessed_raw_files_to_import(self) -> List[GcsfsFilePath]:
        if not self.region.are_raw_data_bq_imports_enabled_in_env():
            raise ValueError(f'Cannot import raw files for region [{self.region.region_code}]')

        unprocessed_paths = self.fs.get_unprocessed_file_paths(self.ingest_directory_path,
                                                               GcsfsDirectIngestFileType.RAW_DATA)
        paths_to_import = []
        for path in unprocessed_paths:
            parts = filename_parts_from_path(path)
            if parts.file_tag in self.region_raw_file_config.raw_file_tags:
                paths_to_import.append(path)
            else:
                logging.warning('Unrecognized raw file tag [%s] for region [%s].',
                                parts.file_tag, self.region.region_code)

        return paths_to_import

    @classmethod
    def raw_tables_dataset_for_region(cls, region_code: str):
        return f'{region_code.lower()}_raw_data'

    def import_raw_file_to_big_query(self,
                                     path: GcsfsFilePath,
                                     file_metadata: GcsfsDirectIngestFileMetadata) -> None:
        """Import a raw data file at the given path to the appropriate raw data table in BigQuery."""

        if not self.region.are_raw_data_bq_imports_enabled_in_env():
            raise ValueError(f'Cannot import raw files for region [{self.region.region_code}]')

        parts = filename_parts_from_path(path)
        if parts.file_tag not in self.region_raw_file_config.raw_file_tags:
            raise ValueError(
                f'Attempting to import raw file with tag [{parts.file_tag}] unspecified by [{self.region.region_code}] '
                f'config.')

        if parts.file_type != GcsfsDirectIngestFileType.RAW_DATA:
            raise ValueError(f'Unexpected file type [{parts.file_type}] for path [{parts.file_tag}].')

        logging.info('Beginning BigQuery upload of raw file [%s] - downloading raw path to local file', path.abs_path())

        contents_handle = self.fs.download_to_temp_file(path)
        if not contents_handle:
            raise ValueError(f'Failed to load path [{path.abs_path()}] to disk.')

        logging.info('Done downloading contents to local file')

        temp_output_paths = self._upload_contents_to_temp_gcs_paths(path, file_metadata, contents_handle)
        self._load_contents_to_bigquery(path, temp_output_paths)

        logging.info('Completed BigQuery import of [%s]', path.abs_path())

    def _upload_contents_to_temp_gcs_paths(
            self,
            path: GcsfsFilePath,
            file_metadata: GcsfsDirectIngestFileMetadata,
            contents_handle: GcsfsFileContentsHandle) -> List[Tuple[GcsfsFilePath, List[str]]]:
        temp_paths_with_columns = []
        logging.info('Starting chunked upload of contents to GCS')
        try:
            for i, raw_data_df in enumerate(self._read_contents_into_dataframes(path, contents_handle)):
                logging.info('Loaded DataFrame chunk [%d] has [%d] rows', i, raw_data_df.shape[0])

                augmented_df = self._augment_raw_data_with_metadata_columns(path=path,
                                                                            file_metadata=file_metadata,
                                                                            raw_data_df=raw_data_df)
                print(raw_data_df.columns)
                logging.info('Augmented DataFrame chunk [%d] has [%d] rows', i, augmented_df.shape[0])
                temp_output_path = self._get_temp_df_output_path(path, chunk_num=i)

                logging.info('Writing DataFrame chunk [%d] to temporary output path [%s]',
                             i, temp_output_path.abs_path())
                self.fs.upload_from_string(temp_output_path, augmented_df.to_csv(header=False, index=False), 'text/csv')
                logging.info('Done writing to temporary output path')

                temp_paths_with_columns.append((temp_output_path, augmented_df.columns))
        except Exception as e:
            logging.error('Failed to upload to GCS - cleaning up temp paths')
            self._delete_temp_output_paths([path for path, _ in temp_paths_with_columns])
            raise e

        return temp_paths_with_columns

    def _load_contents_to_bigquery(self,
                                   path: GcsfsFilePath,
                                   temp_paths_with_columns: List[Tuple[GcsfsFilePath, List[str]]]):
        """Loads the contents in the given handle to the appropriate table in BigQuery."""

        logging.info('Starting chunked load of contents to BigQuery')
        temp_output_paths = [path for path, _ in temp_paths_with_columns]
        temp_path_to_load_job: Dict[GcsfsFilePath, bigquery.LoadJob] = {}
        dataset_id = self.raw_tables_dataset_for_region(self.region.region_code)

        try:
            for i, (temp_output_path, columns) in enumerate(temp_paths_with_columns):
                parts = filename_parts_from_path(path)
                load_job = self.big_query_client.insert_into_table_from_cloud_storage_async(
                    source_uri=temp_output_path.uri(),
                    destination_dataset_ref=self.big_query_client.dataset_ref_for_id(dataset_id),
                    destination_table_id=parts.file_tag,
                    destination_table_schema=self._create_raw_table_schema_from_columns(columns),
                )
                logging.info('Load job [%s] for chunk [%d] started', load_job.job_id, i)

                temp_path_to_load_job[temp_output_path] = load_job
        except Exception as e:
            logging.error('Failed to start load jobs - cleaning up temp paths')
            self._delete_temp_output_paths(temp_output_paths)
            raise e

        try:
            self._wait_for_jobs(temp_path_to_load_job)
        finally:
            self._delete_temp_output_paths(temp_output_paths)

    @staticmethod
    def _wait_for_jobs(temp_path_to_load_job: Dict[GcsfsFilePath, bigquery.LoadJob]) -> None:
        for temp_output_path, load_job in temp_path_to_load_job.items():
            try:
                load_job.result()
                logging.info('BigQuery load of [%s] complete', temp_output_path.abs_path())
            except BadRequest as e:
                logging.error('Insert job [%s] for path [%s] failed with errors: [%s]',
                              load_job.job_id, temp_output_path, load_job.errors)
                raise e

    def _delete_temp_output_paths(self, temp_output_paths: List[GcsfsFilePath]) -> None:
        for temp_output_path in temp_output_paths:
            logging.info('Deleting temp file [%s].', temp_output_path.abs_path())
            self.fs.delete(temp_output_path)

    def _get_temp_df_output_path(self, path: GcsfsFilePath, chunk_num: int) -> GcsfsFilePath:
        name, _extension = os.path.splitext(path.file_name)

        return GcsfsFilePath.from_directory_and_file_name(self.temp_output_directory_path,
                                                          f'temp_{name}_{chunk_num}.csv')

    def _read_contents_into_dataframes(self,
                                       path: GcsfsFilePath,
                                       contents_handle: GcsfsFileContentsHandle) -> Iterator[pd.DataFrame]:
        parts = filename_parts_from_path(path)
        file_config = self.region_raw_file_config.raw_file_configs[parts.file_tag]

        columns = self._get_validated_columns(file_config, contents_handle)
        try:
            for df in pd.read_csv(
                    contents_handle.local_file_path,
                    sep=file_config.separator,
                    dtype=str,
                    index_col=False,
                    header=None,
                    skiprows=1,
                    encoding=file_config.encoding,
                    quoting=(csv.QUOTE_NONE if file_config.ignore_quotes else csv.QUOTE_MINIMAL),
                    usecols=columns,
                    names=columns,
                    chunksize=self.upload_chunk_size,
                    keep_default_na=False):
                yield df
        except Exception as e:
            logging.error('Failed to parse DataFrame for path [%s] with config [%s]', path.abs_path(), file_config)
            raise e

    @staticmethod
    def _augment_raw_data_with_metadata_columns(path: GcsfsFilePath,
                                                file_metadata: GcsfsDirectIngestFileMetadata,
                                                raw_data_df: pd.DataFrame) -> pd.DataFrame:
        """Add file_id and update_datetime columns to all rows in the dataframe."""

        parts = filename_parts_from_path(path)

        logging.info('Adding extra columns with file_id [%s] and update_datetime [%s]',
                     file_metadata.file_id, parts.utc_upload_datetime)
        raw_data_df[_FILE_ID_COL_NAME] = file_metadata.file_id
        raw_data_df[_UPDATE_DATETIME_COL_NAME] = parts.utc_upload_datetime

        return raw_data_df

    @staticmethod
    def remove_column_non_printable_characters(columns: List[str]) -> List[str]:
        """Removes all non-printable characters that occasionally show up in column names. This is known to happen in
        random columns """
        fixed_columns = []
        for col in columns:
            fixed_col = ''.join([x for x in col if x in string.printable])
            if fixed_col != col:
                logging.info('Found non-printable characters in column [%s]. Original: [%s]',
                             fixed_col, col.__repr__())
            fixed_columns.append(fixed_col)
        return fixed_columns

    def _get_validated_columns(self,
                               file_config: DirectIngestRawFileConfig,
                               contents_handle: GcsfsFileContentsHandle) -> List[str]:
        columns = pd.read_csv(contents_handle.local_file_path,
                              nrows=1,
                              sep=file_config.separator).columns

        columns = self.remove_column_non_printable_characters(columns)

        for column_name in columns:
            if not column_name:
                raise ValueError(f'Found empty column name in [{file_config.file_tag}]')

            non_allowable_chars = self._get_non_allowable_bq_column_chars(column_name)
            if non_allowable_chars:
                # TODO(3020): Some regions (US_MO) are known to have unsupported chars in their column names - will need
                #  to implement how we reliably convert these column names.
                raise ValueError(f'Column [{column_name}] for file has non-allowable characters {non_allowable_chars}.')

        return columns

    @staticmethod
    def _get_non_allowable_bq_column_chars(column_name: str) -> Set[str]:
        def is_bq_allowable_column_char(x: str) -> bool:
            return x in string.ascii_letters or x in string.digits or x == '_'

        return {x for x in column_name if not is_bq_allowable_column_char(x)}

    @staticmethod
    def _create_raw_table_schema_from_columns(columns: List[str]) -> List[bigquery.SchemaField]:
        """Creates schema for use in `to_gbq` based on the provided columns."""
        schema = []
        normalized_columns = [c.strip() for c in columns]
        for name in normalized_columns:
            typ_str = bigquery.enums.SqlTypeNames.STRING.value
            mode = 'NULLABLE'
            if name == _FILE_ID_COL_NAME:
                mode = 'REQUIRED'
                typ_str = bigquery.enums.SqlTypeNames.INTEGER.value
            if name == _UPDATE_DATETIME_COL_NAME:
                mode = 'REQUIRED'
                typ_str = bigquery.enums.SqlTypeNames.DATETIME.value
            schema.append(bigquery.SchemaField(name=name, field_type=typ_str, mode=mode))
        return schema
