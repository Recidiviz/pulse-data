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
import time
from typing import List, Dict, Any, Set, Optional, Tuple

import attr
import gcsfs
import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_functions.cloud_function_utils import GCSFS_NO_CACHING
from recidiviz.common import attr_validators
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import filename_parts_from_path, \
    GcsfsDirectIngestFileType
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.gcsfs_csv_reader import GcsfsCsvReader, COMMON_RAW_FILE_ENCODINGS
from recidiviz.ingest.direct.controllers.gcsfs_csv_reader_delegates import ReadOneGcsfsCsvReaderDelegate, \
    SplittingGcsfsCsvReaderDelegate
from recidiviz.persistence.entity.operations.entities import DirectIngestFileMetadata
from recidiviz.utils import metadata
from recidiviz.utils.regions import Region
from recidiviz.utils.yaml_dict import YAMLDict


@attr.s
class RawTableColumnInfo:
    # The column name in BigQuery-compatible, normalized form (e.g. punctuation stripped)
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # True if a column is a date/time
    is_datetime: bool = attr.ib(validator=attr.validators.instance_of(bool))
    # Describes the column contents
    description: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)


@attr.s(frozen=True)
class DirectIngestRawFileConfig:
    """Struct for storing any configuration for raw data imports for a certain file tag."""

    # The file tag / table name that this file will get written to
    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # Description of the raw data file contents
    file_description: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # A list of columns that constitute the primary key for this file. If empty, this table cannot be used in an ingest
    # view query and a '*_latest' view will not be generated for this table. May be left empty for the purposes of
    # allowing us to quickly upload a new file into BQ and then determine the primary keys by querying BQ.
    primary_key_cols: List[str] = attr.ib(validator=attr.validators.instance_of(list))

    # A list of names and descriptions for each column in a file
    columns: List[RawTableColumnInfo] = attr.ib()

    # An additional string clause that will be added to the ORDER BY list that determines which is the most up-to-date
    # row to pick among all rows that have the same primary key.
    # NOTE: Right now this clause does not have access to the date-normalized version of the columns in datetime_cols,
    #  so must handle its own date parsing logic - if this becomes too cumbersome, we can restructure the query to do
    #  date normalization in a subquery before ordering.
    supplemental_order_by_clause: str = attr.ib()

    # Most likely string encoding for this file (e.g. UTF-8)
    encoding: str = attr.ib()

    # The separator character used to denote columns (e.g. ',' or '|').
    separator: str = attr.ib()

    # If true, quoted strings are ignored and separators inside of quotes are treated as column separators. This should
    # be used on any file that has free text fields where the quotes are not escaped and the separator is not common to
    # free text. For example, to handle this row from a pipe separated file that has an open quotation with no close
    # quote:
    #     123|456789|2|He said, "I will be there.|ASDF
    ignore_quotes: bool = attr.ib()

    # TODO(#4243): Add alerts for order in magnitude changes in exported files.
    # If true, means that we **always** will get a historical version of this raw data file from the state and will
    # never change to incremental uploads (for example, because we need to detect row deletions).
    always_historical_export: bool = attr.ib()

    # A comma-separated string representation of the primary keys
    primary_key_str: str = attr.ib()

    @primary_key_str.default
    def _primary_key_str(self) -> str:
        return ", ".join(self.primary_key_cols)

    def encodings_to_try(self) -> List[str]:
        """Returns an ordered list of encodings we should try for this file."""
        return [self.encoding] + [encoding for encoding in COMMON_RAW_FILE_ENCODINGS
                                  if encoding.upper() != self.encoding.upper()]
    @property
    def datetime_cols(self) -> List[str]:
        return [column.name for column in self.columns if column.is_datetime]


    @classmethod
    def from_yaml_dict(cls,
                       region_code: str,
                       file_tag: str,
                       default_encoding: str,
                       default_separator: str,
                       file_config_dict: YAMLDict) -> 'DirectIngestRawFileConfig':
        """Returns a DirectIngestRawFileConfig built from a YAMLDict"""
        primary_key_cols = file_config_dict.pop('primary_key_cols', list)
        # TODO(#5399): Migrate raw file configs for all legacy regions to have file descriptions
        if region_code.upper() in {'US_ND', 'US_ID', 'US_PA'}:
            file_description = file_config_dict.pop_optional('file_description', str)\
                               or 'LEGACY_FILE_MISSING_DESCRIPTION'
        else:
            file_description = file_config_dict.pop('file_description', str)
        # TODO(#5399): Migrate raw file configs for all legacy regions to have column descriptions
        if region_code.upper() in {'US_MO', 'US_ND', 'US_ID', 'US_PA'}:
            columns = file_config_dict.pop_optional('columns', list) or []
        else:
            columns = file_config_dict.pop('columns', list)

        column_names = [column['name'] for column in columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError(f'Found duplicate columns in raw_file [{file_tag}]')

        supplemental_order_by_clause = file_config_dict.pop_optional('supplemental_order_by_clause', str)
        encoding = file_config_dict.pop_optional('encoding', str)
        separator = file_config_dict.pop_optional('separator', str)
        ignore_quotes = file_config_dict.pop_optional('ignore_quotes', bool)
        always_historical_export = file_config_dict.pop_optional('always_historical_export', bool)

        if len(file_config_dict) > 0:
            raise ValueError(f'Found unexpected config values for raw file'
                             f'[{file_tag}]: {repr(file_config_dict.get())}')

        return DirectIngestRawFileConfig(
            file_tag=file_tag,
            file_description=file_description,
            primary_key_cols=primary_key_cols,
            columns=[
                RawTableColumnInfo(name=column['name'],
                                   is_datetime=column.get('is_datetime', False),
                                   description=column.get('description', None)) for column in columns],
            supplemental_order_by_clause=supplemental_order_by_clause if supplemental_order_by_clause else '',
            encoding=encoding if encoding else default_encoding,
            separator=separator if separator else default_separator,
            ignore_quotes=ignore_quotes if ignore_quotes else False,
            always_historical_export=always_historical_export if always_historical_export else False
        )


@attr.s
class DirectIngestRegionRawFileConfig:
    """Class that parses and stores raw data import configs for a region"""

    # TODO(#5262): Add documentation for the structure of the raw data yaml files
    region_code: str = attr.ib()
    yaml_config_file_dir: str = attr.ib()
    raw_file_configs: Dict[str, DirectIngestRawFileConfig] = attr.ib()

    def _region_ingest_dir(self) -> str:
        return os.path.join(os.path.dirname(__file__),
                            '..',
                            'regions',
                            f'{self.region_code.lower()}')

    @yaml_config_file_dir.default
    def _config_file_dir(self) -> str:
        return os.path.join(self._region_ingest_dir(), 'raw_data')

    @raw_file_configs.default
    def _raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        return self._get_raw_data_file_configs()

    def _get_raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        """Returns list of file tags we expect to see on raw files for this region."""
        if os.path.isdir(self.yaml_config_file_dir):
            default_filename = f'{self.region_code}_default.yaml'
            default_file_path = os.path.join(self.yaml_config_file_dir, default_filename)
            if not os.path.exists(default_file_path):
                raise ValueError(f'Missing default raw data configs for region: {self.region_code}')

            default_contents = YAMLDict.from_path(default_file_path)
            default_encoding = default_contents.pop('default_encoding', str)
            default_separator = default_contents.pop('default_separator', str)

            raw_data_configs = {}
            for filename in os.listdir(self.yaml_config_file_dir):
                if filename == default_filename:
                    continue
                yaml_file_path = os.path.join(self.yaml_config_file_dir, filename)
                yaml_contents = YAMLDict.from_path(yaml_file_path)

                file_tag = yaml_contents.pop('file_tag', str)
                if not file_tag:
                    raise ValueError(f'Missing file_tag in [{yaml_file_path}]')
                if filename != f'{self.region_code.lower()}_{file_tag}.yaml':
                    raise ValueError(f'Mismatched file_tag [{file_tag}] and filename [{filename}]'
                                     f' in [{yaml_file_path}]')
                if file_tag in raw_data_configs:
                    raise ValueError(f'Found file tag [{file_tag}] in [{yaml_file_path}]'
                                     f' that is already defined in another yaml file.')

                raw_data_configs[file_tag] = DirectIngestRawFileConfig.from_yaml_dict(self.region_code,
                                                                                      file_tag,
                                                                                      default_encoding,
                                                                                      default_separator,
                                                                                      yaml_contents)
        else:
            raise ValueError(f'Missing raw data configs for region: {self.region_code}')
        return raw_data_configs

    raw_file_tags: Set[str] = attr.ib()

    @raw_file_tags.default
    def _raw_file_tags(self) -> Set[str]:
        return set(self.raw_file_configs.keys())


_FILE_ID_COL_NAME = 'file_id'
_UPDATE_DATETIME_COL_NAME = 'update_datetime'
_DEFAULT_BQ_UPLOAD_CHUNK_SIZE = 250000

# The number of seconds of spacing we need to have between each table load operation to avoid going over the
# "5 operations every 10 seconds per table" rate limit (with a little buffer): https://cloud.google.com/bigquery/quotas
_PER_TABLE_UPDATE_RATE_LIMITING_SEC = 2.5


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
        self.csv_reader = GcsfsCsvReader(gcsfs.GCSFileSystem(project=metadata.project_id(),
                                                             cache_timeout=GCSFS_NO_CACHING))

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
    def raw_tables_dataset_for_region(cls, region_code: str) -> str:
        return f'{region_code.lower()}_raw_data'

    def import_raw_file_to_big_query(self,
                                     path: GcsfsFilePath,
                                     file_metadata: DirectIngestFileMetadata) -> None:
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

        logging.info('Beginning BigQuery upload of raw file [%s]', path.abs_path())

        temp_output_paths = self._upload_contents_to_temp_gcs_paths(path, file_metadata)
        self._load_contents_to_bigquery(path, temp_output_paths)

        logging.info('Completed BigQuery import of [%s]', path.abs_path())

    def _upload_contents_to_temp_gcs_paths(
            self,
            path: GcsfsFilePath,
            file_metadata: DirectIngestFileMetadata) -> List[Tuple[GcsfsFilePath, List[str]]]:
        """Uploads the contents of the file at the provided path to one or more GCS files, with whitespace stripped and
        additional metadata columns added.
        Returns a list of tuple pairs containing the destination paths and corrected CSV columns for that file.
        """

        logging.info('Starting chunked upload of contents to GCS')

        parts = filename_parts_from_path(path)
        file_config = self.region_raw_file_config.raw_file_configs[parts.file_tag]

        columns = self._get_validated_columns(path, file_config)

        delegate = DirectIngestRawDataSplittingGcsfsCsvReaderDelegate(path,
                                                                      self.fs,
                                                                      file_metadata,
                                                                      self.temp_output_directory_path)

        self.csv_reader.streaming_read(path,
                                       delegate=delegate,
                                       chunk_size=self.upload_chunk_size,
                                       encodings_to_try=file_config.encodings_to_try(),
                                       index_col=False,
                                       header=None,
                                       skiprows=1,
                                       usecols=columns,
                                       names=columns,
                                       keep_default_na=False,
                                       **self._common_read_csv_kwargs(file_config))

        return delegate.output_paths_with_columns

    def _load_contents_to_bigquery(self,
                                   path: GcsfsFilePath,
                                   temp_paths_with_columns: List[Tuple[GcsfsFilePath, List[str]]]) -> None:
        """Loads the contents in the given handle to the appropriate table in BigQuery."""

        logging.info('Starting chunked load of contents to BigQuery')
        temp_output_paths = [path for path, _ in temp_paths_with_columns]
        temp_path_to_load_job: Dict[GcsfsFilePath, bigquery.LoadJob] = {}
        dataset_id = self.raw_tables_dataset_for_region(self.region.region_code)

        try:
            for i, (temp_output_path, columns) in enumerate(temp_paths_with_columns):
                if i > 0:
                    # Note: If this sleep becomes a serious performance issue, we could refactor to intersperse reading
                    # chunks to temp paths with starting each load job. In this case, we'd have to be careful to delete
                    # any partially uploaded uploaded portion of the file if we fail to parse a chunk in the middle.
                    logging.info('Sleeping for [%s] seconds to avoid exceeding per-table update rate quotas.',
                                 _PER_TABLE_UPDATE_RATE_LIMITING_SEC)
                    time.sleep(_PER_TABLE_UPDATE_RATE_LIMITING_SEC)

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
                logging.info('Waiting for load of [%s]', temp_output_path.abs_path())
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
                               path: GcsfsFilePath,
                               file_config: DirectIngestRawFileConfig) -> List[str]:
        """Returns a list of normalized column names for the raw data file at the given path."""
        # TODO(#3020): We should not derive the columns from what we get in the uploaded raw data CSV - we should
        # instead define the set of columns we expect to see in each input CSV (with mandatory documentation) and update
        # this function to make sure that the columns in the CSV is a strict subset of expected columns. This will allow
        # to gracefully any raw data re-imports where a new column gets introduced in a later file.

        delegate = ReadOneGcsfsCsvReaderDelegate()
        self.csv_reader.streaming_read(path, delegate=delegate, chunk_size=1, nrows=1,
                                       **self._common_read_csv_kwargs(file_config))
        df = delegate.df

        if not isinstance(df, pd.DataFrame):
            raise ValueError(f'Unexpected type for DataFrame: [{type(df)}]')

        columns = self.remove_column_non_printable_characters(df.columns)

        # Strip whitespace from head/tail of column names
        columns = [c.strip() for c in columns]

        normalized_columns = set()
        for i, column_name in enumerate(columns):
            if not column_name:
                raise ValueError(f'Found empty column name in [{file_config.file_tag}]')

            column_name = self._convert_non_allowable_bq_column_chars(column_name)

            # BQ doesn't allow column names to begin with a number, so we prepend an underscore in that case
            if column_name[0] in string.digits:
                column_name = '_' + column_name

            if column_name in normalized_columns:
                raise ValueError(f'Multiple columns with name [{column_name}] after normalization.')
            normalized_columns.add(column_name)
            columns[i] = column_name

        return columns

    @staticmethod
    def _convert_non_allowable_bq_column_chars(column_name: str) -> str:
        def is_bq_allowable_column_char(x: str) -> bool:
            return x in string.ascii_letters or x in string.digits or x == '_'
        column_name = "".join([c if is_bq_allowable_column_char(c) else '_' for c in column_name])
        return column_name

    @staticmethod
    def _create_raw_table_schema_from_columns(columns: List[str]) -> List[bigquery.SchemaField]:
        """Creates schema for use in `to_gbq` based on the provided columns."""
        schema = []
        for name in columns:
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

    @staticmethod
    def _common_read_csv_kwargs(file_config: DirectIngestRawFileConfig) -> Dict[str, Any]:
        return {
            'sep': file_config.separator,
            'quoting': (csv.QUOTE_NONE if file_config.ignore_quotes else csv.QUOTE_MINIMAL),
        }


class DirectIngestRawDataSplittingGcsfsCsvReaderDelegate(SplittingGcsfsCsvReaderDelegate):
    """An implementation of the GcsfsCsvReaderDelegate that augments chunks of a raw data file and re-uploads each
    chunk to a temporary Google Cloud Storage path.
    """

    def __init__(self,
                 path: GcsfsFilePath,
                 fs: DirectIngestGCSFileSystem,
                 file_metadata: DirectIngestFileMetadata,
                 temp_output_directory_path: GcsfsDirectoryPath):

        super().__init__(path, fs, include_header=False)
        self.file_metadata = file_metadata
        self.temp_output_directory_path = temp_output_directory_path

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Stripping white space from all fields
        df = df.applymap(lambda x: x.strip())

        augmented_df = self._augment_raw_data_with_metadata_columns(path=self.path,
                                                                    file_metadata=self.file_metadata,
                                                                    raw_data_df=df)
        return augmented_df

    def get_output_path(self, chunk_num: int) -> GcsfsFilePath:
        name, _extension = os.path.splitext(self.path.file_name)

        return GcsfsFilePath.from_directory_and_file_name(self.temp_output_directory_path,
                                                          f'temp_{name}_{chunk_num}.csv')

    @staticmethod
    def _augment_raw_data_with_metadata_columns(path: GcsfsFilePath,
                                                file_metadata: DirectIngestFileMetadata,
                                                raw_data_df: pd.DataFrame) -> pd.DataFrame:
        """Add file_id and update_datetime columns to all rows in the dataframe."""

        parts = filename_parts_from_path(path)

        logging.info('Adding extra columns with file_id [%s] and update_datetime [%s]',
                     file_metadata.file_id, parts.utc_upload_datetime)
        raw_data_df[_FILE_ID_COL_NAME] = file_metadata.file_id
        raw_data_df[_UPDATE_DATETIME_COL_NAME] = parts.utc_upload_datetime

        return raw_data_df
