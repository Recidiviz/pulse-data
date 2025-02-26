# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Direct ingest controller for regions that read CSV files from the
GCSFileSystem.
"""

import abc
import inspect
import os
from typing import List, Optional, Callable

import gcsfs
import pandas as pd

from recidiviz import IngestInfo
from recidiviz.cloud_functions.cloud_function_utils import GCSFS_NO_CACHING
from recidiviz.common.ingest_metadata import SystemLevel

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs, filename_parts_from_path, GcsfsDirectIngestFileType
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.ingest.direct.controllers.gcsfs_csv_reader_delegates import ReadOneGcsfsCsvReaderDelegate, \
    SplittingGcsfsCsvReaderDelegate
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.ingest.extractor.csv_data_extractor import CsvDataExtractor
from recidiviz.utils import metadata


class DirectIngestFileSplittingGcsfsCsvReaderDelegate(SplittingGcsfsCsvReaderDelegate):
    def __init__(self, path: GcsfsFilePath, fs: DirectIngestGCSFileSystem, output_directory_path: GcsfsDirectoryPath):
        super().__init__(path, fs, include_header=True)
        self.output_directory_path = output_directory_path

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def get_output_path(self, chunk_num: int):
        name, _extension = os.path.splitext(self.path.file_name)

        return GcsfsFilePath.from_directory_and_file_name(self.output_directory_path,
                                                          f'temp_direct_ingest_{name}_{chunk_num}.csv')


class CsvGcsfsDirectIngestController(GcsfsDirectIngestController):
    """Direct ingest controller for regions that read CSV files from the
    GCSFileSystem.
    """

    def __init__(self,
                 region_name: str,
                 system_level: SystemLevel,
                 ingest_directory_path: Optional[str],
                 storage_directory_path: Optional[str],
                 max_delay_sec_between_files: Optional[int] = None):
        super().__init__(region_name,
                         system_level,
                         ingest_directory_path,
                         storage_directory_path,
                         max_delay_sec_between_files)
        self.csv_reader = GcsfsCsvReader(gcsfs.GCSFileSystem(project=metadata.project_id(),
                                                             cache_timeout=GCSFS_NO_CACHING))

    @classmethod
    @abc.abstractmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        pass

    def _file_meets_file_line_limit(
            self,
            line_limit: int,
            path: GcsfsFilePath) -> bool:
        delegate = ReadOneGcsfsCsvReaderDelegate()

        # Read a chunk up to one line bigger than the acceptable size
        self.csv_reader.streaming_read(path, delegate=delegate, chunk_size=(line_limit + 1))

        if delegate.df is None:
            # If the file is empty, it's fine.
            return True

        # If length of the only chunk is less than or equal to the acceptable
        # size, file meets line limit.
        return len(delegate.df) <= line_limit

    def _split_file(self, path: GcsfsFilePath) -> List[GcsfsFilePath]:
        parts = filename_parts_from_path(path)

        if self.region.is_raw_vs_ingest_file_name_detection_enabled() and \
                parts.file_type == GcsfsDirectIngestFileType.RAW_DATA:
            raise ValueError(f'Splitting raw files unsupported. Attempting to split [{path.abs_path()}]')

        delegate = DirectIngestFileSplittingGcsfsCsvReaderDelegate(path, self.fs, self.temp_output_directory_path)
        self.csv_reader.streaming_read(path, delegate=delegate, chunk_size=self.ingest_file_split_line_limit)
        output_paths = [path for path, _ in delegate.output_paths_with_columns]

        return output_paths

    def _yaml_filepath(self, file_tag):
        return os.path.join(os.path.dirname(inspect.getfile(self.__class__)),
                            f'{self.region.region_code}_{file_tag}.yaml')

    @staticmethod
    def _wrap_with_tag(file_tag: str, callback: Optional[Callable]):
        if callback is None:
            return None

        def wrapped_cb(*args):
            return callback(file_tag, *args)

        return wrapped_cb

    @classmethod
    def _wrap_list_with_tag(cls, file_tag: str, callbacks: List[Callable]):
        return [cls._wrap_with_tag(file_tag, callback)
                for callback in callbacks]

    def _parse(self,
               args: GcsfsIngestArgs,
               contents_handle: GcsfsFileContentsHandle) -> IngestInfo:
        file_tag = self.file_tag(args.file_path)

        if file_tag not in self.get_file_tag_rank_list():
            raise DirectIngestError(
                msg=f"No mapping found for tag [{file_tag}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        file_mapping = self._yaml_filepath(file_tag)

        row_pre_processors = self._wrap_list_with_tag(
            file_tag, self._get_row_pre_processors_for_file(file_tag))
        row_post_processors = self._wrap_list_with_tag(
            file_tag, self._get_row_post_processors_for_file(file_tag))
        file_post_processors = self._wrap_list_with_tag(
            file_tag, self._get_file_post_processors_for_file(file_tag))
        # pylint: disable=assignment-from-none
        primary_key_override_callback = self._wrap_with_tag(
            file_tag, self._get_primary_key_override_for_file(file_tag))
        # pylint: disable=assignment-from-none
        ancestor_chain_overrides_callback = \
            self._wrap_with_tag(
                file_tag,
                self._get_ancestor_chain_overrides_callback_for_file(file_tag))
        should_set_with_empty_values = \
            file_tag in self._get_files_to_set_with_empty_values()

        data_extractor = CsvDataExtractor(
            file_mapping,
            row_pre_processors,
            row_post_processors,
            file_post_processors,
            ancestor_chain_overrides_callback,
            primary_key_override_callback,
            self.system_level,
            should_set_with_empty_values)

        return data_extractor.extract_and_populate_data(
            contents_handle.get_contents_iterator())

    def _are_contents_empty(self,
                            args: GcsfsIngestArgs,
                            contents_handle: GcsfsFileContentsHandle) -> bool:
        """Returns true if the CSV file is emtpy, i.e. it contains no non-header
         rows.
         """
        delegate = ReadOneGcsfsCsvReaderDelegate()
        self.csv_reader.streaming_read(args.file_path, delegate=delegate, chunk_size=1, skiprows=1)
        return delegate.df is None

    def _get_row_pre_processors_for_file(self, _file_tag) -> List[Callable]:
        """Subclasses should override to return row_pre_processors for a given
        file tag.
        """
        return []

    def _get_row_post_processors_for_file(self, _file_tag) -> List[Callable]:
        """Subclasses should override to return row_post_processors for a given
        file tag.
        """
        return []

    def _get_file_post_processors_for_file(self, _file_tag) -> List[Callable]:
        """Subclasses should override to return file_post_processors for a given
        file tag.
        """
        return []

    def _get_ancestor_chain_overrides_callback_for_file(
            self, _file_tag: str) -> Optional[Callable]:
        """Subclasses should override to return an
        ancestor_chain_overrides_callback for a given file tag.
        """
        return None

    def _get_primary_key_override_for_file(
            self, _file_tag: str) -> Optional[Callable]:
        """Subclasses should override to return a primary_key_override for a
        given file tag.
        """
        return None

    def _get_files_to_set_with_empty_values(self) -> List[str]:
        """Subclasses should override to return which files to set with empty
        values (see CsvDataExtractor).
        """
        return []
