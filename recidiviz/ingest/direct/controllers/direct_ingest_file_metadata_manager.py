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
"""A class that handles writing metadata about each direct ingest file to disk."""
import abc
import datetime
import logging
from typing import Optional, Union, Dict, Any

import attr
from more_itertools import one

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DIRECT_INGEST_UNPROCESSED_PREFIX
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType, \
    filename_parts_from_path
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath


# TODO(3020): Once metadata tables are in postgres, these will be moved to a SQLAlchemy file
@attr.s
class GcsfsDirectIngestFileMetadata:
    region_code: str = attr.ib()
    file_tag: str = attr.ib()
    file_id: int = attr.ib()
    normalized_file_name: str = attr.ib()
    processed_time: Optional[datetime.datetime] = attr.ib()


@attr.s
class RawFileMetadataRow(GcsfsDirectIngestFileMetadata):
    import_time: Optional[datetime.datetime] = attr.ib()
    datetimes_contained_lower_bound_inclusive: Optional[datetime.datetime] = attr.ib()
    datetimes_contained_upper_bound_inclusive: Optional[datetime.datetime] = attr.ib()


@attr.s
class IngestFileMetadataRow(GcsfsDirectIngestFileMetadata):
    export_time: Optional[datetime.datetime] = attr.ib()
    is_invalidated: Optional[bool] = attr.ib()
    datetimes_contained_lower_bound_exclusive: Optional[datetime.datetime] = attr.ib()
    datetimes_contained_upper_bound_inclusive: Optional[datetime.datetime] = attr.ib()


class DirectIngestFileMetadataManager:
    """An abstract interface for a class that handles writing metadata about each direct ingest file to disk."""

    @abc.abstractmethod
    def register_new_file(self, path: GcsfsFilePath) -> None:
        """Writes a new row to the appropriate metadata table for a new, unprocessed file."""

    @abc.abstractmethod
    def get_file_metadata(self,
                          path: GcsfsFilePath) -> GcsfsDirectIngestFileMetadata:
        """Returns metadata information for the provided path. If the file has not yet been registered in the
        appropriate metadata table, this function will generate a file_id to return with the metadata.
        """

    @abc.abstractmethod
    def mark_file_as_processed(self,
                               path: GcsfsFilePath,
                               # TODO(3020): Once we write rows to postgres immediately when we encounter a new file, we
                               #  shouldn't need this arg - just query for the appropriate row to get the file id.
                               metadata: GcsfsDirectIngestFileMetadata,
                               processed_time: datetime.datetime) -> None:
        """Marks the file represented by the |metadata| as processed in the appropriate metadata table."""


class BigQueryDirectIngestFileMetadataManager(DirectIngestFileMetadataManager):
    """An an implementation for a class that handles writing metadata about each direct ingest file to BigQuery."""

    DIRECT_INGEST_METADATA_DATASET = 'direct_ingest_processing_metadata'

    FILE_ID_COL_NAME = 'file_id'
    PROCESSED_TIME_COL_NAME = 'processed_time'

    REGION_CODE_COL_NAME = 'region_code'

    def __init__(self, *, region_code: str, big_query_client: BigQueryClient):
        self.region_code = region_code.upper()

        # TODO(3020): Once metadata tables are in postgres, this class will not need a BigQueryClient
        self.big_query_client = big_query_client

    def register_new_file(self, path: GcsfsFilePath) -> None:
        # TODO(3020): Once metadata tables are in postgres (and we don't have any limits on UPDATE queries), insert
        #  a row here that will have the processed_time filled in later.

        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError('Expect only unprocessed paths in this function.')

        logging.info('Would write new file metadata row for path [%s]', path.abs_path())

    def get_file_metadata(self, path: GcsfsFilePath) -> GcsfsDirectIngestFileMetadata:
        logging.info('Getting file metadata for path [%s]', path.abs_path())

        row = self._get_row_for_path(path)

        if not row:
            return self._generate_metadata_row_for_path(path, processed_time=None)

        # TODO(3020): Once metadata tables are in postgres (and we don't have any limits on UPDATE queries), we actually
        #  will always expect to find a row at this point.
        raise ValueError(f'Not expecting for there to be rows written at this point for path [{path.abs_path()}]')

    def mark_file_as_processed(self,
                               path: GcsfsFilePath,
                               metadata: GcsfsDirectIngestFileMetadata,
                               processed_time: datetime.datetime) -> None:
        if self._get_row_for_path(path):
            raise ValueError('We do not yet support writing then updating metadata rows. We do not expect to have a'
                             'row for this path already written here.')

        # TODO(3020): Once metadata tables are in postgres (and we don't have any limits on UPDATE queries), this should
        #  merely update an existing row.
        metadata.processed_time = processed_time
        self._add_row_to_metadata_table(metadata=metadata)

    def _generate_metadata_row_for_path(
            self,
            path: GcsfsFilePath,
            processed_time: Optional[datetime.datetime]) -> Union[RawFileMetadataRow, IngestFileMetadataRow]:
        parts = filename_parts_from_path(path)
        if parts.file_type == GcsfsDirectIngestFileType.RAW_DATA:
            return RawFileMetadataRow(
                region_code=self.region_code,
                file_id=self._get_next_available_file_id(parts.file_type),
                file_tag=parts.file_tag,
                normalized_file_name=path.file_name,
                import_time=parts.utc_upload_datetime,
                processed_time=processed_time,
                # TODO(3020): Fill in lower_bound_inclusive once we're not just receiving historical refreshes.
                datetimes_contained_lower_bound_inclusive=None,
                datetimes_contained_upper_bound_inclusive=parts.utc_upload_datetime)

        if parts.file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
            # TODO(3020): Implement metadata row generation for ingest files
            raise ValueError('Unimplemented!')

        raise ValueError(f'Unexpected file type [{parts.file_type}] for file [{path.abs_path()}]')

    def _add_row_to_metadata_table(self, *, metadata: GcsfsDirectIngestFileMetadata) -> None:
        if not metadata.normalized_file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError('We should only be adding rows for unprocessed paths.')

        row_dict = metadata.__dict__

        if isinstance(metadata, RawFileMetadataRow):
            file_type = GcsfsDirectIngestFileType.RAW_DATA
        elif isinstance(metadata, IngestFileMetadataRow):
            file_type = GcsfsDirectIngestFileType.INGEST_VIEW
        else:
            raise ValueError(f'Unexpected row type {type(metadata)}')

        table_name = self._get_metadata_table_name_for_file_type(file_type=file_type)
        self.big_query_client.insert_rows_into_table(dataset_id=self.DIRECT_INGEST_METADATA_DATASET,
                                                     table_id=table_name,
                                                     rows=[row_dict])

    # TODO(3020): Once metadata tables are in postgres (and we don't have any limits on UPDATE queries), return a
    #  non-optional row here.
    def _get_row_for_path(self, path: GcsfsFilePath) -> Optional[Dict[str, Any]]:
        """Returns the metadata row as a key-value dict for the given path."""
        parts = filename_parts_from_path(path)
        table_name = self._get_metadata_table_name_for_file_type(file_type=parts.file_type)
        table_full_name = f'{self.big_query_client.project_id}.{self.DIRECT_INGEST_METADATA_DATASET}.{table_name}'

        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError('Expect only unprocessed paths in this function.')

        query = \
            f"""SELECT {self.FILE_ID_COL_NAME}, {self.PROCESSED_TIME_COL_NAME} FROM `{table_full_name}`
            WHERE {self.REGION_CODE_COL_NAME} = '{self.region_code}' AND normalized_file_name = '{path.file_name}'"""

        query_job = self.big_query_client.run_query_async(query_str=query)
        rows = query_job.result()

        if rows.total_rows > 1:
            raise ValueError(
                f'Expected there to only be one row per combination of normalized file name [{path.file_name}] and '
                f'[{self.region_code}].')

        if not rows.total_rows:
            # TODO(3020): Once metadata tables are in postgres (and we don't have any limits on UPDATE queries), throw
            #   here since we don't expect to ever query for a metadata row that hasn't already been written.
            logging.info('No found row for normalized file name [%s] and region [%s] in [%s].',
                         path.file_name, self.region_code, table_full_name)
            return None

        row = one(rows)
        logging.info('Found row for [%s] and [%s] with values file_id: [%s] and processed_time: [%s]',
                     path.file_name, self.region_code, row[self.FILE_ID_COL_NAME], row[self.PROCESSED_TIME_COL_NAME])
        return row

    # TODO(3020): Once this data lives in Postgres, we can just use an auto-increment field to generate file ids.
    def _get_next_available_file_id(self, file_type: GcsfsDirectIngestFileType) -> int:
        """Retrieves the next available file_id in the metadata table for the given file type."""
        table_name = self._get_metadata_table_name_for_file_type(file_type=file_type)
        query = f"""SELECT MAX(file_id) AS max_file_id
                    FROM `{self.big_query_client.project_id}.direct_ingest_processing_metadata.{table_name}`"""
        query_job = self.big_query_client.run_query_async(query)
        rows = query_job.result()
        max_file_id = one(rows).get('max_file_id')
        if max_file_id is None:
            return 1
        return max_file_id + 1

    @staticmethod
    def _get_metadata_table_name_for_file_type(file_type: GcsfsDirectIngestFileType) -> str:
        if file_type == GcsfsDirectIngestFileType.RAW_DATA:
            return 'raw_file_metadata'
        if file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
            return 'ingest_file_metadata'
        raise ValueError(f'Unexpected file type {file_type}')
