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
"""
Uploads raw_data file(s) received from regions into BQ. In doing this, it updates the required metadata table and
append only tables in BQ. This script should only be used before our ingest flow supports SQL preprocessing (#3020).

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.upload_raw_data_files_to_bq \
    --paths ~/local/ \
    --region-code US_ID \
    --import-time 2020-04-27 \
    --project-id recidiviz-staging \
    --separator '|' \
    --dry-run True
"""
import argparse
import csv
import datetime
import logging
import os
import string
from typing import List, Dict

import pandas as pd
from google.cloud import bigquery

from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import to_normalized_unprocessed_file_name
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType
from recidiviz.tools.utils import to_datetime, add_row_to_raw_metadata, get_file_id_and_processed_status_for_file, \
    mark_existing_metadata_row_as_processed, MetadataType, get_next_available_file_id
from recidiviz.utils.params import str_to_bool

_FILE_ID_COL = 'file_id'
_UPDATE_DATETIME_COL = 'update_datetime'


class UploadRawDataFilesToBqController:
    """Class with functionality to upload raw data files to BQ and update relevant metadata tables."""

    def __init__(
            self,
            dry_run: bool,
            project_id: str,
            region_code: str,
            paths: List[str],
            import_time: datetime.datetime,
            separator: str,
            chunk_size: int,
            ignore_quotes: bool,
            encoding: str):
        self.dry_run = dry_run
        self.project_id = project_id
        self.region_code = region_code
        self.paths = paths
        self.import_time = import_time
        self.separator = separator
        self.chunk_size = chunk_size
        self.quoting = csv.QUOTE_NONE if ignore_quotes else csv.QUOTE_MINIMAL
        self.encoding = encoding
        self.client = bigquery.Client()

    def _mark_file_as_processed(
            self,
            file_id: int,
            file_tag: str,
            normalized_file_name: str,
            processed_time: datetime.datetime,
            file_exists_in_metadata: bool) -> None:
        """Marks the provided |normalized_file_name| as processed in the raw_file_metadata table."""
        # TODO(#3020): Fill in lower_bound_inclusive once we're not just receiving historical refreshes.
        if not file_exists_in_metadata:
            add_row_to_raw_metadata(
                client=self.client,
                file_id=file_id,
                file_tag=file_tag,
                region_code=self.region_code,
                import_time=self.import_time,
                project_id=self.project_id,
                dry_run=self.dry_run,
                normalized_file_name=normalized_file_name,
                processed_time=processed_time,
                datetimes_contained_upper_bound_inclusive=self.import_time)
        else:
            mark_existing_metadata_row_as_processed(
                metadata_type=MetadataType.RAW,
                project_id=self.project_id,
                dry_run=self.dry_run,
                client=self.client,
                file_id=file_id,
                processed_time=processed_time)

    def _append_df_to_table(self, dataset: str, table_name: str, df: pd.DataFrame) -> None:
        """Uploads the provided |df| into the relevant append-only BQ table. If the table does not already exist in BQ,
        it will be created.
        """
        if self.dry_run:
            logging.info('[DRY RUN] Would have uploaded dataframe to %s.%s.%s', self.project_id, dataset, table_name)
            return
        logging.info('Uploading dataframe to to %s.%s.%s', self.project_id, dataset, table_name)
        schema = self._create_schema_from_columns(df.columns)

        # to_gbq is significantly faster than the BQ client API `insert_rows_from_dataframe` which relies on the
        # streaming service rather than batch loading.
        # TODO(#3020): Instead of uploading in batches via `to_gbq`, try writing the CSV to GCS and then using a load
        #  operation. While it does force us to create a temp file, we expect that to have a performance increase.
        df.to_gbq(
            destination_table=f'{dataset}.{table_name}',
            project_id=self.project_id,
            progress_bar=True,
            if_exists='append',
            table_schema=schema,
            chunksize=self.chunk_size)

    def _create_schema_from_columns(self, columns: List[str]) -> List[Dict[str, str]]:
        """Creates schema for use in `to_gbq` based on the provided columns."""
        schema = []
        normalized_columns = [c.strip() for c in columns]
        for name in normalized_columns:
            typ_str = 'STRING'
            mode = 'NULLABLE'
            if name == _FILE_ID_COL:
                mode = 'REQUIRED'
                typ_str = 'INTEGER'
            if name == _UPDATE_DATETIME_COL:
                mode = 'REQUIRED'
                typ_str = 'DATETIME'
            schema.append({'name': name, 'type': typ_str, 'mode': mode})
        return schema

    def _get_dataframe_from_csv_with_extra_cols(self, file_id: int, local_file_path: str) -> pd.DataFrame:
        """Parses the provided |local_file_path| into a dataframe. Adds file_id and update_time columns to the dataframe
        before returning.
        """
        columns = pd.read_csv(local_file_path, nrows=1, sep=self.separator).columns
        # Remove non printable characters that occasionally show up in column names. This has been known to happen in
        # random column names provided by US_ID.
        columns = [remove_non_allowable_bq_column_chars(c) for c in columns]
        df = pd.read_csv(
            local_file_path,
            sep=self.separator,
            dtype=str,
            index_col=False,
            header=None,
            skiprows=1,
            encoding=self.encoding,
            quoting=self.quoting,
            usecols=list(range(len(columns))),
            names=columns,
            keep_default_na=False)
        # add FILE_ID_COL and _UPDATE_DATETIME_COL to all rows in dataframe.
        df[_FILE_ID_COL] = file_id
        df[_UPDATE_DATETIME_COL] = self.import_time

        return df

    def _upload_raw_data_file_to_bq(self, local_file_path: str) -> None:
        """Attempts to upload the given |local_file_path| to all relevant tables in BQ."""
        logging.info('\n\n\n ============== Beginning processing for file %s ================ \n\n\n', local_file_path)
        _, file_name = os.path.split(local_file_path)
        file_tag, _ = os.path.splitext(file_name)

        normalized_file_name = to_normalized_unprocessed_file_name(
            file_name=file_name, file_type=GcsfsDirectIngestFileType.RAW_DATA, dt=self.import_time)
        file_id_processed_tuple = get_file_id_and_processed_status_for_file(
            metadata_type=MetadataType.RAW,
            region_code=self.region_code,
            client=self.client,
            project_id=self.project_id,
            normalized_file_name=normalized_file_name)

        file_in_metadata = file_id_processed_tuple[0] is not None
        file_id = file_id_processed_tuple[0] if file_id_processed_tuple[0] is not None \
            else get_next_available_file_id(
                metadata_type=MetadataType.RAW, client=self.client, project_id=self.project_id)
        file_already_processed = file_id_processed_tuple[1]

        if file_already_processed:
            logging.warning('File %s is already marked as processed. Skipping file processing.', normalized_file_name)
            return

        df = self._get_dataframe_from_csv_with_extra_cols(local_file_path=local_file_path, file_id=file_id)

        raw_data_dataset = f'{self.region_code.lower()}_raw_data'
        logging.info('\n\nLoaded dataframe has %d rows\n', df.shape[0])
        logging.info('\n\nLoaded dataframe with intent of uploading to %s.%s: \n\n%s',
                     raw_data_dataset, file_tag, str(df.head()))

        if not self.dry_run:
            i = input('Continue? [y/n]: ')
            if i.upper() != 'Y':
                return

        self._append_df_to_table(dataset=raw_data_dataset, table_name=file_tag, df=df)
        self._mark_file_as_processed(
            file_id=file_id,
            file_tag=file_tag,
            normalized_file_name=normalized_file_name,
            processed_time=datetime.datetime.now(),
            file_exists_in_metadata=file_in_metadata)

    def do_upload(self) -> None:
        """Loops through all provided raw data files, attempting to upload their contents to BQ."""
        local_file_paths = _get_all_file_paths(self.paths)

        succeeded_files = []
        failed_files = []
        for local_file_path in local_file_paths:
            try:
                self._upload_raw_data_file_to_bq(
                    local_file_path=local_file_path)
                succeeded_files.append(local_file_path)
            except Exception:
                logging.exception('Failed to parse file [%s]', local_file_path)
                failed_files.append(local_file_path)

        logging.info('Succeeded in uploading files [%s]', succeeded_files)
        logging.info('Failed in uploading files [%s]', failed_files)


def remove_non_allowable_bq_column_chars(str_to_normalize: str) -> str:
    """Removes all characters from |str_to_normalize| that are forbidden for column names in BQ."""
    return ''.join([x for x in str_to_normalize if x in string.ascii_letters or x in string.digits or x == '_'])


def _get_all_file_paths(paths: List[str]) -> List[str]:
    """Expands the provided list of |paths| (with both file and directory paths) so that all directories in the
    original list are expanded to enumerate file paths within that directory.
    """
    file_paths = []
    for path in paths:
        if os.path.isdir(path):
            for filename in os.listdir(path):
                if filename.startswith('.'):    # Skip hidden files
                    continue
                file_paths.append(os.path.join(path, filename))
        elif os.path.isfile(path):
            file_paths.append(path)
    return file_paths


if __name__ == '__main__':
    # Set maximum display options
    pd.options.display.max_columns = 999
    pd.options.display.max_rows = 999

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs copy in dry-run mode, only prints the file copies it would do.')
    parser.add_argument('--paths', metavar='PATH', nargs='+',
                        help='Path to files to to upload to BQ, either single file path or directory path.')
    parser.add_argument('--region-code', required=True, default='', help='The region code for the uploaded file(s)')
    parser.add_argument('--import-time', required=True, help='The time the file was received by Recidiviz. Expected in '
                                                             'format %Y-%m-%d %H:%M')
    parser.add_argument('--separator', required=False, default=',', help='Separator for the csv. Defaults to \',\'')
    parser.add_argument('--project-id', required=True, help='The project_id for the destination table')
    parser.add_argument('--chunk-size', required=False, default=100000,
                        help='Number of rows to be inserted into BQ at a time. Defaults to 100,000.')
    parser.add_argument('--encoding', required=False, default='UTF-8',
                        help='Encoding for the file to be parsed. Defaults to UTF-8. If you are parsing files from '
                             'US_ID, you might need ISO-8859-1.')
    parser.add_argument('--ignore-quotes', required=False, default=False, type=str_to_bool,
                        help='If false, assumes text between quotes should be treated as a unified string (even if the '
                             'text contains the specified separator). If True, does not treat quotes as a special '
                             'character. Defaults to False.')

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    UploadRawDataFilesToBqController(
        dry_run=args.dry_run,
        project_id=args.project_id,
        region_code=args.region_code,
        import_time=to_datetime(args.import_time),
        paths=args.paths,
        separator=args.separator,
        chunk_size=args.chunk_size,
        encoding=args.encoding,
        ignore_quotes=args.ignore_quotes).do_upload()
