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
Script for downloading all date-bound queries for a given state into INGEST_VIEW CSVs. When generating the CSVs this
only writes query results that appear in the |end_date| query and not in the |begin_date| query. When downloading the
CSVs, this script also updates the required metadata table in BQ.

This script should only be used before our ingest flow fully supports SQL preprocessing.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.download_bq_results_for_state \
    --project-id recidiviz-staging \
    --state-code US_ID \
    --local-dir ~/Desktop \
    --start-date '2020-02-27 00:00' \
    --end-date '2020-04-27 00:00' \
    --dry-run True
"""

import argparse
import datetime
import logging
import os
from typing import List, Tuple, Dict

import pandas as pd
from google.cloud import bigquery, bigquery_storage_v1beta1

from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import to_normalized_unprocessed_file_name
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import UPDATE_DATETIME_PARAM_NAME
from recidiviz.ingest.direct.regions.us_id import us_id_sql_queries
from recidiviz.tools.utils import to_datetime, get_file_id_and_processed_status_for_file, MetadataType, \
    add_row_to_ingest_metadata, get_next_available_file_id
from recidiviz.utils.params import str_to_bool

DATE_BOUND_QUERIES_BY_STATE: Dict[str, List[Tuple[str, str]]] = {
    'us_id': us_id_sql_queries.get_query_name_to_query_list(),
}


# TODO(3020): Update logic to infer dates based on other written metadata tables.

class DownloadBqResultsForStateController:
    """Class responsible for downloading queries from BQ into a local set of files."""
    def __init__(self,
                 *,
                 dry_run: bool,
                 project_id: str,
                 state_code: str,
                 local_dir: str,
                 encoding: str,
                 start_date: datetime.datetime,
                 end_date: datetime.datetime):
        self.dry_run = dry_run
        self.project_id = project_id
        self.state_code = state_code
        self.local_dir = local_dir
        self.encoding = encoding
        self.start_date = start_date
        self.end_date = end_date
        self.metadata_type = MetadataType.INGEST
        self.bqclient = bigquery.Client(project=project_id)
        self.bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient()

    def _filter_seen_rows(self, *, old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """Returns a dataframe containing all rows in |new_df| which are not present in |old_df|."""
        merged = old_df.merge(new_df, how='outer', indicator=True)
        updated = merged[merged['_merge'] == 'right_only']
        updated = updated.drop(['_merge'], axis=1)
        return updated

    def _download_query_for_date(self, *, query: str, query_name: str, query_date: datetime.datetime) -> pd.DataFrame:
        """Downloads the provided |query| using the |query_date| parameter and returns results as a dataframe."""
        if not query:
            logging.info('Unexpected empty query, please fill it in')

        logging.info('Beginning query [%s]', query_name)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(UPDATE_DATETIME_PARAM_NAME, "DATETIME", query_date)],
        )
        logging.info('\n\n Beginning query for date %s', query_date.isoformat())
        df = self.bqclient.query(query, job_config=job_config).result().to_dataframe(
            bqstorage_client=self.bqstorageclient)
        logging.info('\n\nLoaded df for date %s, which has %d rows\n', query_date.isoformat(), df.shape[0])
        return df

    def _write_dataframe_to_csv(self, *, df: pd.DataFrame, file_path: str) -> None:
        """Writes the |df| to a file '|query_name|.csv' in the provided |local_dir|."""
        if self.dry_run:
            logging.info('[DRY RUN] Would have written dataframe with file_path %s: \n\n%s', file_path, str(df.head()))
            return
        i = input(f'Going to write dataframe to file_path {file_path}: \n\n{str(df.head())}\n\n Continue? [y/n]:')
        if i.upper() != 'Y':
            return
        df.to_csv(file_path, encoding=self.encoding, index=False)
        logging.info('Successfully wrote dataframe to filepath %s', file_path)

    def download_query_to_csv(self, query_name, query):
        """Downloads the provided |query| into a CSV file with the name |query_name|."""
        output_file_name = query_name + '.csv'
        local_file_path = os.path.join(self.local_dir, output_file_name)
        normalized_file_name = to_normalized_unprocessed_file_name(
            file_name=output_file_name, file_type=GcsfsDirectIngestFileType.INGEST_VIEW, dt=self.end_date)
        file_id_processed_tuple = get_file_id_and_processed_status_for_file(
            metadata_type=self.metadata_type,
            project_id=self.project_id,
            state_code=self.state_code,
            client=self.bqclient,
            normalized_file_name=normalized_file_name)
        file_in_metadata = file_id_processed_tuple[0] is not None
        if file_in_metadata:
            logging.info("File %s is already found in metadata, skipping download", normalized_file_name)
            return

        file_id = get_next_available_file_id(
            metadata_type=self.metadata_type, client=self.bqclient, project_id=self.project_id)

        before_df = self._download_query_for_date(
            query=query,
            query_name=query_name,
            query_date=datetime.datetime(year=2020, month=2, day=27))
        after_df = self._download_query_for_date(
            query=query,
            query_name=query_name,
            query_date=datetime.datetime(year=2020, month=4, day=27))
        update_df = self._filter_seen_rows(old_df=before_df, new_df=after_df)
        logging.info('\n\nUpdated dataframe has %d rows\n', update_df.shape[0])
        self._write_dataframe_to_csv(df=update_df, file_path=local_file_path)
        add_row_to_ingest_metadata(
            client=self.bqclient,
            project_id=self.project_id,
            state_code=self.state_code,
            export_time=self.end_date,
            dry_run=self.dry_run,
            file_id=file_id,
            file_tag=query_name,
            normalized_file_name=normalized_file_name,
            datetimes_contained_lower_bound_exclusive=self.start_date,
            datetimes_contained_upper_bound_inclusive=self.end_date)

    def download_queries_to_csvs(self) -> None:
        all_queries = DATE_BOUND_QUERIES_BY_STATE.get(self.state_code)
        if not all_queries:
            logging.error('Provided state code [%s] does not have queries in ALL_QUERIES_BY_STATE')
            return

        # Set maximum display options
        pd.options.display.max_columns = 999
        pd.options.display.max_rows = 999

        succeeded_downloads = []
        failed_downloads = []
        for query_name, query in all_queries:
            try:
                self.download_query_to_csv(query=query, query_name=query_name)
                succeeded_downloads.append(query_name)
            except Exception:
                logging.exception('Could not download query [%s] and write to csv', query_name)
                failed_downloads.append(query_name)

        logging.info('Succeeded for queries %s', succeeded_downloads)
        if failed_downloads:
            logging.error('Failed for queries %s', failed_downloads)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs copy in dry-run mode, only prints downloads that the script woould do.')
    parser.add_argument('--local-dir', required=True, help='The local directory for store all written csvs.')
    parser.add_argument('--project-id', required=True, help='The project_id for the data to download')
    parser.add_argument('--state-code', required=True, help='The relevant state to download query results for.')
    parser.add_argument('--separator', required=False, default=',', help='Separator for the csv. Defaults to \',\'')
    parser.add_argument('--encoding', required=False, default='utf-8',
                        help='Encoding for the file to be written Defaults to utf-8')
    parser.add_argument('--start-date', required=True, help='Expected in the format %Y-%m-%d %H:%M')
    parser.add_argument('--end-date', required=True, help='Expected in the format %Y-%m-%d %H:%M')

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    DownloadBqResultsForStateController(
        dry_run=args.dry_run,
        project_id=args.project_id,
        state_code=args.state_code.lower(),
        start_date=to_datetime(args.start_date),
        end_date=to_datetime(args.end_date),
        local_dir=args.local_dir,
        encoding=args.encoding).download_queries_to_csvs()
