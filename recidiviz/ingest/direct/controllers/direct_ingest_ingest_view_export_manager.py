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
"""Logic related to exporting ingest views to a region's direct ingest bucket."""
import datetime
import logging
from collections import defaultdict
from typing import List, Optional, Dict, Tuple

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import DirectIngestPreProcessedIngestView
from recidiviz.ingest.direct.controllers.direct_ingest_file_metadata_manager import DirectIngestFileMetadataManager
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystem, \
    to_normalized_unprocessed_file_name
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import DirectIngestPreProcessedIngestViewCollector
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsIngestViewExportArgs, \
    GcsfsDirectIngestFileType
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.persistence.entity.operations.entities import DirectIngestIngestFileMetadata, DirectIngestRawFileMetadata
from recidiviz.utils import regions
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.regions import Region

UPDATE_TIMESTAMP_PARAM_NAME = 'update_timestamp'
UPPER_BOUND_TIMESTAMP_PARAM_NAME = 'update_timestamp_upper_bound_inclusive'
LOWER_BOUND_TIMESTAMP_PARAM_NAME = 'update_timestamp_lower_bound_exclusive'
SELECT_SUBQUERY = 'SELECT * FROM `{project_id}.{dataset_id}.{table_name}`;'
TABLE_NAME_DATE_FORMAT = '%Y_%m_%d_%H_%M_%S'


@attr.s(frozen=True)
class _IngestViewExportState:
    ingest_view_file_tag: str = attr.ib()

    # The last ingest metadata row written for the view this represents. May be for an export job that has not yet
    # completed.
    last_export_metadata: Optional[DirectIngestIngestFileMetadata] = attr.ib()

    # A list of metadata rows for raw files that have been added since last_export_metadata was written to the DB.
    raw_table_dependency_updated_metadatas: List[DirectIngestRawFileMetadata] = attr.ib()

    # A list with a tuple for each date that raw file dependencies were updated, along with the max
    # datetimes_contained_upper_bound_inclusive for raw tables updated on that date. This list is sorted in ascending
    # order by date.
    max_update_datetime_by_date: List[Tuple[datetime.date, datetime.datetime]] = attr.ib()

    @max_update_datetime_by_date.default
    def _max_update_datetime_by_date(self) -> List[Tuple[datetime.date, datetime.datetime]]:
        date_dict: Dict[datetime.date, List[datetime.datetime]] = defaultdict(list)
        for dt in [m.datetimes_contained_upper_bound_inclusive for m in self.raw_table_dependency_updated_metadatas]:
            date_dict[dt.date()].append(dt)
        result = []
        for day in sorted(date_dict.keys()):
            result.append((day, max(date_dict[day])))
        return result


# TODO(#3020): Detailed tests for this class
class DirectIngestIngestViewExportManager:
    """Class that manages logic related to exporting ingest views to a region's direct ingest bucket."""
    def __init__(self,
                 *,
                 region: Region,
                 fs: DirectIngestGCSFileSystem,
                 ingest_directory_path: GcsfsDirectoryPath,
                 big_query_client: BigQueryClient,
                 file_metadata_manager: DirectIngestFileMetadataManager,
                 view_collector: BigQueryViewCollector[DirectIngestPreProcessedIngestView]):

        self.region = region
        self.fs = fs
        self.ingest_directory_path = ingest_directory_path
        self.big_query_client = big_query_client
        self.file_metadata_manager = file_metadata_manager
        self.ingest_views_by_tag = {
            view.file_tag: view
            for view in view_collector.collect_views()}

    def get_ingest_view_export_task_args(self) -> List[GcsfsIngestViewExportArgs]:
        """Looks at what files have been exported for a given region and returns args for all the export jobs that
        should be started, given what has updated in the raw data tables since the last time we exported data. Also
        returns any tasks that have not yet completed.
        """
        if not self.region.are_ingest_view_exports_enabled_in_env():
            raise ValueError(f'Ingest view exports not enabled for region [{self.region.region_code}]')

        logging.info('Gathering export state for each ingest tag')
        ingest_view_to_export_state = {}
        for ingest_view_tag, ingest_view in self.ingest_views_by_tag.items():
            export_state = self._get_export_state_for_ingest_view(ingest_view)
            self._validate_ascending_raw_file_update_dates(export_state)
            ingest_view_to_export_state[ingest_view_tag] = export_state
        logging.info('Done gathering export state for each ingest tag')

        # At this point we know that we have no new raw data backfills that should invalidate either pending or past
        # completed ingest view exports (checked in _validate_ascending_raw_file_update_dates()). We can now generate
        # any new jobs.

        jobs_to_schedule = []
        metadata_pending_export = self.file_metadata_manager.get_ingest_view_metadata_pending_export()
        if metadata_pending_export:
            args_list = self._export_args_from_metadata(metadata_pending_export)
            jobs_to_schedule.extend(args_list)

        logging.info('Found [%s] already pending jobs to schedule.', len(jobs_to_schedule))

        logging.info('Generating new ingest jobs.')
        for ingest_view_tag, export_state in ingest_view_to_export_state.items():
            lower_bound_datetime_exclusive = \
                export_state.last_export_metadata.datetimes_contained_upper_bound_inclusive \
                if export_state.last_export_metadata else None

            ingest_args_list = []
            for _date, upper_bound_datetime_inclusive in export_state.max_update_datetime_by_date:
                args = GcsfsIngestViewExportArgs(
                    ingest_view_name=ingest_view_tag,
                    upper_bound_datetime_prev=lower_bound_datetime_exclusive,
                    upper_bound_datetime_to_export=upper_bound_datetime_inclusive
                )
                logging.info('Generating job args for tag [%s]: [%s].', ingest_view_tag, args)

                self.file_metadata_manager.register_ingest_file_export_job(args)
                ingest_args_list.append(args)
                lower_bound_datetime_exclusive = upper_bound_datetime_inclusive

            jobs_to_schedule.extend(ingest_args_list)

        logging.info('Returning [%s] jobs to schedule.', len(jobs_to_schedule))
        return jobs_to_schedule

    def _generate_export_job_for_date(
            self,
            table_name: str,
            ingest_view: DirectIngestPreProcessedIngestView,
            date_bound: datetime.datetime) -> bigquery.QueryJob:
        """Generates a query for the provided |ingest view| on the given |date bound| and starts a job to load the
        results of that query into the provided |table_name|. Returns the potentially in progress QueryJob to the
        caller.
        """
        query, query_params = self._generate_query_and_params_for_date(ingest_view, date_bound)
        query_job = self.big_query_client.create_table_from_query_async(
            dataset_id=ingest_view.dataset_id,
            table_id=table_name,
            query=query,
            query_parameters=query_params,
            overwrite=True)
        return query_job

    @staticmethod
    def create_date_diff_query(upper_bound_query: str, upper_bound_prev_query: str, do_reverse_date_diff: bool) -> str:
        """Provided the given |upper_bound_query| and |upper_bound_prev_query| returns a query which will return the
        delta between those two queries. The ordering of the comparison depends on the provided |do_reverse_date_diff|.
        """
        main_query, filter_query = (upper_bound_prev_query, upper_bound_query) \
            if do_reverse_date_diff else (upper_bound_query, upper_bound_prev_query)
        filter_query = filter_query.rstrip().rstrip(';')
        main_query = main_query.rstrip().rstrip(';')
        query = f'(\n{main_query}\n) EXCEPT DISTINCT (\n{filter_query}\n);'
        return query

    def export_view_for_args(self, ingest_view_export_args: GcsfsIngestViewExportArgs) -> bool:
        """Performs an Cloud Storage export of a single ingest view with date bounds specified in the provided args. If
        the provided args contain an upper and lower bound date, the exported view contains only the delta between the
        two dates. If only the upper bound is provided, then the exported view contains historical results up until the
        bound date.

        Note: In order to prevent resource exhaustion in BigQuery, the ultimate query in this method is broken down
        into distinct parts. This method first persists the results of historical queries for each given bound date
        (upper and lower) into temporary tables. The delta between those tables is then queried separately using
        SQL's `EXCEPT DISTINCT` and those final results are exported to Cloud Storage.
        """
        if not self.region.are_ingest_view_exports_enabled_in_env():
            raise ValueError(f'Ingest view exports not enabled for region [{self.region.region_code}]')

        metadata = self.file_metadata_manager.get_ingest_view_metadata_for_export_job(ingest_view_export_args)

        if not metadata:
            raise ValueError(f'Found no metadata for the given job args: [{ingest_view_export_args}].')

        if metadata.export_time:
            logging.warning('Already exported view for args [%s] - returning.', ingest_view_export_args)
            return False

        output_path = self._generate_output_path(ingest_view_export_args, metadata)
        logging.info('Generated output path [%s]', output_path.uri())

        if not metadata.normalized_file_name:
            self.file_metadata_manager.register_ingest_view_export_file_name(metadata, output_path)

        ingest_view = self.ingest_views_by_tag[ingest_view_export_args.ingest_view_name]

        # If the view requires a reverse date diff (i.e. only outputting what is found from Date 1 that's not in Date
        # 2), then no work is necessary when we only have one date.
        if ingest_view.do_reverse_date_diff and not ingest_view_export_args.upper_bound_datetime_prev:
            return True

        single_date_table_ids = []
        single_date_table_export_jobs = []

        upper_bound_table_name = \
            f'{ingest_view_export_args.ingest_view_name}_' \
            f'{ingest_view_export_args.upper_bound_datetime_to_export.strftime(TABLE_NAME_DATE_FORMAT)}_' \
            f'upper_bound'
        export_job = self._generate_export_job_for_date(
            table_name=upper_bound_table_name,
            ingest_view=ingest_view,
            date_bound=ingest_view_export_args.upper_bound_datetime_to_export)
        single_date_table_ids.append(upper_bound_table_name)
        single_date_table_export_jobs.append(export_job)

        query = SELECT_SUBQUERY.format(
            project_id=self.big_query_client.project_id,
            dataset_id=ingest_view.dataset_id,
            table_name=upper_bound_table_name)

        if ingest_view_export_args.upper_bound_datetime_prev:
            lower_bound_table_name = \
                f'{ingest_view_export_args.ingest_view_name}_' \
                f'{ingest_view_export_args.upper_bound_datetime_prev.strftime(TABLE_NAME_DATE_FORMAT)}_' \
                f'lower_bound'
            export_job = self._generate_export_job_for_date(
                table_name=lower_bound_table_name,
                ingest_view=ingest_view,
                date_bound=ingest_view_export_args.upper_bound_datetime_prev)
            single_date_table_export_jobs.append(export_job)
            single_date_table_ids.append(lower_bound_table_name)

            upper_bound_prev_query = SELECT_SUBQUERY.format(
                project_id=self.big_query_client.project_id,
                dataset_id=ingest_view.dataset_id,
                table_name=lower_bound_table_name)
            query = DirectIngestIngestViewExportManager.create_date_diff_query(
                upper_bound_query=query,
                upper_bound_prev_query=upper_bound_prev_query,
                do_reverse_date_diff=ingest_view.do_reverse_date_diff)

        query = DirectIngestPreProcessedIngestView.add_order_by_suffix(
            query=query, order_by_cols=ingest_view.order_by_cols)

        # Wait for completion of all async date queries
        for query_job in single_date_table_export_jobs:
            query_job.result()
        logging.info('Completed loading results of individual date queries into intermediate tables.')

        logging.info('Generated final export query [%s]', str(query))

        export_configs = [
            ExportQueryConfig(
                query=query,
                query_parameters=[],
                intermediate_dataset_id=ingest_view.dataset_id,
                intermediate_table_name=f'{ingest_view_export_args.ingest_view_name}_latest_export',
                output_uri=output_path.uri(),
                output_format=bigquery.DestinationFormat.CSV,
            )
        ]

        logging.info('Starting export to cloud storage.')
        self.big_query_client.export_query_results_to_cloud_storage(export_configs=export_configs)
        logging.info('Export to cloud storage complete.')

        for table_id in single_date_table_ids:
            self.big_query_client.delete_table(dataset_id=ingest_view.dataset_id, table_id=table_id)
            logging.info('Deleted intermediate table [%s]', table_id)

        self.file_metadata_manager.mark_ingest_view_exported(metadata)

        return True

    @classmethod
    def print_debug_query_for_args(cls,
                                   ingest_views_by_tag: Dict[str, DirectIngestPreProcessedIngestView],
                                   ingest_view_export_args: GcsfsIngestViewExportArgs):
        """Prints a version of the export query for the provided args that can be run in the BigQuery UI."""
        query, query_params = cls._debug_generate_unified_query(
            ingest_views_by_tag[ingest_view_export_args.ingest_view_name],
            ingest_view_export_args)

        for param in query_params:
            dt = param.value
            query = query.replace(
                f'@{param.name}',
                f'DATETIME({dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute}, {dt.second})')

        print(query)

    @staticmethod
    def _debug_generate_unified_query(
            ingest_view: DirectIngestPreProcessedIngestView,
            ingest_view_export_args: GcsfsIngestViewExportArgs
    ) -> Tuple[str, List[bigquery.ScalarQueryParameter]]:
        """Generates a single query that is date bounded such that it represents the data that has changed for this view
        between the specified date bounds in the provided export args.

        If there is no lower bound, this produces a query for a historical query up to the upper bound date. Otherwise,
        it diffs two historical queries to produce a delta query, using the SQL 'EXCEPT DISTINCT' function.

        Important Note: This query is meant for debug use only. In the actual DirectIngest flow, query results for
        individual dates are persisted into temporary tables, and those temporary tables are then diff'd using SQL's
        `EXCEPT DISTINCT` function.
        """

        query_params = [
            bigquery.ScalarQueryParameter(UPPER_BOUND_TIMESTAMP_PARAM_NAME,
                                          bigquery.enums.SqlTypeNames.DATETIME.value,
                                          ingest_view_export_args.upper_bound_datetime_to_export)
        ]
        query = ingest_view.date_parametrized_view_query(UPPER_BOUND_TIMESTAMP_PARAM_NAME)
        if ingest_view_export_args.upper_bound_datetime_prev:
            query_params.append(
                bigquery.ScalarQueryParameter(LOWER_BOUND_TIMESTAMP_PARAM_NAME,
                                              bigquery.enums.SqlTypeNames.DATETIME.value,
                                              ingest_view_export_args.upper_bound_datetime_prev)
            )
            query = DirectIngestIngestViewExportManager.create_date_diff_query(
                upper_bound_query=query,
                upper_bound_prev_query=ingest_view.date_parametrized_view_query(LOWER_BOUND_TIMESTAMP_PARAM_NAME),
                do_reverse_date_diff=ingest_view.do_reverse_date_diff)
            query = DirectIngestPreProcessedIngestView.add_order_by_suffix(
                query=query, order_by_cols=ingest_view.order_by_cols)
        return query, query_params

    @staticmethod
    def _generate_query_and_params_for_date(
            ingest_view: DirectIngestPreProcessedIngestView,
            update_timestamp: datetime.datetime
    ) -> Tuple[str, List[bigquery.ScalarQueryParameter]]:
        """Generates a single query for the provided |ingest view| that is date bounded by |update_timestamp|."""
        query_params = [
            bigquery.ScalarQueryParameter(UPDATE_TIMESTAMP_PARAM_NAME,
                                          bigquery.enums.SqlTypeNames.DATETIME.value,
                                          update_timestamp)
        ]
        query = ingest_view.date_parametrized_view_query(UPDATE_TIMESTAMP_PARAM_NAME)
        logging.info('Generated bound query with params \nquery: [%s]\nparams: [%s]', query, query_params)
        return query, query_params

    def _generate_output_path(self,
                              ingest_view_export_args: GcsfsIngestViewExportArgs,
                              metadata: DirectIngestIngestFileMetadata) -> GcsfsFilePath:
        ingest_view = self.ingest_views_by_tag[ingest_view_export_args.ingest_view_name]
        if not metadata.normalized_file_name:
            output_file_name = to_normalized_unprocessed_file_name(
                f'{ingest_view.file_tag}.csv',
                GcsfsDirectIngestFileType.INGEST_VIEW,
                dt=ingest_view_export_args.upper_bound_datetime_to_export
            )
        else:
            output_file_name = metadata.normalized_file_name

        return GcsfsFilePath.from_directory_and_file_name(self.ingest_directory_path, output_file_name)

    def _get_export_state_for_ingest_view(self,
                                          ingest_view: DirectIngestPreProcessedIngestView) -> _IngestViewExportState:
        last_export_metadata = \
            self.file_metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job(ingest_view.file_tag)
        last_job_time = last_export_metadata.job_creation_time if last_export_metadata else None

        raw_table_dependency_updated_metadatas = []

        for raw_file_tag in {config.file_tag for config in ingest_view.raw_table_dependency_configs}:
            raw_file_metadata_list = \
                self.file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(raw_file_tag,
                                                                                                last_job_time)
            raw_table_dependency_updated_metadatas.extend(raw_file_metadata_list)

        return _IngestViewExportState(
            ingest_view_file_tag=ingest_view.file_tag,
            last_export_metadata=last_export_metadata,
            raw_table_dependency_updated_metadatas=raw_table_dependency_updated_metadatas
        )

    @staticmethod
    def _validate_ascending_raw_file_update_dates(export_state: _IngestViewExportState) -> None:
        """Checks that there are no new raw files with update dates that come BEFORE the last ingest view export for
        this view (indicating that some sort of backfill is trying to process with out properly invalidating legacy
        ingest view metadata rows.
        """

        for raw_file_metadata in export_state.raw_table_dependency_updated_metadatas:
            if export_state.last_export_metadata and \
                    raw_file_metadata.datetimes_contained_upper_bound_inclusive < \
                    export_state.last_export_metadata.datetimes_contained_upper_bound_inclusive:
                raise ValueError(
                    f'Found a newly discovered raw file with an upper bound date '
                    f'[{raw_file_metadata.datetimes_contained_upper_bound_inclusive}] before the last valid export '
                    f'upper bound date [{export_state.last_export_metadata.datetimes_contained_upper_bound_inclusive}].'
                    f' Ingest view rows not properly invalidated before data backfill.')

    @staticmethod
    def _export_args_from_metadata(
            metadata_list: List[DirectIngestIngestFileMetadata]) -> List[GcsfsIngestViewExportArgs]:
        return [GcsfsIngestViewExportArgs(
            ingest_view_name=metadata.file_tag,
            upper_bound_datetime_prev=metadata.datetimes_contained_lower_bound_exclusive,
            upper_bound_datetime_to_export=metadata.datetimes_contained_upper_bound_inclusive
        ) for metadata in metadata_list]


if __name__ == '__main__':

    # Update these variables and run to print an export query you can run in the BigQuery UI
    region_code_: str = 'us_id'
    ingest_view_name_: str = 'early_discharge_incarceration_sentence_deleted_rows'
    upper_bound_datetime_prev_: datetime.datetime = datetime.datetime(2020, 6, 29)
    upper_bound_datetime_to_export_: datetime.datetime = datetime.datetime(2020, 7, 6)

    with local_project_id_override(GCP_PROJECT_STAGING):
        region_ = regions.get_region(region_code_, is_direct_ingest=True)
        view_collector_ = DirectIngestPreProcessedIngestViewCollector(region_, [])
        views_by_tag_ = {
            view.file_tag: view
            for view in view_collector_.collect_views()}

        DirectIngestIngestViewExportManager.print_debug_query_for_args(
            views_by_tag_,
            GcsfsIngestViewExportArgs(
                ingest_view_name=ingest_view_name_,
                upper_bound_datetime_prev=upper_bound_datetime_prev_,
                upper_bound_datetime_to_export=upper_bound_datetime_to_export_
            )
        )
