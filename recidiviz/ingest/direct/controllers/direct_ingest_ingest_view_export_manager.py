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

from recidiviz.big_query.big_query_client import BigQueryClient, ExportQueryConfig
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import DirectIngestPreProcessedIngestView
from recidiviz.ingest.direct.controllers.direct_ingest_file_metadata_manager import DirectIngestFileMetadataManager
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystem, \
    to_normalized_unprocessed_file_name
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import DirectIngestPreProcessedIngestViewCollector
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsIngestViewExportArgs, \
    GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.persistence.entity.operations.entities import DirectIngestIngestFileMetadata, DirectIngestRawFileMetadata
from recidiviz.utils import regions
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.regions import Region

UPPER_BOUND_TIMESTAMP_PARAM_NAME = 'update_timestamp_upper_bound_inclusive'
LOWER_BOUND_TIMESTAMP_PARAM_NAME = 'update_timestamp_lower_bound_exclusive'


@attr.s(frozen=True)
class _IngestViewExportState:

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


# TODO(3020): Detailed tests for this class
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
            lower_bound_date_exclusive = \
                export_state.last_export_metadata.datetimes_contained_upper_bound_inclusive \
                if export_state.last_export_metadata else None

            ingest_args_list = []
            for _date, upper_bound_date_inclusive in export_state.max_update_datetime_by_date:
                args = GcsfsIngestViewExportArgs(
                    ingest_view_name=ingest_view_tag,
                    upper_bound_datetime_prev=lower_bound_date_exclusive,
                    upper_bound_datetime_to_export=upper_bound_date_inclusive
                )
                logging.info('Generating job args for tag [%s]: [%s].', ingest_view_tag, args)

                self.file_metadata_manager.register_ingest_file_export_job(args)
                ingest_args_list.append(args)
                lower_bound_date_exclusive = upper_bound_date_inclusive

            jobs_to_schedule.extend(ingest_args_list)

        logging.info('Returning [%s] jobs to schedule.', len(jobs_to_schedule))
        return jobs_to_schedule

    def export_view_for_args(self, ingest_view_export_args: GcsfsIngestViewExportArgs) -> bool:
        """Performs an export of a single ingest view with date bounds specified in the provided args."""
        if not self.region.are_ingest_view_exports_enabled_in_env():
            raise ValueError(f'Ingest view exports not enabled for region [{self.region.region_code}]')

        metadata = self.file_metadata_manager.get_ingest_view_metadata_for_export_job(ingest_view_export_args)

        if not metadata:
            raise ValueError(f'Found no metadata for the given job args: [{ingest_view_export_args}].')

        if metadata.export_time:
            logging.warning('Already exported view for args [%s] - returning.', ingest_view_export_args)
            return False

        logging.info('Beginning export for view tag [%s] with args: [%s].',
                     ingest_view_export_args.ingest_view_name, ingest_view_export_args)

        query, query_params = self._generate_query_with_params(
            self.ingest_views_by_tag[ingest_view_export_args.ingest_view_name],
            ingest_view_export_args)

        logging.info('Generated export query [%s]', query)
        logging.info('Generated export query params [%s]', query_params)

        output_path = self._generate_output_path(ingest_view_export_args, metadata)

        logging.info('Generated output path [%s]', output_path.uri())

        if not metadata.normalized_file_name:
            self.file_metadata_manager.register_ingest_view_export_file_name(metadata, output_path)

        ingest_view = self.ingest_views_by_tag[ingest_view_export_args.ingest_view_name]
        export_configs = [
            ExportQueryConfig(
                query=query,
                query_parameters=query_params,
                intermediate_dataset_id=ingest_view.dataset_id,
                intermediate_table_name=f'{ingest_view_export_args.ingest_view_name}_latest_export',
                output_uri=output_path.uri(),
                output_format=bigquery.DestinationFormat.CSV
            )
        ]

        logging.info('Starting export to cloud storage.')
        self.big_query_client.export_query_results_to_cloud_storage(export_configs)
        logging.info('Export to cloud storage complete.')

        self.file_metadata_manager.mark_ingest_view_exported(metadata)
        return True

    @classmethod
    def print_debug_query_for_args(cls,
                                   ingest_views_by_tag: Dict[str, DirectIngestPreProcessedIngestView],
                                   ingest_view_export_args: GcsfsIngestViewExportArgs):
        """Prints a version of the export query for the provided args that can be run in the BigQuery UI."""
        query, query_params = cls._generate_query_with_params(
            ingest_views_by_tag[ingest_view_export_args.ingest_view_name],
            ingest_view_export_args)

        for param in query_params:
            dt = param.value
            query = query.replace(
                f'@{param.name}',
                f'DATETIME({dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute}, {dt.second})')

        print(query)

    @staticmethod
    def _generate_query_with_params(
            ingest_view: DirectIngestPreProcessedIngestView,
            ingest_view_export_args: GcsfsIngestViewExportArgs
    ) -> Tuple[str, List[bigquery.ScalarQueryParameter]]:
        """Generates a date bounded query that represents the data that has changed for this view between the specified
        date bounds in the provided export args.

        If there is no lower bound, this produces a query for a historical query up to the upper bound date. Otherwise,
        it diffs two historical queries to produce a delta query, using the SQL 'EXCEPT DISTINCT' function.

        Returns the query, along with the query params that must be passed to the BigQuery query job.
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
            query = query.rstrip().rstrip(';')
            filter_query = \
                ingest_view.date_parametrized_view_query(LOWER_BOUND_TIMESTAMP_PARAM_NAME).rstrip().rstrip(';')
            query = f'(\n{query}\n) EXCEPT DISTINCT (\n{filter_query}\n)'

        return query, query_params

    def _generate_output_path(self,
                              ingest_view_export_args: GcsfsIngestViewExportArgs,
                              metadata: DirectIngestIngestFileMetadata) -> GcsfsFilePath:
        ingest_view = self.ingest_views_by_tag[ingest_view_export_args.ingest_view_name]
        if not metadata.normalized_file_name:
            output_file_name = to_normalized_unprocessed_file_name(
                f'{ingest_view.file_tag}.csv',
                GcsfsDirectIngestFileType.INGEST_VIEW
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
    ingest_view_name_: str = 'early_discharge_supervision_sentence'
    upper_bound_datetime_prev_: datetime.datetime = datetime.datetime(2020, 5, 11)
    upper_bound_datetime_to_export_: datetime.datetime = datetime.datetime(2020, 5, 18)

    with local_project_id_override(GAE_PROJECT_STAGING):
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
