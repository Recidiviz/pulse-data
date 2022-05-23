# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a class and associated helpers that generates arguments for outstanding
ingest view materialization tasks for a particular region.
"""
import datetime
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import attr

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_args_generator_delegate import (
    IngestViewMaterializationArgsGeneratorDelegate,
    RegisteredMaterializationJob,
)
from recidiviz.ingest.direct.metadata.direct_ingest_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.utils.regions import Region


@attr.s(frozen=True)
class _IngestViewExportState:
    """Represents the current state of data exported from a particular ingest view."""

    ingest_view_name: str = attr.ib()

    # Metadata about the last materialization job registered for this view. May be for
    # a job that has not yet completed.
    last_registered_job: Optional[RegisteredMaterializationJob] = attr.ib()

    # A list of metadata rows for raw files that have been added since last_export_metadata was written to the DB.
    raw_table_dependency_updated_metadatas: List[
        DirectIngestRawFileMetadata
    ] = attr.ib()

    # A list with a tuple for each date that raw file dependencies were updated, along with the max
    # datetimes_contained_upper_bound_inclusive for raw tables updated on that date. This list is sorted in ascending
    # order by date.
    max_update_datetime_by_date: List[
        Tuple[datetime.date, datetime.datetime]
    ] = attr.ib()

    @max_update_datetime_by_date.default
    def _max_update_datetime_by_date(
        self,
    ) -> List[Tuple[datetime.date, datetime.datetime]]:
        date_dict: Dict[datetime.date, List[datetime.datetime]] = defaultdict(list)
        for dt in [
            m.datetimes_contained_upper_bound_inclusive
            for m in self.raw_table_dependency_updated_metadatas
        ]:
            date_dict[dt.date()].append(dt)
        result = []
        for day in sorted(date_dict.keys()):
            result.append((day, max(date_dict[day])))
        return result


class IngestViewMaterializationArgsGenerator:
    """Class that generates arguments for outstanding ingest view materialization tasks
    for a particular region.
    """

    def __init__(
        self,
        *,
        region: Region,
        delegate: IngestViewMaterializationArgsGeneratorDelegate,
        raw_file_metadata_manager: DirectIngestRawFileMetadataManager,
        view_collector: BigQueryViewCollector[
            DirectIngestPreProcessedIngestViewBuilder
        ],
        launched_ingest_views: List[str],
    ):

        self.region = region
        self.delegate = delegate
        self.raw_file_metadata_manager = raw_file_metadata_manager
        self.ingest_views_by_name = {
            builder.ingest_view_name: builder.build()
            for builder in view_collector.collect_view_builders()
            if builder.ingest_view_name in launched_ingest_views
        }

    def get_ingest_view_materialization_task_args(
        self,
    ) -> List[BQIngestViewMaterializationArgs]:
        """Looks at what files have been exported for a given region and returns args for all the export jobs that
        should be started, given what has updated in the raw data tables since the last time we exported data. Also
        returns any tasks that have not yet completed.
        """
        if not self.region.is_ingest_launched_in_env():
            raise ValueError(
                f"Ingest not enabled for region [{self.region.region_code}]"
            )

        logging.info("Finding outstanding ingest view materialization jobs...")
        logging.info("Gathering export state for each ingest view")
        ingest_view_to_export_state = {}
        for ingest_view_name, ingest_view in self.ingest_views_by_name.items():
            export_state = self._get_export_state_for_ingest_view(ingest_view)
            ingest_view_to_export_state[ingest_view_name] = export_state
        logging.info("Done gathering export state for each ingest view")

        # At this point we know that we have no new raw data backfills that should invalidate either pending or past
        # completed ingest view exports (checked in _get_export_state_for_ingest_view()). We can now generate any new
        # jobs.

        jobs_to_schedule = self.delegate.get_registered_jobs_pending_completion()

        logging.info(
            "Found [%s] already pending materialization jobs to schedule.",
            len(jobs_to_schedule),
        )

        logging.info("Generating new ingest view materialization jobs.")
        for ingest_view_name, export_state in ingest_view_to_export_state.items():
            ingest_view = self.ingest_views_by_name[ingest_view_name]
            lower_bound_datetime_exclusive = (
                export_state.last_registered_job.upper_bound_datetime_inclusive
                if export_state.last_registered_job
                else None
            )

            ingest_args_list = []
            for (
                _date,
                upper_bound_datetime_inclusive,
            ) in export_state.max_update_datetime_by_date:
                # If the view requires a reverse date diff (i.e. only outputting what
                # is found from Date 1 that's not in Date 2), then no work is necessary
                # when we have no lower bound date. We do not create an ingest view
                # materialization task for it in this case.
                if (
                    not lower_bound_datetime_exclusive
                    and ingest_view.do_reverse_date_diff
                ):
                    lower_bound_datetime_exclusive = upper_bound_datetime_inclusive
                    continue

                args = self.delegate.build_new_args(
                    ingest_view_name=ingest_view_name,
                    lower_bound_datetime_exclusive=lower_bound_datetime_exclusive,
                    upper_bound_datetime_inclusive=upper_bound_datetime_inclusive,
                )
                logging.info(
                    "Generating materialization job args for ingest view [%s]: [%s].",
                    ingest_view_name,
                    args,
                )

                self.delegate.register_new_job(args)
                ingest_args_list.append(args)
                lower_bound_datetime_exclusive = upper_bound_datetime_inclusive

            jobs_to_schedule.extend(ingest_args_list)

        logging.info(
            "Returning [%s] ingest view materialization jobs to schedule.",
            len(jobs_to_schedule),
        )
        return jobs_to_schedule

    def _get_export_state_for_ingest_view(
        self, ingest_view: DirectIngestPreProcessedIngestView
    ) -> _IngestViewExportState:
        """Gets list of all the raw files that are newer than the last export job.

        Additionally, validates that there are no files with dates prior to the last ingest view export. We can only
        process new files.
        """
        last_registered_job = self.delegate.get_most_recent_registered_job(
            ingest_view.ingest_view_name
        )
        last_job_time = (
            last_registered_job.job_creation_time if last_registered_job else None
        )

        raw_table_dependency_updated_metadatas = []

        for raw_file_tag in {
            config.file_tag for config in ingest_view.raw_table_dependency_configs
        }:
            raw_file_metadata_list = self.raw_file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(
                raw_file_tag, last_job_time
            )
            for raw_file_metadata in raw_file_metadata_list:
                # Check to see if the file comes BEFORE the last ingest view export for this view. If it does, and it is
                # not a code table, then this indicates that some sort of backfill is trying to process without
                # properly invalidating legacy ingest view metadata rows.
                if (
                    last_registered_job
                    and raw_file_metadata.datetimes_contained_upper_bound_inclusive
                    < last_registered_job.upper_bound_datetime_inclusive
                ):
                    # If it is not a code table, raise an error as we need to run a backfill. If it is a code table the
                    # the data should have already been migrated so we can ignore it.
                    if not raw_file_metadata.is_code_table:
                        raise ValueError(
                            f"Found a newly discovered raw file [tag: {raw_file_metadata.file_tag}] with an upper "
                            f"bound date [{raw_file_metadata.datetimes_contained_upper_bound_inclusive}] before the "
                            f"last valid export upper bound date "
                            f"[{last_registered_job.upper_bound_datetime_inclusive}]. Ingest view rows not "
                            f"properly invalidated before data backfill."
                        )
                # Otherwise, add to the list
                else:
                    raw_table_dependency_updated_metadatas.append(raw_file_metadata)

        return _IngestViewExportState(
            ingest_view_name=ingest_view.ingest_view_name,
            last_registered_job=last_registered_job,
            raw_table_dependency_updated_metadatas=raw_table_dependency_updated_metadatas,
        )
