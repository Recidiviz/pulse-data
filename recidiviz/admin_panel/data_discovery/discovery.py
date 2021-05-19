# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
Data discovery tool for raw data files and ingest views.
"""
import logging
from collections import defaultdict
from io import BytesIO
from typing import List, Tuple, Optional, Literal

import pandas as pd
import pyarrow
import pyarrow.parquet as pq

from recidiviz.admin_panel.data_discovery.arguments import (
    DataDiscoveryArgs,
    DataDiscoveryArgsFactory,
    ConditionGroup,
)
from recidiviz.admin_panel.data_discovery.cache_ingest_file_as_parquet import (
    SingleIngestFileParquetCache,
)
from recidiviz.admin_panel.data_discovery.file_configs import (
    get_raw_data_configs,
    get_ingest_view_configs,
)
from recidiviz.admin_panel.data_discovery.report_builder import build_report
from recidiviz.admin_panel.data_discovery.task_manager import (
    AdminPanelDataDiscoveryCloudTaskManager,
    DevelopmentAdminPanelDataDiscoveryCloudTaskManager,
    AbstractAdminPanelDataDiscoveryCloudTaskManager,
)
from recidiviz.admin_panel.data_discovery.types import (
    ConfigsByFileType,
    FilesByFileType,
    DataFramesByFileType,
    DataDiscoveryTTL,
)
from recidiviz.admin_panel.data_discovery.utils import (
    get_data_discovery_cache,
    get_data_discovery_communicator,
)
from recidiviz.cloud_memorystore.redis_communicator import (
    MessageKind,
)
from recidiviz.cloud_memorystore.utils import await_redis_keys, RedisKeyTimeoutError
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    filename_parts_from_path,
    GcsfsDirectIngestFileType,
)
from recidiviz.ingest.direct.errors import DirectIngestError, DirectIngestErrorType
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.future_executor import FutureExecutor

logger = logging.getLogger(__file__)

flatten = lambda t: [item for sublist in t for item in sublist]


class DataDiscoveryException(Exception):
    pass


def list_object_thread_task(bucket_name: str, prefix: str) -> List[str]:
    gcsfs = GcsfsFactory.build()
    return [
        path.uri()
        for path in gcsfs.ls_with_blob_prefix(bucket_name, prefix)
        if isinstance(path, GcsfsFilePath)
    ]


def get_direct_ingest_storage_paths(args: DataDiscoveryArgs) -> List[str]:
    """ Returns the list of ingest file paths in GCS """
    cache = get_data_discovery_cache()

    if cache.exists(args.state_files_cache_key):
        state_files = (cache.get(args.state_files_cache_key) or b"").decode("utf-8")
    else:
        communicator = get_data_discovery_communicator(args.communication_channel_uuid)
        list_object_targets = [
            {
                "bucket_name": args.direct_ingest_storage_directory.bucket_name,
                "prefix": search_path,
            }
            for search_path in args.search_paths
        ]

        with FutureExecutor.build(
            list_object_thread_task, list_object_targets
        ) as future_executor:
            for progress in future_executor.progress():
                communicator.communicate(
                    f"Preparing GCS object path cache. {progress.format()}"
                )

        state_files = "\n".join(flatten(future_executor.results()))

        cache.setex(
            args.state_files_cache_key,
            DataDiscoveryTTL.STATE_FILES,
            state_files,
        )

    return state_files.split("\n")


def collect_file_paths(
    data_discovery_args: DataDiscoveryArgs,
    configs: ConfigsByFileType,
    gcs_files: List[str],
) -> FilesByFileType:
    """ Given a set of configs configs, filter the listed GCS files to only those that match our search filters """
    collected_files = defaultdict(list)

    for found_file in gcs_files:
        try:
            path = GcsfsFilePath.from_absolute_path(found_file)
            file_parts = filename_parts_from_path(path)
        except DirectIngestError as e:
            if e.error_type == DirectIngestErrorType.INPUT_ERROR:
                continue

            logger.exception(e)
            continue

        if (
            not data_discovery_args.start_date
            <= file_parts.utc_upload_datetime.date()
            <= data_discovery_args.end_date
        ):
            continue

        if file_parts.is_file_split:
            continue

        if file_parts.file_tag in configs[file_parts.file_type]:
            collected_files[file_parts.file_type].append(path)

    return collected_files


def create_cache_ingest_file_as_parquet_task(
    gcs_file: GcsfsFilePath,
    separator: str,
    encoding: str,
    quoting: int,
    custom_line_terminator: Optional[str],
) -> None:
    if in_gcp():
        task_manager = (
            AdminPanelDataDiscoveryCloudTaskManager()
        )  # type: AbstractAdminPanelDataDiscoveryCloudTaskManager
    else:
        task_manager = DevelopmentAdminPanelDataDiscoveryCloudTaskManager()

    task_manager.create_cache_ingest_file_as_parquet_task(
        gcs_file, separator, encoding, quoting, custom_line_terminator
    )


def prepare_cache(
    args: DataDiscoveryArgs, files_by_type: FilesByFileType, configs: ConfigsByFileType
) -> None:
    """ Given a set of files, download them from GCS to our cache directory """
    communicator = get_data_discovery_communicator(args.communication_channel_uuid)

    cache = get_data_discovery_cache()
    required_redis_keys = []

    targets = []

    for file_type, gcs_files in files_by_type.items():
        for gcs_file in gcs_files:
            parquet_key = SingleIngestFileParquetCache.parquet_cache_key(gcs_file)
            required_redis_keys.append(parquet_key)

            if cache.exists(parquet_key):
                # Popular file; extend expiration time
                cache.expire(parquet_key, DataDiscoveryTTL.PARQUET_FILES)
                # No need to enqueue a cache task as it already exists
                continue

            file_parts = filename_parts_from_path(gcs_file)
            config = configs[file_type][file_parts.file_tag]
            targets.append(
                {
                    "gcs_file": gcs_file,
                    "separator": config.separator,
                    "encoding": config.encoding,
                    "quoting": config.quoting,
                    "custom_line_terminator": config.custom_line_terminator,
                }
            )

    with FutureExecutor.build(
        create_cache_ingest_file_as_parquet_task, targets, max_workers=12
    ) as future_execution:
        for progress in future_execution.progress():
            communicator.communicate(
                f"Enqueueing cache_ingest_file_as_parquet tasks. {progress.format()}"
            )

    try:
        total = len(required_redis_keys)
        for remaining_keys in await_redis_keys(cache, required_redis_keys):
            completed = total - len(remaining_keys)
            communicator.communicate(
                f"Awaiting ingest file cache {completed} / {total}"
            )
    except RedisKeyTimeoutError as e:
        missing_keys = "\n".join(e.missing_keys)
        message = f"Failed to cache the following ingest files: {missing_keys}"
        raise DataDiscoveryException(message) from e


PyArrowCondition = Tuple[str, Literal["in", "not in"], List[str]]
PyArrowConditionGroup = List[PyArrowCondition]


def get_filters(
    parquet: BytesIO, condition_groups: List[ConditionGroup]
) -> Optional[List[PyArrowConditionGroup]]:
    """ Given a dataframe and list of conditions, filter the dataframe using a boolean OR """
    schema = pq.read_schema(parquet)

    filters: List[PyArrowConditionGroup] = []

    for group in condition_groups:
        group_tuple = []

        for entry in group["conditions"]:
            try:
                field = schema.field(entry["column"])

                if field.type == pyarrow.null():
                    # Empty dataset; thus no inherent data type for entry["column"]
                    continue

                group_tuple.append(
                    (
                        entry["column"],
                        entry["operator"],
                        entry["values"],
                    )
                )
            except KeyError:
                # Column does not exist within schema
                continue

        if group_tuple:
            filters.append(group_tuple)

    return filters or None


def load_dataframe(
    file_type: GcsfsDirectIngestFileType,
    file_tag: str,
    gcs_file: GcsfsFilePath,
    condition_groups: List[ConditionGroup],
) -> Tuple[GcsfsDirectIngestFileType, str, pd.DataFrame]:
    """ Returns the dataframe's identifiers as well as the dataframe itself"""
    try:
        dataframe = pd.DataFrame()
        parquet_cache = SingleIngestFileParquetCache(
            get_data_discovery_cache(), gcs_file, expiry=DataDiscoveryTTL.PARQUET_FILES
        )
        for parquet_file in parquet_cache.get_parquet_files():
            dataframe = dataframe.append(
                pd.read_parquet(
                    parquet_file,
                    filters=get_filters(parquet_file, condition_groups),
                    buffer_size=1024 * 40,
                )
            )

            if len(dataframe) > 25000:
                raise DataDiscoveryException(
                    """
                        Expected less than 25,000 results for a single file.
                        Please narrow your search criteria.
                    """
                )

        return file_type, file_tag, dataframe
    except pd.errors.ParserError as e:
        logger.info("Skipping file: %s due to %s", gcs_file.file_name, e)
        return file_type, file_tag, pd.DataFrame()


def load_dataframes(
    data_discovery_args: DataDiscoveryArgs, files_by_type: FilesByFileType
) -> DataFramesByFileType:
    """ Given a set of files and configs, load the data into dataframes in parallel """
    dataframes: DataFramesByFileType = defaultdict(lambda: defaultdict(pd.DataFrame))
    load_dataframe_args = []

    for file_type, gcs_files in files_by_type.items():
        for gcs_file in gcs_files:
            file_parts = filename_parts_from_path(gcs_file)

            load_dataframe_args.append(
                {
                    "file_type": file_type,
                    "file_tag": file_parts.file_tag,
                    "gcs_file": gcs_file,
                    "condition_groups": data_discovery_args.condition_groups,
                }
            )

    communicator = get_data_discovery_communicator(
        data_discovery_args.communication_channel_uuid
    )

    with FutureExecutor.build(
        load_dataframe, load_dataframe_args, max_workers=12
    ) as future_executor:
        for progress in future_executor.progress():
            communicator.communicate(f"Loading dataframes - {progress.format()}")

    for file_type, file_tag, dataframe in future_executor.results():
        if len(dataframe) == 0:
            continue

        dataframes[file_type][file_tag] = dataframes[file_type][file_tag].append(
            dataframe
        )

        if len(dataframes[file_type][file_tag]) > 7500:
            raise DataDiscoveryException(
                f"""
                     {file_type} {file_tag} returned more than 7,500 rows.
                     Searches are limited to 7,500 results per file type.
                     Shorten the date range or change filter conditions and try again.
                 """
            )

    return dataframes


def discover_data(discovery_id: str) -> None:
    """Main entrypoint for the data discovery task; Finds files/views that contain provided columns;
    Downloads files from GCS, applies filters against the data, and builds a report"""
    data_discovery_args = DataDiscoveryArgsFactory.fetch(discovery_id)
    communicator = get_data_discovery_communicator(
        data_discovery_args.communication_channel_uuid
    )

    try:
        communicator.communicate("Collecting view configurations")
        configs = {
            GcsfsDirectIngestFileType.RAW_DATA: {
                config.file_tag: config
                for config in get_raw_data_configs(data_discovery_args.region_code)
                if config.file_tag in data_discovery_args.raw_files
            },
            GcsfsDirectIngestFileType.INGEST_VIEW: {
                config.file_tag: config
                for config in get_ingest_view_configs(data_discovery_args.region_code)
                if config.file_tag in data_discovery_args.ingest_views
            },
        }

        communicator.communicate("Finding GCS file paths")
        files_by_type = collect_file_paths(
            data_discovery_args,
            configs,
            get_direct_ingest_storage_paths(data_discovery_args),
        )
        raw_file_count = len(files_by_type[GcsfsDirectIngestFileType.RAW_DATA])
        ingest_view_count = len(files_by_type[GcsfsDirectIngestFileType.INGEST_VIEW])

        if raw_file_count + ingest_view_count > 2500:
            raise DataDiscoveryException(
                f"""
                    Tried to search {raw_file_count + ingest_view_count} files.
                    Searches are limited to 2,500 files.
                    Shorten the date range filter and try again.
                """
            )

        prepare_cache(data_discovery_args, files_by_type, configs)

        communicator.communicate("Loading data/dataframes")
        loaded_dataframes = load_dataframes(data_discovery_args, files_by_type)

        communicator.communicate("Building report")
        communicator.communicate(
            build_report(loaded_dataframes, configs), kind=MessageKind.CLOSE
        )
    except DataDiscoveryException as e:
        communicator.communicate(
            f"Failed to search data, due to exception: {e}", kind=MessageKind.CLOSE
        )
