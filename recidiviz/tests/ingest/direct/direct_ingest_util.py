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
"""Helpers for direct ingest tests."""
import datetime
import time
import unittest
from types import ModuleType
from typing import Dict, List, Optional, Type

import attr
from mock import Mock, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers import (
    direct_ingest_raw_table_migration_collector,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_file_path,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileImportManager,
    DirectIngestRegionRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    GcsfsIngestArgs,
    filename_parts_from_path,
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.tests.cloud_storage.fake_gcs_file_system import (
    FakeGCSFileSystem,
    FakeGCSFileSystemDelegate,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.ingest.direct.fake_async_direct_ingest_cloud_task_manager import (
    FakeAsyncDirectIngestCloudTaskManager,
)
from recidiviz.tests.ingest.direct.fake_direct_ingest_big_query_client import (
    FakeDirectIngestBigQueryClient,
)
from recidiviz.tests.ingest.direct.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)
from recidiviz.utils import metadata
from recidiviz.utils.regions import Region

PLACEHOLDER_TO_DO_STRING = "TO" + "DO"


class DirectIngestFakeGCSFileSystemDelegate(FakeGCSFileSystemDelegate):
    def __init__(self, controller: BaseDirectIngestController, can_start_ingest: bool):
        self.controller = controller
        self.can_start_ingest = can_start_ingest

    def on_file_added(self, path: GcsfsFilePath) -> None:
        if path.abs_path().startswith(self.controller.ingest_bucket_path.abs_path()):
            self.controller.handle_file(path, start_ingest=self.can_start_ingest)


@attr.s
class FakeDirectIngestRegionRawFileConfig(DirectIngestRegionRawFileConfig):
    def _get_raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        return {
            "tagA": DirectIngestRawFileConfig(
                file_tag="tagA",
                file_path="path/to/tagA.yaml",
                file_description="file description",
                primary_key_cols=["mockKey"],
                columns=[
                    RawTableColumnInfo(
                        name="mockKey",
                        description="mockKey description",
                        is_datetime=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
            ),
            "tagB": DirectIngestRawFileConfig(
                file_tag="tagB",
                file_path="path/to/tagB.yaml",
                file_description="file description",
                primary_key_cols=["mockKey"],
                columns=[
                    RawTableColumnInfo(
                        name="mockKey",
                        description="mockKey description",
                        is_datetime=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
            ),
            "tagC": DirectIngestRawFileConfig(
                file_tag="tagC",
                file_path="path/to/tagC.yaml",
                file_description="file description",
                primary_key_cols=["mockKey"],
                columns=[
                    RawTableColumnInfo(
                        name="mockKey",
                        description="mockKey description",
                        is_datetime=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
            ),
            "tagWeDoNotIngest": DirectIngestRawFileConfig(
                file_tag="tagWeDoNotIngest",
                file_path="path/to/tagWeDoNotIngest.yaml",
                file_description="file description",
                primary_key_cols=[],
                columns=[],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
            ),
        }


class FakeDirectIngestRawFileImportManager(DirectIngestRawFileImportManager):
    """Fake implementation of DirectIngestRawFileImportManager for tests."""

    def __init__(
        self,
        *,
        region: Region,
        fs: DirectIngestGCSFileSystem,
        ingest_bucket_path: GcsfsBucketPath,
        temp_output_directory_path: GcsfsDirectoryPath,
        big_query_client: BigQueryClient,
    ):
        super().__init__(
            region=region,
            fs=fs,
            ingest_bucket_path=ingest_bucket_path,
            temp_output_directory_path=temp_output_directory_path,
            big_query_client=big_query_client,
            region_raw_file_config=FakeDirectIngestRegionRawFileConfig(
                region.region_code
            ),
        )
        self.imported_paths: List[GcsfsFilePath] = []

    def import_raw_file_to_big_query(
        self, path: GcsfsFilePath, file_metadata: DirectIngestRawFileMetadata
    ) -> None:
        self.imported_paths.append(path)


class FakeDirectIngestPreProcessedIngestViewBuilder(
    BigQueryViewBuilder[DirectIngestPreProcessedIngestView]
):
    """Fake BQ View Builder for tests."""

    def __init__(
        self, tag: str, region: Region, view_query_template: Optional[str] = None
    ):
        self.region = region
        self.tag = tag
        self.view_query_template = (
            view_query_template or f"SELECT * FROM {{{self.tag}}}"
        )

    def _build(
        self,
        *,
        dataset_overrides: Optional[  # pylint: disable=unused-argument
            Dict[str, str]
        ] = None,
    ) -> DirectIngestPreProcessedIngestView:
        return DirectIngestPreProcessedIngestView(
            ingest_view_name=self.tag,
            view_query_template=self.view_query_template,
            region_raw_table_config=FakeDirectIngestRegionRawFileConfig(
                region_code=self.region.region_code
            ),
            order_by_cols=None,
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

    def build_and_print(self) -> None:
        self.build()

    def should_build(self) -> bool:
        return True

    @property
    def file_tag(self) -> str:
        return self.tag


class FakeDirectIngestPreProcessedIngestViewCollector(
    DirectIngestPreProcessedIngestViewCollector
):
    def __init__(self, region: Region, controller_tag_rank_list: List[str]):
        super().__init__(region, controller_tag_rank_list)

    def collect_view_builders(self) -> List[DirectIngestPreProcessedIngestViewBuilder]:
        builders = [
            DirectIngestPreProcessedIngestViewBuilder(
                region=self.region.region_code,
                ingest_view_name=tag,
                view_query_template=f"SELECT * FROM {{{tag}}}",
                order_by_cols="",
            )
            for tag in self.controller_tag_rank_list
        ]

        builders.append(
            DirectIngestPreProcessedIngestViewBuilder(
                ingest_view_name="gatedTagNotInTagsList",
                region=self.region.region_code,
                view_query_template="SELECT * FROM {tagA} LEFT OUTER JOIN {tagB} USING (col);",
                order_by_cols="",
            )
        )

        return builders


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging"))
def build_gcsfs_controller_for_tests(
    controller_cls: Type[BaseDirectIngestController],
    ingest_instance: DirectIngestInstance,
    run_async: bool,
    can_start_ingest: bool = True,
    regions_module: ModuleType = fake_regions_module,
) -> BaseDirectIngestController:
    """Builds an instance of |controller_cls| for use in tests with several internal classes mocked properly. """
    fake_fs = FakeGCSFileSystem()

    def mock_build_fs() -> FakeGCSFileSystem:
        return fake_fs

    if "TestDirectIngestController" in controller_cls.__name__:
        view_collector_cls: Type[
            BigQueryViewCollector
        ] = FakeDirectIngestPreProcessedIngestViewCollector
    else:
        view_collector_cls = DirectIngestPreProcessedIngestViewCollector

    with patch(
        f"{BaseDirectIngestController.__module__}.DirectIngestCloudTaskManagerImpl"
    ) as mock_task_factory_cls, patch(
        f"{BaseDirectIngestController.__module__}.BigQueryClientImpl"
    ) as mock_big_query_client_cls, patch(
        f"{BaseDirectIngestController.__module__}.DirectIngestRawFileImportManager",
        FakeDirectIngestRawFileImportManager,
    ), patch(
        f"{BaseDirectIngestController.__module__}.DirectIngestPreProcessedIngestViewCollector",
        view_collector_cls,
    ):
        task_manager = (
            FakeAsyncDirectIngestCloudTaskManager()
            if run_async
            else FakeSynchronousDirectIngestCloudTaskManager()
        )
        mock_task_factory_cls.return_value = task_manager
        mock_big_query_client_cls.return_value = FakeDirectIngestBigQueryClient(
            project_id=metadata.project_id(),
            fs=fake_fs,
            region_code=controller_cls.region_code(),
        )
        with patch.object(GcsfsFactory, "build", new=mock_build_fs):
            with patch.object(
                direct_ingest_raw_table_migration_collector,
                "regions",
                new=regions_module,
            ):
                controller = controller_cls(
                    ingest_bucket_path=gcsfs_direct_ingest_bucket_for_region(
                        region_code=controller_cls.region_code(),
                        system_level=SystemLevel.for_region_code(
                            controller_cls.region_code(),
                            is_direct_ingest=True,
                        ),
                        ingest_instance=ingest_instance,
                        project_id="recidiviz-xxx",
                    )
                )
                controller.csv_reader = GcsfsCsvReader(fake_fs)
                controller.raw_file_import_manager.csv_reader = controller.csv_reader

                task_manager.set_controller(controller)
                fake_fs.test_set_delegate(
                    DirectIngestFakeGCSFileSystemDelegate(
                        controller, can_start_ingest=can_start_ingest
                    )
                )
                return controller


def ingest_args_for_fixture_file(
    controller: BaseDirectIngestController,
    filename: str,
    should_normalize: bool = True,
) -> GcsfsIngestArgs:
    file_path = path_for_fixture_file(
        controller,
        filename,
        should_normalize,
        file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
    )
    if not isinstance(file_path, GcsfsFilePath):
        raise ValueError(f"Unexpected type [{file_path}]")
    return GcsfsIngestArgs(
        ingest_time=datetime.datetime.now(),
        file_path=file_path,
    )


def path_for_fixture_file(
    controller: BaseDirectIngestController,
    filename: str,
    should_normalize: bool,
    file_type: Optional[GcsfsDirectIngestFileType],
    dt: Optional[datetime.datetime] = None,
) -> GcsfsFilePath:
    return path_for_fixture_file_in_test_gcs_directory(
        bucket_path=controller.ingest_bucket_path,
        filename=filename,
        should_normalize=should_normalize,
        file_type=file_type,
        dt=dt,
    )


def path_for_fixture_file_in_test_gcs_directory(
    *,
    bucket_path: GcsfsBucketPath,
    filename: str,
    should_normalize: bool,
    file_type: Optional[GcsfsDirectIngestFileType],
    dt: Optional[datetime.datetime] = None,
) -> GcsfsFilePath:
    file_path_str = filename

    if should_normalize:
        if not file_type:
            raise ValueError("Expected file_type for path normalization but got None")
        file_path_str = to_normalized_unprocessed_file_path(
            original_file_path=file_path_str, file_type=file_type, dt=dt
        )

    file_path = GcsfsFilePath.from_directory_and_file_name(
        dir_path=bucket_path,
        file_name=file_path_str,
    )
    if not isinstance(file_path, GcsfsFilePath):
        raise ValueError(
            f"Expected type GcsfsFilePath, found {type(file_path)} for path: {file_path.abs_path()}"
        )
    return file_path


def add_paths_with_tags_and_process(
    test_case: unittest.TestCase,
    controller: BaseDirectIngestController,
    file_tags: List[str],
    pre_normalized_file_type: Optional[GcsfsDirectIngestFileType] = None,
    unexpected_tags: List[str] = None,
) -> None:
    """Runs a test that queues files for all the provided file tags, waits
    for the controller to finish processing everything, then makes sure that
    all files not in |unexpected_tags| have been moved to storage.
    """
    add_paths_with_tags(controller, file_tags, pre_normalized_file_type)
    process_task_queues(test_case, controller, file_tags, unexpected_tags)


def add_paths_with_tags(
    controller: BaseDirectIngestController,
    file_tags: List[str],
    pre_normalized_file_type: Optional[GcsfsDirectIngestFileType] = None,
) -> None:
    if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
        raise ValueError(
            f"Controller fs must have type "
            f"FakeGCSFileSystem. Found instead "
            f"type [{type(controller.fs.gcs_file_system)}]"
        )

    for file_tag in file_tags:
        file_path = path_for_fixture_file(
            controller,
            f"{file_tag}.csv",
            should_normalize=bool(pre_normalized_file_type),
            file_type=pre_normalized_file_type,
        )
        # Only get a fixture path if it is a file, if it is a directory leave it as None
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path,
            region_code=controller.region_code(),
        )
        time.sleep(0.05)


def process_task_queues(
    test_case: unittest.TestCase,
    controller: BaseDirectIngestController,
    file_tags: List[str],
    unexpected_tags: List[str] = None,
) -> None:
    if unexpected_tags is None:
        unexpected_tags = []

    run_task_queues_to_empty(controller)
    check_all_paths_processed(test_case, controller, file_tags, unexpected_tags)


def check_all_paths_processed(
    test_case: unittest.TestCase,
    controller: BaseDirectIngestController,
    file_tags: List[str],
    unexpected_tags: List[str],
) -> None:
    """Checks that all non-directory paths with expected tags have been
    processed and moved to storage.
    """

    if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
        raise ValueError(
            f"Controller fs must have type "
            f"FakeGCSFileSystem. Found instead "
            f"type [{type(controller.fs.gcs_file_system)}]"
        )

    file_tags_processed = set()
    for path in controller.fs.gcs_file_system.all_paths:
        if isinstance(path, GcsfsDirectoryPath):
            continue

        file_tag = filename_parts_from_path(path).file_tag

        if file_tag not in unexpected_tags:
            # Test all expected files have been moved to storage
            test_case.assertTrue(
                path.abs_path().startswith(
                    controller.storage_directory_path.abs_path()
                ),
                f"{path} has not been moved to correct storage directory",
            )

            file_tags_processed.add(filename_parts_from_path(path).file_tag)
        else:
            test_case.assertTrue(path.file_name.startswith("unprocessed"))

    # Test that each expected file tag has been processed
    test_case.assertEqual(
        file_tags_processed, set(file_tags).difference(set(unexpected_tags))
    )


def run_task_queues_to_empty(controller: BaseDirectIngestController) -> None:
    """Runs task queues until they are all empty."""
    if isinstance(controller.cloud_task_manager, FakeAsyncDirectIngestCloudTaskManager):
        controller.cloud_task_manager.wait_for_all_tasks_to_run()
    elif isinstance(
        controller.cloud_task_manager, FakeSynchronousDirectIngestCloudTaskManager
    ):
        tm = controller.cloud_task_manager
        queue_args = (controller.region, controller.ingest_instance)
        while (
            tm.get_scheduler_queue_info(*queue_args).size()
            or tm.get_process_job_queue_info(*queue_args).size()
            or tm.get_raw_data_import_queue_info(controller.region).size()
            or tm.get_ingest_view_export_queue_info(*queue_args).size()
        ):
            if tm.get_raw_data_import_queue_info(controller.region).size():
                tm.test_run_next_raw_data_import_task()
                tm.test_pop_finished_raw_data_import_task()
            if tm.get_ingest_view_export_queue_info(*queue_args).size():
                tm.test_run_next_ingest_view_export_task()
                tm.test_pop_finished_ingest_view_export_task()
            if tm.get_scheduler_queue_info(*queue_args).size():
                tm.test_run_next_scheduler_task()
                tm.test_pop_finished_scheduler_task()
            if tm.get_process_job_queue_info(*queue_args).size():
                tm.test_run_next_process_job_task()
                tm.test_pop_finished_process_job_task()
    else:
        raise ValueError(
            f"Unexpected type for cloud task manager: "
            f"[{type(controller.cloud_task_manager)}]"
        )
