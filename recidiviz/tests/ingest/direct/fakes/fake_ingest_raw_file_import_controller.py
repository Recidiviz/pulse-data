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
"""Helpers for building a test-only version of the IngestRawFileImportController."""
import os
from types import ModuleType
from typing import Dict, List, Optional, Set

import attr
from mock import Mock, create_autospec, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import (
    FakeGCSFileSystem,
    FakeGCSFileSystemDelegate,
)
from recidiviz.ingest.direct.controllers.ingest_raw_file_import_controller import (
    IngestRawFileImportController,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.raw_data import direct_ingest_raw_table_migration_collector
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct.fakes.fake_async_direct_ingest_cloud_task_manager import (
    FakeAsyncDirectIngestCloudTaskManager,
)
from recidiviz.tests.ingest.direct.fakes.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)


class DirectIngestFakeGCSFileSystemDelegate(FakeGCSFileSystemDelegate):
    def __init__(
        self, controller: IngestRawFileImportController, can_start_ingest: bool
    ):
        self.controller = controller
        self.can_start_ingest = can_start_ingest

    def on_file_added(self, path: GcsfsFilePath) -> None:
        # We can only handle a file if there is a rerun currently in progress.
        instance_is_running = (
            self.controller.ingest_instance == DirectIngestInstance.PRIMARY
            or self.controller.ingest_instance_status_manager.get_current_ingest_rerun_start_timestamp()
        )

        if (
            instance_is_running
            and path.bucket_path == self.controller.raw_data_bucket_path
        ):
            self.controller.handle_file(path, start_ingest=self.can_start_ingest)

    def on_file_delete(self, path: GcsfsFilePath) -> bool:
        # To simulate a raw file import happening twice, we do not want to delete the lock
        # otherwise, we delete the file.
        return not "someDuplicate" in path.file_name


@attr.s
class FakeDirectIngestRegionRawFileConfig(DirectIngestRegionRawFileConfig):
    def _read_configs_from_disk(self) -> Dict[str, DirectIngestRawFileConfig]:
        return {
            "tagFullyEmptyFile": DirectIngestRawFileConfig(
                file_tag="tagFullyEmptyFile",
                file_path="path/to/tagFullyEmptyFile.yaml",
                file_description="file description",
                data_classification=RawDataClassification.SOURCE,
                primary_key_cols=["mockKey"],
                columns=[
                    RawTableColumnInfo(
                        name="mockKey",
                        description="mockKey description",
                        field_type=RawTableColumnFieldType.STRING,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                no_valid_primary_keys=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
                table_relationships=[],
                update_cadence=RawDataFileUpdateCadence.WEEKLY,
            ),
            "tagHeadersNoContents": DirectIngestRawFileConfig(
                file_tag="tagHeadersNoContents",
                file_path="path/to/tagHeadersNoContents.yaml",
                file_description="file description",
                data_classification=RawDataClassification.SOURCE,
                primary_key_cols=["mockKey"],
                columns=[
                    RawTableColumnInfo(
                        name="mockKey",
                        description="mockKey description",
                        field_type=RawTableColumnFieldType.STRING,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                no_valid_primary_keys=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
                table_relationships=[],
                update_cadence=RawDataFileUpdateCadence.WEEKLY,
            ),
            "tagBasicData": DirectIngestRawFileConfig(
                file_tag="tagBasicData",
                file_path="path/to/tagBasicData.yaml",
                file_description="file description",
                data_classification=RawDataClassification.VALIDATION,
                primary_key_cols=["mockKey"],
                columns=[
                    RawTableColumnInfo(
                        name="mockKey",
                        description="mockKey description",
                        field_type=RawTableColumnFieldType.STRING,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                no_valid_primary_keys=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
                table_relationships=[],
                update_cadence=RawDataFileUpdateCadence.WEEKLY,
            ),
            "tagMoreBasicData": DirectIngestRawFileConfig(
                file_tag="tagMoreBasicData",
                file_path="path/to/tagMoreBasicData.yaml",
                file_description="file description",
                data_classification=RawDataClassification.VALIDATION,
                primary_key_cols=["mockKey"],
                columns=[
                    RawTableColumnInfo(
                        name="mockKey",
                        description="mockKey description",
                        field_type=RawTableColumnFieldType.STRING,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                no_valid_primary_keys=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
                table_relationships=[],
                update_cadence=RawDataFileUpdateCadence.WEEKLY,
            ),
            "tagWeDoNotIngest": DirectIngestRawFileConfig(
                file_tag="tagWeDoNotIngest",
                file_path="path/to/tagWeDoNotIngest.yaml",
                data_classification=RawDataClassification.SOURCE,
                file_description="file description",
                primary_key_cols=[],
                columns=[],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                no_valid_primary_keys=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
                table_relationships=[],
                update_cadence=RawDataFileUpdateCadence.WEEKLY,
            ),
        }


class FakeDirectIngestRawFileImportManager(DirectIngestRawFileImportManager):
    """Fake implementation of DirectIngestRawFileImportManager for tests."""

    def __init__(
        self,
        *,
        region: DirectIngestRegion,
        fs: DirectIngestGCSFileSystem,
        temp_output_directory_path: GcsfsDirectoryPath,
        big_query_client: BigQueryClient,
        csv_reader: GcsfsCsvReader,
        instance: DirectIngestInstance,
    ):
        super().__init__(
            region=region,
            fs=fs,
            temp_output_directory_path=temp_output_directory_path,
            big_query_client=big_query_client,
            region_raw_file_config=FakeDirectIngestRegionRawFileConfig(
                region.region_code
            ),
            csv_reader=csv_reader,
            instance=instance,
        )
        self.imported_paths: List[GcsfsFilePath] = []

    def import_raw_file_to_big_query(
        self, path: GcsfsFilePath, file_metadata: DirectIngestRawFileMetadata
    ) -> None:
        self.imported_paths.append(path)


class _MockBigQueryClientForControllerTests:
    """A fake BQ client that only wraps a test FS that other mocks can access."""

    def __init__(self, fs: FakeGCSFileSystem) -> None:
        self.fs = fs


class FakeIngestRawFileImportController(IngestRawFileImportController):
    """Base class for test direct ingest controllers used in this file."""

    def __init__(
        self,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        region_module_override: Optional[ModuleType],
    ) -> None:
        super().__init__(state_code, ingest_instance, region_module_override)
        self.local_paths: Set[str] = set()

    def has_temp_paths_in_disk(self) -> bool:
        for path in self.local_paths:
            if os.path.exists(path):
                return True
        return False


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging"))
def build_fake_ingest_raw_file_import_controller(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    initial_statuses: List[DirectIngestStatus],
    run_async: bool,
    can_start_ingest: bool = True,
    regions_module: ModuleType = fake_regions_module,
    shared_task_manager: Optional[FakeSynchronousDirectIngestCloudTaskManager] = None,
) -> FakeIngestRawFileImportController:
    """Builds an instance of |controller_cls| for use in tests with several internal
    classes mocked properly.
    """
    fake_fs = FakeGCSFileSystem()

    def mock_build_fs() -> FakeGCSFileSystem:
        return fake_fs

    def mock_build_status_manager(
        region_code: str,
        ingest_instance: DirectIngestInstance,
    ) -> DirectIngestInstanceStatusManager:
        status_manager = DirectIngestInstanceStatusManager(
            region_code=region_code,
            ingest_instance=ingest_instance,
        )
        for status in initial_statuses:
            status_manager.add_instance_status(status)
        return status_manager

    with patch(
        f"{IngestRawFileImportController.__module__}.DirectIngestCloudTaskQueueManagerImpl"
    ) as mock_task_factory_cls, patch(
        f"{IngestRawFileImportController.__module__}.BigQueryClientImpl"
    ) as mock_big_query_client_cls, patch(
        f"{IngestRawFileImportController.__module__}.DirectIngestRawFileImportManager",
        FakeDirectIngestRawFileImportManager,
    ), patch(
        f"{IngestRawFileImportController.__module__}.DirectIngestInstanceStatusManager",
        mock_build_status_manager,
    ):
        task_manager = (
            shared_task_manager
            if shared_task_manager
            else (
                FakeAsyncDirectIngestCloudTaskManager()
                if run_async
                else FakeSynchronousDirectIngestCloudTaskManager()
            )
        )
        mock_task_factory_cls.return_value = task_manager
        mock_big_query_client_cls.return_value = _MockBigQueryClientForControllerTests(
            fs=fake_fs
        )
        with patch.object(GcsfsFactory, "build", new=mock_build_fs), patch.object(
            direct_ingest_raw_table_migration_collector,
            "regions",
            new=regions_module,
        ):
            controller = FakeIngestRawFileImportController(
                state_code=state_code,
                ingest_instance=ingest_instance,
                region_module_override=regions_module,
            )

            task_manager.set_controller(ingest_instance, controller)
            if not shared_task_manager:
                for instance in DirectIngestInstance:
                    if instance != ingest_instance:
                        mock_controller = create_autospec(IngestRawFileImportController)
                        mock_controller.ingest_instance = instance
                        task_manager.set_controller(instance, mock_controller)

            fake_fs.test_set_delegate(
                DirectIngestFakeGCSFileSystemDelegate(
                    controller, can_start_ingest=can_start_ingest
                )
            )
            return controller
