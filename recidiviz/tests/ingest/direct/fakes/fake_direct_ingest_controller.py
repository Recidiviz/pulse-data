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
"""Helpers for building a test-only version of the BaseDirectIngestController."""
from types import ModuleType
from typing import Dict, List, Type

import attr
from mock import Mock, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.ingest.direct.ingest_view_materialization.file_based_materializer_delegate import (
    FileBasedMaterializerDelegate,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_gating_context import (
    IngestViewMaterializationGatingContext,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializer,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer_delegate import (
    IngestViewMaterializerDelegate,
)
from recidiviz.ingest.direct.raw_data import direct_ingest_raw_table_migration_collector
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileImportManager,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    GcsfsIngestViewExportArgs,
    IngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.tests.cloud_storage.fake_gcs_file_system import (
    FakeGCSFileSystem,
    FakeGCSFileSystemDelegate,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct.fakes.fake_async_direct_ingest_cloud_task_manager import (
    FakeAsyncDirectIngestCloudTaskManager,
)
from recidiviz.tests.ingest.direct.fakes.fake_instance_ingest_view_contents import (
    FakeInstanceIngestViewContents,
)
from recidiviz.tests.ingest.direct.fakes.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    _get_fixture_for_direct_ingest_path,
)
from recidiviz.utils.regions import Region


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
                        is_datetime=False,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
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
                        is_datetime=False,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
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
                        is_datetime=False,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
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
                        is_datetime=False,
                        is_pii=False,
                    )
                ],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
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
                import_chunk_size_rows=10,
                infer_columns_from_config=False,
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


class FakeDirectIngestPreProcessedIngestViewCollector(
    DirectIngestPreProcessedIngestViewCollector
):
    def __init__(self, region: Region, controller_ingest_view_rank_list: List[str]):
        super().__init__(region, controller_ingest_view_rank_list)

    def collect_view_builders(self) -> List[DirectIngestPreProcessedIngestViewBuilder]:
        builders = [
            DirectIngestPreProcessedIngestViewBuilder(
                region=self.region.region_code,
                ingest_view_name=tag,
                view_query_template=f"SELECT * FROM {{{tag}}}",
                order_by_cols="",
            )
            for tag in self.controller_ingest_view_rank_list
        ]

        builders.append(
            DirectIngestPreProcessedIngestViewBuilder(
                ingest_view_name="gatedTagNotInTagsList",
                region=self.region.region_code,
                view_query_template="SELECT * FROM {tagBasicData} LEFT OUTER JOIN {tagBasicData} USING (col);",
                order_by_cols="",
            )
        )

        return builders


# TODO(#11424): Delete this once all states have been migrated to BQ-based ingest view
#  materialization.
MATERIALIZATION_CONFIG_YAML = """
states:
- US_ND:
   PRIMARY: FILE
   SECONDARY: FILE
- US_MO:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
- US_ID:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
- US_PA:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
- US_TN:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
- US_ME:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
- US_MI:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
- US_CO:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
- US_XX:
   PRIMARY: FILE
   # TODO(#9717): Flip this to 'BQ' to test BQ materialization functionality in
   #  controller tests.
   SECONDARY: FILE
"""


class _MockBigQueryClientForControllerTests:
    """A fake BQ client that only wraps a test FS that other mocks can access."""

    def __init__(self, fs: FakeGCSFileSystem) -> None:
        self.fs = fs


class FakeIngestViewMaterializer(IngestViewMaterializer):
    """A fake implementation of IngestViewMaterializer for use in tests."""

    def __init__(
        self,
        *,
        region: Region,
        ingest_instance: DirectIngestInstance,
        delegate: IngestViewMaterializerDelegate,
        big_query_client: _MockBigQueryClientForControllerTests,
        view_collector: BigQueryViewCollector[
            DirectIngestPreProcessedIngestViewBuilder
        ],
        launched_ingest_views: List[str],
    ):
        self.region = region
        self.fs = big_query_client.fs
        self.delegate = delegate
        self.processed_args: List[IngestViewMaterializationArgs] = []

        self.ingest_instance = ingest_instance
        self.view_collector = view_collector
        self.launched_ingest_views = launched_ingest_views

    def materialize_view_for_args(
        self, ingest_view_materialization_args: IngestViewMaterializationArgs
    ) -> bool:
        if ingest_view_materialization_args in self.processed_args:
            return False

        self.delegate.prepare_for_job(ingest_view_materialization_args)

        if isinstance(self.delegate, FileBasedMaterializerDelegate):
            if not isinstance(
                ingest_view_materialization_args, GcsfsIngestViewExportArgs
            ):
                raise ValueError(
                    f"Unexpected args type [{ingest_view_materialization_args}]"
                )
            export_path = FileBasedMaterializerDelegate.generate_output_path(
                ingest_view_materialization_args
            )
            data_local_path = _get_fixture_for_direct_ingest_path(
                export_path, region_code=self.region.region_code.upper()
            )
            if not data_local_path:
                raise ValueError("No support yet for actually running export queries")
            self.fs.test_add_path(export_path, data_local_path)
        else:
            # TODO(#9717): Will need to replicate this logic for new materialization
            raise NotImplementedError(
                "TODO(#9717): Will need to fake materialization logic for BQ materialization."
            )

        self.delegate.mark_job_complete(ingest_view_materialization_args)
        self.processed_args.append(ingest_view_materialization_args)

        return True

    def get_materialized_ingest_views(self) -> List[str]:
        return [arg.ingest_view_name for arg in self.processed_args]


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging"))
def build_fake_direct_ingest_controller(
    controller_cls: Type[BaseDirectIngestController],
    ingest_instance: DirectIngestInstance,
    run_async: bool,
    can_start_ingest: bool = True,
    regions_module: ModuleType = fake_regions_module,
) -> BaseDirectIngestController:
    """Builds an instance of |controller_cls| for use in tests with several internal
    classes mocked properly.
    """
    fake_fs = FakeGCSFileSystem()

    def mock_build_fs() -> FakeGCSFileSystem:
        return fake_fs

    # TODO(#11424): Delete this line once all states have been migrated to BQ-based
    #  ingest view materialization.
    fake_fs.upload_from_string(
        path=IngestViewMaterializationGatingContext.gating_config_path(),
        contents=MATERIALIZATION_CONFIG_YAML,
        content_type="text/yaml",
    )

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
        f"{BaseDirectIngestController.__module__}.IngestViewMaterializerImpl",
        FakeIngestViewMaterializer,
    ), patch(
        f"{BaseDirectIngestController.__module__}.DirectIngestPreProcessedIngestViewCollector",
        view_collector_cls,
    ), patch(
        f"{BaseDirectIngestController.__module__}.InstanceIngestViewContentsImpl",
        FakeInstanceIngestViewContents,
    ):
        task_manager = (
            FakeAsyncDirectIngestCloudTaskManager()
            if run_async
            else FakeSynchronousDirectIngestCloudTaskManager()
        )
        mock_task_factory_cls.return_value = task_manager
        mock_big_query_client_cls.return_value = _MockBigQueryClientForControllerTests(
            fs=fake_fs
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
