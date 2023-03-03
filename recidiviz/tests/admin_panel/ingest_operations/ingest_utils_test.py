# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

"""Tests for admin panel ingest utilities"""


from mock import ANY, MagicMock, call, create_autospec, patch

from recidiviz.admin_panel.ingest_operations.ingest_utils import (
    import_raw_files_to_bq_sandbox,
)
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_controller import (
    FakeDirectIngestRegionRawFileConfig,
)
from recidiviz.tests.utils.fake_region import fake_region


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch(
    "recidiviz.admin_panel.ingest_operations.ingest_utils.get_direct_ingest_region",
    MagicMock(
        return_value=fake_region(
            region_code=StateCode.US_XX.value.lower(),
            environment="staging",
            region_module=fake_regions,
        )
    ),
)
@patch(
    "recidiviz.admin_panel.ingest_operations.ingest_utils.DirectIngestRawFileImportManager",
)
@patch(
    "recidiviz.admin_panel.ingest_operations.ingest_utils.update_raw_data_table_schema"
)
def test_import_raw_files_to_bq_sandbox(
    mock_update_schema: MagicMock,
    mock_import_manager_cls: MagicMock,
) -> None:
    # Arrange
    big_query_client = create_autospec(BigQueryClient)
    gcsfs = create_autospec(GCSFileSystem)
    mock_import_manager = mock_import_manager_cls.return_value
    mock_import_manager.region_raw_file_config = FakeDirectIngestRegionRawFileConfig(
        "US_XX"
    )

    path1 = GcsfsFilePath(
        "bar-bucket",
        "unprocessed_2019-08-12T00:00:00:000000_raw_tagBasicData.csv",
    )
    path2 = GcsfsFilePath(
        "bar-bucket",
        "unprocessed_2019-08-12T00:00:00:000000_raw_tagMoreBasicData.csv",
    )
    gcsfs.ls_with_blob_prefix.return_value = [path1, path2]

    # Act
    import_raw_files_to_bq_sandbox(
        state_code=StateCode.US_XX,
        sandbox_dataset_prefix="foo",
        source_bucket=GcsfsBucketPath("bar-bucket"),
        file_tag_filters=None,
        allow_incomplete_configs=False,
        big_query_client=big_query_client,
        gcsfs=gcsfs,
    )

    # Assert
    big_query_client.create_dataset_if_necessary.assert_called()
    mock_update_schema.assert_has_calls(
        [
            call(
                state_code=StateCode.US_XX,
                instance=DirectIngestInstance.PRIMARY,
                raw_file_tag="tagBasicData",
                big_query_client=big_query_client,
                sandbox_dataset_prefix="foo",
            ),
            call(
                state_code=StateCode.US_XX,
                instance=DirectIngestInstance.PRIMARY,
                raw_file_tag="tagMoreBasicData",
                big_query_client=big_query_client,
                sandbox_dataset_prefix="foo",
            ),
        ]
    )
    mock_import_manager.import_raw_file_to_big_query.assert_has_calls(
        [call(path1, ANY), call(path2, ANY)]
    )
