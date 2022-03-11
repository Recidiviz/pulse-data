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
"""Tests for IngestViewMaterializationGatingContext."""
from unittest import TestCase, mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_gating_context import (
    IngestViewMaterializationGatingContext,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

GATING_CONTEXT_PACKAGE_NAME = IngestViewMaterializationGatingContext.__module__


SIMPLE_CONFIG_YAML = """# Change values from FILE to BQ as instances are migrated
states:
- US_XX:
   PRIMARY: FILE
   SECONDARY: BQ
- US_YY:
   PRIMARY: FILE
   SECONDARY: FILE
- US_WW:
   PRIMARY: BQ
   SECONDARY: BQ
"""

BAD_GATING_CONFIG_YAML = """
states:
- US_XX:
   PRIMARY: BQ
   SECONDARY: FILE
"""


class TestIngestViewMaterializationGatingContext(TestCase):
    """Tests for IngestViewMaterializationGatingContext."""

    def setUp(self) -> None:
        self.mock_project_id = "recidiviz-456"
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.gcs_factory_patcher = mock.patch(
            f"{GATING_CONTEXT_PACKAGE_NAME}.GcsfsFactory.build"
        )
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_gcs

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.gcs_factory_patcher.stop()

    def set_config_yaml(self, contents: str) -> None:
        path = GcsfsFilePath.from_absolute_path(
            f"gs://{self.mock_project_id}-configs/bq_materialization_gating_config.yaml"
        )
        self.fake_gcs.upload_from_string(
            path=path, contents=contents, content_type="text/yaml"
        )

    def test_simple_config(self) -> None:
        # Arrange
        self.set_config_yaml(SIMPLE_CONFIG_YAML)

        # Act
        gating_context = IngestViewMaterializationGatingContext.load_from_gcs()

        # Assert
        self.assertFalse(
            gating_context.is_bq_ingest_view_materialization_enabled(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )
        )
        self.assertTrue(
            gating_context.is_bq_ingest_view_materialization_enabled(
                StateCode.US_XX, DirectIngestInstance.SECONDARY
            )
        )
        for ingest_instance in DirectIngestInstance:
            self.assertFalse(
                gating_context.is_bq_ingest_view_materialization_enabled(
                    StateCode.US_YY, ingest_instance
                )
            )
        for ingest_instance in DirectIngestInstance:
            self.assertTrue(
                gating_context.is_bq_ingest_view_materialization_enabled(
                    StateCode.US_WW, ingest_instance
                )
            )

    def test_config_primary_ungated_before_secondary(self) -> None:
        # Arrange
        self.set_config_yaml(BAD_GATING_CONFIG_YAML)

        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"State \[US_XX\] has the PRIMARY instance migrated to BQ materialization "
            r"before the SECONDARY instance.",
        ):
            _ = IngestViewMaterializationGatingContext.load_from_gcs()
