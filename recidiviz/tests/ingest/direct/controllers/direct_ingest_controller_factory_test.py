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
"""Tests for the DirectIngestControllerFactory."""
import unittest

from mock import Mock, patch

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.ingest.direct import templates
from recidiviz.ingest.direct.controllers import direct_ingest_controller_factory
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_dir_names,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import DirectIngestError
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.utils.fake_region import fake_region

CONTROLLER_FACTORY_PACKAGE_NAME = direct_ingest_controller_factory.__name__


class TestDirectIngestControllerFactory(unittest.TestCase):
    """Tests for the DirectIngestControllerFactory."""

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-456"
        )
        self.bq_client_patcher = patch("google.cloud.bigquery.Client")
        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.task_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")

        def mock_build_fs() -> FakeGCSFileSystem:
            return FakeGCSFileSystem()

        self.fs_patcher = patch.object(GcsfsFactory, "build", new=mock_build_fs)

        self.project_id_patcher.start()
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.task_client_patcher.start()
        self.fs_patcher.start()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.task_client_patcher.stop()
        self.fs_patcher.stop()

    def test_build_gcsfs_ingest_controller_all_regions(self) -> None:
        for region_code in get_existing_region_dir_names():
            for ingest_instance in DirectIngestInstance:
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=False,
                )

                self.assertIsNotNone(controller)
                self.assertIsInstance(controller, BaseDirectIngestController)
                self.assertEqual(ingest_instance, controller.ingest_instance)

    def test_build_gcsfs_ingest_controller_all_regions_do_not_allow_launched(
        self,
    ) -> None:
        for region_code in get_existing_region_dir_names():
            for ingest_instance in DirectIngestInstance:
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=True,
                )

                # Should still succeed for all controllers in the test environment
                self.assertIsNotNone(controller)
                self.assertIsInstance(controller, BaseDirectIngestController)
                self.assertEqual(ingest_instance, controller.ingest_instance)

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    @patch(
        f"{CONTROLLER_FACTORY_PACKAGE_NAME}.get_supported_direct_ingest_region_codes",
        Mock(return_value=["us_xx"]),
    )
    def test_build_throws_in_prod_region_only_launched_in_staging(
        self,
    ) -> None:
        mock_region = fake_region(
            region_code="us_xx",
            environment="staging",
            region_module=templates,
        )
        with patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region",
            Mock(return_value=mock_region),
        ):
            with self.assertRaisesRegex(
                DirectIngestError,
                r"^Bad environment \[production\] for region \[us_xx\].$",
            ):
                _ = DirectIngestControllerFactory.build(
                    region_code=mock_region.region_code,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                    allow_unlaunched=False,
                )

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="staging"),
    )
    @patch(
        f"{CONTROLLER_FACTORY_PACKAGE_NAME}.get_supported_direct_ingest_region_codes",
        Mock(return_value=["us_xx"]),
    )
    def test_build_succeeds_in_staging_region_launched_in_prod(self) -> None:
        mock_region = fake_region(
            region_code="us_xx",
            environment="production",
            region_module=templates,
        )
        with patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region",
            Mock(return_value=mock_region),
        ):
            controller = DirectIngestControllerFactory.build(
                region_code=mock_region.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY,
                allow_unlaunched=False,
            )
            self.assertIsNotNone(controller)
            self.assertIsInstance(controller, BaseDirectIngestController)
            self.assertEqual(DirectIngestInstance.PRIMARY, controller.ingest_instance)

    def test_build_for_unsupported_region_throws(self) -> None:
        with self.assertRaisesRegex(
            DirectIngestError,
            r"^Unsupported direct ingest region \[us_xx\] in project \[recidiviz-456\]$",
        ):
            _ = DirectIngestControllerFactory.build(
                region_code="us_xx",
                ingest_instance=DirectIngestInstance.PRIMARY,
                allow_unlaunched=False,
            )
