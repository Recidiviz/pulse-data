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
import os

from gcsfs import GCSFileSystem
from mock import Mock, create_autospec, patch

from recidiviz.common.cloud_task_factory import CloudTaskFactoryInterface
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_types import \
    IngestArgsType
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_factory import GcsfsFactory
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.regions import Region


def create_mock_gcsfs() -> GCSFileSystem:
    def mock_open(file_path):
        path, file_name = os.path.split(file_path)

        mock_fp = Mock()
        fixture_contents = fixtures.as_string(path, file_name)
        mock_fp.read.return_value = bytes(fixture_contents, 'utf-8')
        mock_fp_context_manager = Mock()
        mock_fp_context_manager.__enter__ = Mock(return_value=mock_fp)
        mock_fp_context_manager.__exit__ = Mock(return_value=False)
        return mock_fp_context_manager

    mock_fs = create_autospec(spec=GCSFileSystem)
    mock_fs.open = mock_open
    return mock_fs


def build_controller_for_tests(
        fs, controller_cls) -> GcsfsDirectIngestController:
    def mock_build_fs():
        return fs

    with patch('recidiviz.ingest.direct.controllers.'
               'base_direct_ingest_controller.CloudTaskFactory') \
            as mock_task_factory_cls:
        task_factory = TestCloudTaskFactory()
        mock_task_factory_cls.return_value = task_factory
        with patch.object(GcsfsFactory, 'build', new=mock_build_fs):
            controller = controller_cls()
            task_factory.set_controller(controller)
            return controller


class TestCloudTaskFactory(CloudTaskFactoryInterface):
    def __init__(self):
        self.controller: BaseDirectIngestController = None

    def set_controller(self, controller: GcsfsDirectIngestController):
        self.controller = controller

    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgsType):
        self.controller.run_ingest_job(ingest_args)

    def create_direct_ingest_scheduler_queue_task(self,
                                                  region: Region,
                                                  ingest_args: IngestArgsType,
                                                  delay_sec: int):
        # Do nothing for now - hard to simulate an async wait that relinquishes
        # the main thread - this relies on the tests queueing another job to
        # schedule.
        pass
