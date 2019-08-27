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
"""Tests for GcsfsDirectIngestController."""
import datetime
import json
import unittest
from typing import List

from mock import patch, Mock

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.serialization import attr_to_json_dict, \
    datetime_to_serializable, serializable_to_datetime, attr_from_json_dict
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_controller_for_tests, add_paths_with_tags_and_process, \
    ingest_args_for_fixture_file, FakeDirectIngestGCSFileSystem
from recidiviz.tests.ingest.direct.\
    fake_synchronous_direct_ingest_cloud_task_manager import \
    FakeSynchronousDirectIngestCloudTaskManager
from recidiviz.tests.utils.fake_region import TEST_STATE_REGION, \
    TEST_COUNTY_REGION


class StateTestGcsfsDirectIngestController(CsvGcsfsDirectIngestController):
    def __init__(self,
                 ingest_directory_path: str,
                 storage_directory_path: str):
        super().__init__(TEST_STATE_REGION.region_code,
                         SystemLevel.STATE,
                         ingest_directory_path,
                         storage_directory_path)

    def _get_file_tag_rank_list(self) -> List[str]:
        return ['tagA', 'tagB', 'tagC']


class CountyTestGcsfsDirectIngestController(CsvGcsfsDirectIngestController):
    def __init__(self,
                 ingest_directory_path: str,
                 storage_directory_path: str):
        super().__init__(TEST_COUNTY_REGION.region_code,
                         SystemLevel.COUNTY,
                         ingest_directory_path,
                         storage_directory_path)

    def _get_file_tag_rank_list(self) -> List[str]:
        return ['tagA', 'tagB']


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
class TestGcsfsDirectIngestController(unittest.TestCase):
    """Tests for GcsfsDirectIngestController."""

    FIXTURE_PATH_PREFIX = 'direct/controllers'

    def run_async_file_order_test_for_controller_cls(self, controller_cls):
        """Writes all expected files to the mock fs, then kicks the controller
        and ensures that all jobs are run to completion in the proper order."""

        controller = build_controller_for_tests(controller_cls,
                                                self.FIXTURE_PATH_PREFIX,
                                                run_async=True)

        # pylint:disable=protected-access
        file_tags = list(
            reversed(sorted(controller._get_file_tag_rank_list())))

        add_paths_with_tags_and_process(self, controller, file_tags)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_STATE_REGION))
    def test_state_runs_files_in_order(self):
        self.run_async_file_order_test_for_controller_cls(
            StateTestGcsfsDirectIngestController)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_COUNTY_REGION))
    def test_county_runs_files_in_order(self):
        self.run_async_file_order_test_for_controller_cls(
            CountyTestGcsfsDirectIngestController)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_STATE_REGION))
    def test_state_unexpected_tag(self):
        controller = build_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        file_tags = ['tagA', 'Unexpected_Tag', 'tagB', 'tagC']
        unexpected_tags = ['Unexpected_Tag']

        add_paths_with_tags_and_process(
            self, controller, file_tags, unexpected_tags)

    def test_do_not_queue_same_job_twice(self):
        controller = build_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        args = ingest_args_for_fixture_file(controller,
                                            f'tagA.csv')
        args2 = ingest_args_for_fixture_file(controller,
                                             f'tagB.csv')
        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        controller.fs.test_add_path(args.file_path)
        controller.kick_scheduler(just_finished_job=False)

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        controller.fs.test_add_path(args2.file_path)
        controller.kick_scheduler(just_finished_job=False)

        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        # This is the task that got queued by kick_schedule()
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        # This is the task that got queued by finishing a job
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            1,
            task_manager.get_process_job_queue_info(controller.region).size())

    def test_next_schedule_runs_before_process_job_clears(self):
        controller = build_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        args = ingest_args_for_fixture_file(controller,
                                            f'tagA.csv')
        args2 = ingest_args_for_fixture_file(controller,
                                             f'tagB.csv')
        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        controller.fs.test_add_path(args.file_path)
        controller.fs.test_add_path(args2.file_path)

        controller.kick_scheduler(just_finished_job=False)

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        task_manager.test_run_next_process_job_task()
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_process_job_task()
        task_manager.test_pop_finished_scheduler_task()

        # We should have still queued a process job, even though the last
        # one hadn't run when schedule executes
        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())

    def test_do_not_schedule_more_than_one_delayed_scheduler_job(self):
        controller = build_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        args_with_wait_time = ingest_args_for_fixture_file(controller,
                                                           f'tagB.csv')

        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        controller.fs.test_add_path(args_with_wait_time.file_path)

        # Kick the scheduler 5 times
        for _ in range(5):
            controller.kick_scheduler(just_finished_job=False)

        # This will queue 5 immediate jobs
        for _ in range(5):
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        # But after running all those jobs, there should only be one job in
        # the queue.
        self.assertEqual(
            1,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())

    def test_serialize_gcsfs_ingest_args(self):
        now = datetime.datetime.now()

        str_now = datetime_to_serializable(now)
        now_converted = serializable_to_datetime(str_now)

        self.assertTrue(now, now_converted)

        args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path='foo/bar.csv',
        )

        args_dict = attr_to_json_dict(args)
        serialized = json.dumps(args_dict).encode()
        args_dict = json.loads(serialized)
        result_args = attr_from_json_dict(args_dict)
        self.assertEqual(args, result_args)
