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

"""Tests for ingest/direct_control.py."""
import datetime
import json
import unittest
from unittest import mock

from flask import Flask
from mock import patch, create_autospec, Mock

from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import to_normalized_unprocessed_file_path, \
    DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.direct_ingest_raw_data_table_latest_view_updater import \
    DirectIngestRawDataTableLatestViewUpdater
from recidiviz.ingest.direct.controllers.direct_ingest_raw_update_cloud_task_manager import \
    DirectIngestRawUpdateCloudTaskManager
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsRawDataBQImportArgs, \
    GcsfsDirectIngestFileType, GcsfsIngestViewExportArgs
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    DirectIngestCloudTaskManager
from recidiviz.ingest.direct.direct_ingest_control import kick_all_schedulers
from recidiviz.ingest.direct.direct_ingest_region_utils import get_existing_region_dir_names
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.regions import Region

CONTROL_PACKAGE_NAME = direct_ingest_control.__name__


@patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
@patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
class TestDirectIngestControl(unittest.TestCase):
    """Tests for requests to the Direct Ingest API."""

    def setUp(self) -> None:
        app = Flask(__name__)
        app.register_blueprint(direct_ingest_control.direct_ingest_control)
        app.config['TESTING'] = True
        self.client = app.test_client()

        self.bq_client_patcher = patch('google.cloud.bigquery.Client')
        self.storage_client_patcher = patch('google.cloud.storage.Client')
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_schedule(self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        """Tests that the start operation chains together the correct calls."""

        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(environment='production', ingestor=mock_controller)
        mock_environment.return_value = 'production'

        region = 'us_nd'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/scheduler',
                                   query_string=request_args,
                                   headers=headers)
        self.assertEqual(200, response.status_code)

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)
        mock_controller.schedule_next_ingest_job_or_wait_if_necessary.assert_called_with(just_finished_job=False)
        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_schedule_diff_environment_in_production(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)

        region = 'us_nd'

        mock_region.return_value = fake_region(environment='staging',
                                               region_code=region,
                                               ingestor=mock_controller)

        request_args = {'region': region}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/scheduler',
                                   query_string=request_args,
                                   headers=headers)

        mock_controller.schedule_next_ingest_job_or_wait_if_necessary.assert_not_called()
        self.assertEqual(400, response.status_code)
        self.assertEqual(
            response.get_data().decode(),
            "Bad environment [production] for region [us_nd].")

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_start_diff_environment_in_staging(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'staging'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_controller)

        region = 'us_nd'
        request_args = {'region': region, 'just_finished_job': 'True'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/scheduler',
                                   query_string=request_args,
                                   headers=headers)
        self.assertEqual(200, response.status_code)

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)
        mock_controller.schedule_next_ingest_job_or_wait_if_necessary.assert_called_with(just_finished_job=True)

    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    def test_schedule_unsupported_region(self, mock_supported: mock.MagicMock) -> None:
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'just_finished_job': 'False'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/scheduler',
                                   query_string=request_args,
                                   headers=headers)
        self.assertEqual(400, response.status_code)
        self.assertTrue(
            response.get_data().decode().startswith("Unsupported direct ingest region [us_ca]"))

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    def test_schedule_unlaunched_region(
            self,
            mock_supported: mock.MagicMock,
            mock_region: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        mock_supported.return_value = ['us_nd', 'us_pa']

        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        request_args = {'region': 'us_nd', 'just_finished_job': 'False'}
        headers = {'X-Appengine-Cron': 'test-cron'}

        response = self.client.get('/scheduler',
                                   query_string=request_args,
                                   headers=headers)
        self.assertEqual(400, response.status_code)
        self.assertEqual(
            response.get_data().decode(),
            "Bad environment [production] for region [us_nd].")

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    def test_process_job(
            self,
            mock_supported: mock.MagicMock,
            mock_region: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        mock_supported.return_value = ['us_nd', 'us_pa']

        region_code = 'us_nd'

        mock_environment.return_value = 'staging'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        ingest_args = IngestArgs(datetime.datetime(year=2019, month=7, day=20))
        request_args = {
            'region': region_code,
        }
        body = {
            'cloud_task_args': ingest_args.to_serializable(),
            'args_type': 'IngestArgs',
        }
        body_encoded = json.dumps(body).encode()

        headers = {'X-Appengine-Cron': 'test-cron'}

        response = self.client.post('/process_job',
                                    query_string=request_args,
                                    headers=headers,
                                    data=body_encoded)
        self.assertEqual(200, response.status_code)
        mock_controller.run_ingest_job_and_kick_scheduler_on_completion.assert_called_with(ingest_args)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    def test_process_job_unlaunched_region(
            self,
            mock_supported: mock.MagicMock,
            mock_region: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        mock_supported.return_value = ['us_ca', 'us_pa']

        region_code = 'us_ca'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        ingest_args = IngestArgs(datetime.datetime(year=2019, month=7, day=20))
        request_args = {
            'region': region_code,
        }
        body = {
            'cloud_task_args': ingest_args.to_serializable(),
            'args_type': 'IngestArgs',
        }
        body_encoded = json.dumps(body).encode()

        headers = {'X-Appengine-Cron': 'test-cron'}

        response = self.client.post('/process_job',
                                    query_string=request_args,
                                    headers=headers,
                                    data=body_encoded)
        self.assertEqual(400, response.status_code)
        self.assertEqual(response.get_data().decode(), "Bad environment [production] for region [us_ca].")

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_no_start_ingest(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='production',
            ingestor=mock_controller)

        path = GcsfsFilePath.from_absolute_path('bucket-us-nd/Elite_Offenders.csv')

        request_args = {
            'region': region_code,
            'bucket': path.bucket_name,
            'relative_file_path': path.blob_name,
            'start_ingest': 'false',
        }
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/handle_direct_ingest_file',
                                   query_string=request_args,
                                   headers=headers)

        mock_controller.handle_file.assert_called_with(path, False)

        # Even though the region isn't supported, we don't crash
        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_start_ingest(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(region_code=region_code,
                                               environment='production',
                                               ingestor=mock_controller)
        path = GcsfsFilePath.from_absolute_path(
            'bucket-us-nd/elite_offenders.csv')

        request_args = {
            'region': region_code,
            'bucket': path.bucket_name,
            'relative_file_path': path.blob_name,
            'start_ingest': 'True',
        }
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/handle_direct_ingest_file',
                                   query_string=request_args,
                                   headers=headers)

        mock_controller.handle_file.assert_called_with(path, True)

        # Even though the region isn't supported, we don't crash
        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_start_ingest_unsupported_region(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(region_code=region_code,
                                               environment='staging',
                                               ingestor=mock_controller)

        path = GcsfsFilePath.from_absolute_path(
            'bucket-us-nd/elite_offenders.csv')

        request_args = {
            'region': region_code,
            'bucket': path.bucket_name,
            'relative_file_path': path.blob_name,
            'start_ingest': 'False',
        }
        headers = {'X-Appengine-Cron': 'test-cron'}

        response = self.client.get('/handle_direct_ingest_file',
                                   query_string=request_args,
                                   headers=headers)

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)
        mock_controller.handle_file.assert_called_with(path, False)

        # Even though the region isn't supported, we don't crash - the
        # controller handles not starting ingest, and if it does by accident,
        # the actual schedule/process_job endpoints handle the unlaunched
        # region check.
        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_files_start_ingest(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(region_code=region_code,
                                               environment='production',
                                               ingestor=mock_controller)
        request_args = {
            'region': region_code,
            'can_start_ingest': 'True',
        }
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/handle_new_files',
                                   query_string=request_args,
                                   headers=headers)

        mock_controller.handle_new_files.assert_called_with(True)

        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_files_no_start_ingest(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        region_code = 'us_nd'

        mock_environment.return_value = 'staging'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(region_code=region_code,
                                               environment='staging',
                                               ingestor=mock_controller)
        request_args = {
            'region': region_code,
            'can_start_ingest': 'False',
        }
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/handle_new_files',
                                   query_string=request_args,
                                   headers=headers)

        mock_controller.handle_new_files.assert_called_with(False)

        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
    def test_normalize_file_path(
            self, mock_fs_factory: mock.MagicMock, mock_environment: mock.MagicMock) -> None:

        mock_environment.return_value = 'production'
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        path = GcsfsFilePath.from_absolute_path(
            'bucket-us-xx/file-tag.csv')

        mock_fs.test_add_path(path, local_path=None)

        request_args = {
            'bucket': path.bucket_name,
            'relative_file_path': path.blob_name,
        }

        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/normalize_raw_file_path',
                                   query_string=request_args,
                                   headers=headers)

        self.assertEqual(200, response.status_code)

        self.assertEqual(1, len(mock_fs.all_paths))
        registered_path = mock_fs.all_paths[0]
        if not isinstance(registered_path, GcsfsFilePath):
            self.fail(f'Unexpected type for path [{type(registered_path)}]')
        self.assertTrue(DirectIngestGCSFileSystem.is_normalized_file_path(registered_path))

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_new_files_no_start_ingest_in_production(
            self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock) -> None:
        """Tests that handle_new_files will run and rename files in unlaunched locations, but will not schedule a job to
        process any files."""
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(region_code=region_code, environment='staging', ingestor=mock_controller)
        request_args = {
            'region': region_code,
            'can_start_ingest': 'False',
        }
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/handle_new_files', query_string=request_args, headers=headers)

        mock_controller.schedule_next_ingest_job_or_wait_if_necessary.assert_not_called()
        mock_controller.run_ingest_job_and_kick_scheduler_on_completion.assert_not_called()
        mock_controller.handle_new_files.assert_called_with(False)

        self.assertEqual(200, response.status_code)

    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_ensure_all_file_paths_normalized(
            self,
            mock_get_region: mock.MagicMock,
            mock_environment: mock.MagicMock,
            mock_supported_region_codes: mock.MagicMock) -> None:

        fake_supported_regions = {
            'us_mo':
                fake_region(region_code='us_mo',
                            environment='staging',
                            ingestor=Mock()),
            'us_nd':
                fake_region(region_code='us_nd',
                            environment='production',
                            ingestor=Mock()),
        }

        mock_cloud_task_manager = create_autospec(DirectIngestCloudTaskManager)
        for region in fake_supported_regions.values():
            region.get_ingestor().__class__ = GcsfsDirectIngestController
            region.get_ingestor().cloud_task_manager.return_value = \
                mock_cloud_task_manager

        def fake_get_region(region_code: str, is_direct_ingest: bool) -> Region:
            if not is_direct_ingest:
                self.fail("is_direct_ingest is False")

            return fake_supported_regions[region_code]

        mock_get_region.side_effect = fake_get_region

        mock_supported_region_codes.return_value = fake_supported_regions.keys()

        mock_environment.return_value = 'staging'

        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/ensure_all_file_paths_normalized',
                                   query_string={},
                                   headers=headers)

        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch('recidiviz.ingest.direct.controllers.base_direct_ingest_controller.DirectIngestCloudTaskManagerImpl')
    def test_ensure_all_file_paths_normalized_actual_regions(
            self,
            mock_cloud_task_manager: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        with local_project_id_override('recidiviz-staging'):
            mock_environment.return_value = 'staging'
            mock_cloud_task_manager.return_value = create_autospec(DirectIngestCloudTaskManager)

            headers = {'X-Appengine-Cron': 'test-cron'}
            response = self.client.get('/ensure_all_file_paths_normalized',
                                       query_string={},
                                       headers=headers)

            self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    @patch('recidiviz.ingest.direct.direct_ingest_control.DirectIngestRawDataTableLatestViewUpdater')
    def test_update_raw_data_latest_views_for_state(
            self, mock_updater_fn: mock.MagicMock,
            mock_supported: mock.MagicMock,
            mock_region: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        with local_project_id_override('recidiviz-staging'):
            mock_supported.return_value = ['us_xx']
            mock_updater = create_autospec(DirectIngestRawDataTableLatestViewUpdater)
            mock_updater_fn.return_value = mock_updater

            region_code = 'us_xx'

            mock_environment.return_value = 'staging'
            mock_region.return_value = fake_region(
                region_code=region_code,
                environment='staging')

            request_args = {
                'region': region_code,
            }

            headers = {'X-Appengine-Cron': 'test-cron'}

            response = self.client.post('/update_raw_data_latest_views_for_state',
                                        query_string=request_args,
                                        headers=headers)
            mock_updater.update_views_for_state.assert_called_once()
            self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch(f"{CONTROL_PACKAGE_NAME}.DirectIngestRawUpdateCloudTaskManager")
    def test_create_raw_data_latest_view_update_tasks(
            self,
            mock_cloud_task_manager_fn: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        with local_project_id_override('recidiviz-staging'):
            mock_environment.return_value = 'staging'

            mock_cloud_task_manager = create_autospec(DirectIngestRawUpdateCloudTaskManager)
            mock_cloud_task_manager_fn.return_value = mock_cloud_task_manager

            headers = {'X-Appengine-Cron': 'test-cron'}
            response = self.client.post('/create_raw_data_latest_view_update_tasks',
                                        query_string={},
                                        headers=headers)

            self.assertEqual(200, response.status_code)

            expected_calls = [mock.call(region_code)
                              for region_code in get_existing_region_dir_names()]
            mock_cloud_task_manager.create_raw_data_latest_view_update_task.assert_has_calls(expected_calls)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    def test_raw_data_import(
            self,
            mock_supported: mock.MagicMock,
            mock_region: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        mock_supported.return_value = ['us_xx']

        region_code = 'us_xx'

        mock_environment.return_value = 'staging'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        import_args = GcsfsRawDataBQImportArgs(
            raw_data_file_path=GcsfsFilePath.from_absolute_path(
                to_normalized_unprocessed_file_path('bucket/raw_data_path.csv',
                                                    file_type=GcsfsDirectIngestFileType.RAW_DATA)))
        request_args = {
            'region': region_code,
        }
        body = {
            'cloud_task_args': import_args.to_serializable(),
            'args_type': 'GcsfsRawDataBQImportArgs',
        }
        body_encoded = json.dumps(body).encode()

        headers = {'X-Appengine-Cron': 'test-cron'}

        response = self.client.post('/raw_data_import',
                                    query_string=request_args,
                                    headers=headers,
                                    data=body_encoded)
        self.assertEqual(200, response.status_code)
        mock_controller.do_raw_data_import.assert_called_with(import_args)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    def test_ingest_view_export(
            self,
            mock_supported: mock.MagicMock,
            mock_region: mock.MagicMock,
            mock_environment: mock.MagicMock) -> None:
        mock_supported.return_value = ['us_xx']

        region_code = 'us_xx'

        mock_environment.return_value = 'staging'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='my_ingest_view',
            upper_bound_datetime_prev=datetime.datetime(2020, 4, 29),
            upper_bound_datetime_to_export=datetime.datetime(2020, 4, 30)
        )

        request_args = {
            'region': region_code,
        }
        body = {
            'cloud_task_args': export_args.to_serializable(),
            'args_type': 'GcsfsIngestViewExportArgs',
        }
        body_encoded = json.dumps(body).encode()

        headers = {'X-Appengine-Cron': 'test-cron'}

        response = self.client.post('/ingest_view_export',
                                    query_string=request_args,
                                    headers=headers,
                                    data=body_encoded)
        self.assertEqual(200, response.status_code)
        mock_controller.do_ingest_view_export.assert_called_with(export_args)

    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_kick_all_schedulers(
            self,
            mock_get_region: mock.MagicMock,
            mock_environment: mock.MagicMock,
            mock_supported_region_codes: mock.MagicMock) -> None:

        fake_supported_regions = {
            'us_mo':
                fake_region(region_code='us_mo',
                            environment='staging',
                            ingestor=Mock()),
            'us_nd':
                fake_region(region_code='us_nd',
                            environment='production',
                            ingestor=Mock()),
        }

        mock_cloud_task_manager = create_autospec(DirectIngestCloudTaskManager)
        for region in fake_supported_regions.values():
            region.get_ingestor().__class__ = GcsfsDirectIngestController
            region.get_ingestor().cloud_task_manager.return_value = \
                mock_cloud_task_manager

        def fake_get_region(region_code: str, is_direct_ingest: bool) -> Region:
            if not is_direct_ingest:
                self.fail("is_direct_ingest is False")

            return fake_supported_regions[region_code]

        mock_get_region.side_effect = fake_get_region

        mock_supported_region_codes.return_value = fake_supported_regions.keys()

        mock_environment.return_value = 'staging'

        kick_all_schedulers()

        mock_supported_region_codes.assert_called()
        for region in fake_supported_regions.values():
            region.get_ingestor().kick_scheduler.assert_called_once()
