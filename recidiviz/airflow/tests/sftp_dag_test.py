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
"""Unit test to test the SFTP DAG."""
import datetime
import re
from unittest.mock import patch

from airflow.models import DAG, BaseOperator, DagBag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import text
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.dag_registry import get_sftp_dag_id
from recidiviz.airflow.dags.sftp.metadata import (
    END_SFTP,
    SFTP_ENABLED_YAML_CONFIG,
    START_SFTP,
    TASK_RETRIES,
    get_configs_bucket,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.airflow.tests.operators.sftp.sftp_test_utils import (
    FakeSftpDownloadDelegateFactory,
)
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    fake_operator_from_callable,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.local_file_paths import filepath_relative_to_caller
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.types import assert_type

TESTING_PROJECT_ID = "recidiviz-testing"


class TestSftpSequencingDag(AirflowIntegrationTest):
    """Tests the task sequencing for the SFTP DAG."""

    project_id = GCP_PROJECT_STAGING
    SFTP_DAG_ID = get_sftp_dag_id(GCP_PROJECT_STAGING)

    def setUp(self) -> None:
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": GCP_PROJECT_STAGING,
            },
        )
        self.environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()

    def test_import(self) -> None:
        """Just tests that the sftp_dag file can be imported"""
        # Need to import calculation_dag inside test suite so environment variables are
        # set before importing, otherwise sftp_dag will raise an Error and not
        # import.

        # pylint: disable=C0415 import-outside-toplevel
        from recidiviz.airflow.dags.sftp_dag import dag  # pylint: disable=unused-import

        # If nothing fails, this test passes

    def test_start_sftp_upstream_of_state_specific_tasks(self) -> None:
        """Tests that the `start_sftp` check happens before the state task group
        starts executing."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z]",
            include_downstream=False,
            include_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        self.assertIn(START_SFTP, upstream_tasks)

    def test_check_config_upstream_of_remote_file_discovery_tasks(self) -> None:
        """Tests that the `check_config` check happens before we discover remote file
        discovery tasks."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z]\.remote_file_discovery.find_sftp_files_to_download",
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        for task_id in upstream_tasks:
            self.assertTrue("check_config" in task_id)

    def test_mark_files_discovered_upstream_of_gather_discovered_files(self) -> None:
        """Tests that gathering all discovered files occurs after marking new files discovered."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z].*gather_discovered.*files",
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        for task_id in upstream_tasks:
            self.assertRegex(task_id, r"US_[A-Z][A-Z].*mark_.*files_discovered")

    def test_mark_files_discovered_upstream_of_mark_files_loaded(self) -> None:
        """Tests that marking files uploaded or downloaded occurs after marking them discovered."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z].*mark_.*files_.*loaded",
            include_downstream=False,
            include_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            for task_id in task.upstream_task_ids:
                if re.match(r"US_[A-Z][A-Z].*mark_.*files_.*discovered", task_id):
                    upstream_tasks.add(task_id)

        self.assertNotEqual(0, len(upstream_tasks))

    def test_sftp_and_gcs_operators_have_retries(self) -> None:
        task_types_with_retries = [
            "remote_file_download.download_sftp_files",
            "remote_file_download.post_process_downloaded_files",
            "ingest_ready_file_upload.upload_files_to_ingest_bucket",
        ]
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        for task in dag.tasks:
            for task_type in task_types_with_retries:
                if task_type in task.task_id:
                    self.assertEqual(TASK_RETRIES, task.retries)

    def test_acquire_permission_happens_at_start_of_ingest_file_upload(self) -> None:
        """Tests that releasing permission happens at before ingest file upload"""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z].ingest_ready_file_upload",
            include_downstream=True,
            include_upstream=False,
            include_direct_upstream=False,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        # makes sure that acquire_permission_for_ingest_file_upload happens before we upload
        # to the ingest bucket
        for task in state_specific_tasks_dag.roots:
            assert any(
                "acquire_permission_for_ingest_file_upload" in task_id
                for task_id in task.downstream_task_ids
            )

    def test_release_permission_happens_if_we_uploaded(self) -> None:
        """Tests that the last thing in each branch is release_permission_for_ingest_file_upload,
        if we uploaded files.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=END_SFTP,
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        for task_id in upstream_tasks:
            self.assertTrue(
                "release_permission_for_ingest_file_upload" in task_id
                or "do_not_upload_ingest_ready_files" in task_id
            )

    def test_all_done_operators_downstream_of_check_if_ingest_ready_files_have_stabilized_are_directly_downstream(
        self,
    ) -> None:
        """Test that makes sure that all ALL_DONE operators downstream of the
        `check_if_ingest_ready_files_have_stabilized` task are directly downstream
        (and therefore can be skipped if their branch is not chosen)
        """

        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        downstream_of_stabilized = dag.partial_subset(
            task_ids_or_regex="check_if_ingest_ready_files_have_stabilized",
            include_downstream=True,
            include_upstream=False,
            include_direct_upstream=False,
        )
        self.assertNotEqual(0, len(downstream_of_stabilized.task_ids))

        downstream_all_done_tasks = set()
        directly_downstream_of_check_if_ingest_ready_files_have_stabilized_tasks = set()
        for task in downstream_of_stabilized.tasks:
            if task.trigger_rule == TriggerRule.ALL_DONE:
                if "check_if_ingest_ready_files_have_stabilized" in task.task_id:
                    directly_downstream_of_check_if_ingest_ready_files_have_stabilized_tasks.update(
                        task.downstream_task_ids
                    )
                elif END_SFTP not in task.task_id:
                    downstream_all_done_tasks.add(task.task_id)

        if (
            missing := downstream_all_done_tasks
            - directly_downstream_of_check_if_ingest_ready_files_have_stabilized_tasks
        ):
            raise ValueError(
                f"Found tasks downstream of `check_if_ingest_ready_files_have_stabilized` "
                f"that have an ALL_DONE trigger rule but are not directly downstream of "
                f"check_if_ingest_ready_files_have_stabilized: [{missing}]"
            )


class TestSFTPIntegrationTests(AirflowIntegrationTest):
    """Integration tests for the SFTP DAG."""

    metas = [OperationsBase]
    conn_id = "operations_postgres_conn_id"
    project_id = TESTING_PROJECT_ID
    sftp_dag = get_sftp_dag_id(TESTING_PROJECT_ID)

    def setUp(self) -> None:
        super().setUp()

        # env mocks

        self.fake_gcsfs = FakeGCSFileSystem()
        self.gcsfs_patchers = [
            patch(
                call_site,
                return_value=self.fake_gcsfs,
            )
            for call_site in [
                "recidiviz.airflow.dags.utils.gcsfs_utils.get_gcsfs_from_hook",
                "recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator.get_gcsfs_from_hook",
                "recidiviz.airflow.dags.operators.sftp.gcs_transform_file_operator.get_gcsfs_from_hook",
                "recidiviz.airflow.dags.operators.sftp.filter_invalid_gcs_files.get_gcsfs_from_hook",
            ]
        ]
        for patcher in self.gcsfs_patchers:
            patcher.start()

        self.delegate_factory_patchers = [
            patch(call_location, FakeSftpDownloadDelegateFactory)
            for call_location in [
                "recidiviz.airflow.dags.sftp_dag.SftpDownloadDelegateFactory",
                "recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator.SftpDownloadDelegateFactory",
                "recidiviz.airflow.dags.operators.sftp.find_sftp_files_operator.SftpDownloadDelegateFactory",
                "recidiviz.airflow.dags.operators.sftp.gcs_transform_file_operator.SftpDownloadDelegateFactory",
                "recidiviz.airflow.dags.sftp.filter_downloaded_files_sql_query_generator.SftpDownloadDelegateFactory",
            ]
        ]
        for patcher in self.delegate_factory_patchers:  # type: ignore
            patcher.start()

        self.cloud_sql_db_hook_patcher = patch(
            "recidiviz.airflow.dags.operators.cloud_sql_query_operator.CloudSQLDatabaseHook"
        )
        self.mock_cloud_sql_db_hook = self.cloud_sql_db_hook_patcher.start()
        self.mock_cloud_sql_db_hook().get_database_hook.return_value = PostgresHook(
            self.conn_id
        )

        # external system mocks

        self.find_sftp_files_patcher = patch(
            "recidiviz.airflow.dags.sftp_dag.FindSftpFilesOperator",
            side_effect=fake_operator_from_callable(self._sftp_call),
        )
        self.find_sftp_files_mock = self.find_sftp_files_patcher.start()

        self.sftp_to_gcs_operator_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator.RecidivizSFTPHook",
        )
        self.sftp_to_gcs_operator_patcher.start()

        # mocks the call to super().execute()
        self.sftp_gcs_to_gcs_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.gcs_to_gcs_operator.GCSToGCSOperator.execute"
        )
        self.sftp_gcs_to_gcs_patcher.start()

        # datetime mocks

        self.remote_discovery_time_patch = patch(
            "recidiviz.airflow.dags.sftp.mark_remote_files_discovered_sql_query_generator.postgres_formatted_current_datetime_utc_str"
        )
        self.remote_discovery_time_mock = self.remote_discovery_time_patch.start()

        self.remote_download_time_patch = patch(
            "recidiviz.airflow.dags.sftp.mark_remote_files_downloaded_sql_query_generator.postgres_formatted_current_datetime_utc_str"
        )
        self.remote_download_time_mock = self.remote_download_time_patch.start()

        self.ingest_discovery_time_patch = patch(
            "recidiviz.airflow.dags.sftp.mark_ingest_ready_files_discovered_sql_query_generator.postgres_formatted_current_datetime_utc_str"
        )
        self.ingest_discovery_time_mock = self.ingest_discovery_time_patch.start()

        self.ingest_upload_time_patch = patch(
            "recidiviz.airflow.dags.sftp.mark_ingest_ready_files_uploaded_sql_query_generator.postgres_formatted_current_datetime_utc_str"
        )
        self.ingest_upload_time_mock = self.ingest_upload_time_patch.start()

    def tearDown(self) -> None:
        super().tearDown()
        for patcher in self.gcsfs_patchers:
            patcher.stop()
        for patcher in self.delegate_factory_patchers:  # type: ignore
            patcher.stop()
        self.find_sftp_files_patcher.stop()
        self.cloud_sql_db_hook_patcher.stop()
        self.sftp_to_gcs_operator_patcher.stop()
        self.sftp_gcs_to_gcs_patcher.stop()
        self.remote_discovery_time_patch.stop()
        self.remote_download_time_patch.stop()
        self.ingest_discovery_time_patch.stop()
        self.ingest_upload_time_patch.stop()

    def _create_dag(self) -> DAG:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.sftp_dag import sftp_dag

        return sftp_dag()

    def _sftp_call(self, _base: BaseOperator, _context: Context) -> list:
        return self.found_sftp_files

    def _set_up_env(
        self,
        *,
        sftp_files_to_find: list | None = None,
        enabled_config: str = "all_enabled_config",
        remote_discovery_time: datetime.datetime | None = None,
        remote_download_time: datetime.datetime | None = None,
        ingest_discovery_time: datetime.datetime | None = None,
        ingest_upload_time: datetime.datetime | None = None,
    ) -> None:
        """Sets values for environment mocks."""
        self.found_sftp_files = sftp_files_to_find or []

        self.remote_discovery_time = remote_discovery_time or datetime.datetime(
            2024, 1, 1, 1, tzinfo=datetime.UTC
        )
        self.remote_discovery_time_mock.return_value = (
            postgres_formatted_datetime_with_tz(self.remote_discovery_time)
        )

        self.remote_download_time = remote_download_time or datetime.datetime(
            2024, 1, 1, 2, tzinfo=datetime.UTC
        )
        self.remote_download_time_mock.return_value = (
            postgres_formatted_datetime_with_tz(self.remote_download_time)
        )

        self.ingest_discovery_time = ingest_discovery_time or datetime.datetime(
            2024, 1, 1, 3, tzinfo=datetime.UTC
        )

        self.ingest_discovery_time_mock.return_value = (
            postgres_formatted_datetime_with_tz(self.ingest_discovery_time)
        )

        self.ingest_upload_time = ingest_upload_time or datetime.datetime(
            2024, 1, 1, 4, tzinfo=datetime.UTC
        )

        self.ingest_upload_time_mock.return_value = postgres_formatted_datetime_with_tz(
            self.ingest_upload_time
        )

        self.fake_gcsfs.test_add_path(
            path=GcsfsFilePath.from_directory_and_file_name(
                dir_path=get_configs_bucket(TESTING_PROJECT_ID),
                file_name=SFTP_ENABLED_YAML_CONFIG,
            ),
            local_path=filepath_relative_to_caller(
                f"{enabled_config}.yaml", "fixtures/sftp", caller_depth=2
            ),
        )

    def test_no_files(self) -> None:

        with Session(bind=self.engine) as session:
            dag = self._create_dag()
            self._set_up_env()
            result = self.run_dag_test(
                dag,
                session=session,
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    # no files to download, we skip downloading & marking
                    r"remote_file_download\.download_sftp_files",
                    r"remote_file_download\.post_process_downloaded_files",
                    r"remote_file_download\.mark_remote_files_downloaded",
                    # the entire ingest ready file upload stage
                    r"ingest_ready_file_upload\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_discover_files(self) -> None:

        file_paths = [
            {
                "remote_file_path": "/recidiviz/fake-file-1.txt",
                "sftp_timestamp": 1731283716,
            },
            {
                "remote_file_path": "/recidiviz/fake-file-2.txt",
                "sftp_timestamp": 1731283717,
            },
            {
                "remote_file_path": "/recidiviz/fake-file-3.txt",
                "sftp_timestamp": 1731283718,
            },
        ]

        with Session(bind=self.engine) as session:
            dag = self._create_dag()
            self._set_up_env(sftp_files_to_find=file_paths)
            result = self.run_dag_test(
                dag,
                session=session,
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    # we had files to download
                    r"remote_file_download\.do_not_mark_remote_files_downloaded",
                    # we found files, but they aren't stable
                    r"ingest_ready_file_upload\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # validate correct persistence layer

            remote_file_to_expected_post_processed_name = {
                "/recidiviz/fake-file-1.txt": "2024-11-11T00:08:36:000000/recidiviz/fake-file-1.txt",
                "/recidiviz/fake-file-2.txt": "2024-11-11T00:08:37:000000/recidiviz/fake-file-2.txt",
                "/recidiviz/fake-file-3.txt": "2024-11-11T00:08:38:000000/recidiviz/fake-file-3.txt",
            }

            # we've found & downloaded the files
            remote_file_metadata = {
                metadata[0]: metadata
                for metadata in session.execute(
                    text(
                        "SELECT remote_file_path, sftp_timestamp, file_discovery_time, file_download_time FROM direct_ingest_sftp_remote_file_metadata"
                    )
                )
            }

            for file_path in file_paths:
                assert file_path["remote_file_path"] in remote_file_metadata
                file_metadata = remote_file_metadata[file_path["remote_file_path"]]
                assert file_metadata[1] == file_path["sftp_timestamp"]
                assert file_metadata[2] == self.remote_discovery_time
                assert file_metadata[3] == self.remote_download_time

            # they are ingest ready but not yet downloaded
            ingest_file_metadata = {
                metadata[0]: metadata
                for metadata in session.execute(
                    text(
                        "SELECT remote_file_path, post_processed_normalized_file_path, file_discovery_time, file_upload_time FROM direct_ingest_sftp_ingest_ready_file_metadata"
                    )
                )
            }

            for file_path in file_paths:
                remote_file_path: str = assert_type(file_path["remote_file_path"], str)
                assert remote_file_path in ingest_file_metadata
                file_metadata = ingest_file_metadata[remote_file_path]
                assert (
                    file_metadata[1]
                    == remote_file_to_expected_post_processed_name[remote_file_path]
                )
                assert file_metadata[2] == self.ingest_discovery_time
                assert file_metadata[3] is None

            # locks
            assert not list(
                session.execute(
                    text("SELECT * FROM direct_ingest_raw_data_resource_lock")
                )
            )

    def test_discover_and_download_files(self) -> None:

        file_paths = [
            {
                "remote_file_path": "/recidiviz/fake-file-1.txt",
                "sftp_timestamp": 1731283716,
            },
            {
                "remote_file_path": "/recidiviz/fake-file-2.txt",
                "sftp_timestamp": 1731283717,
            },
            {
                "remote_file_path": "/recidiviz/fake-file-3.txt",
                "sftp_timestamp": 1731283718,
            },
        ]

        remote_file_to_expected_post_processed_name = {
            "/recidiviz/fake-file-1.txt": "2024-11-11T00:08:36:000000/recidiviz/fake-file-1.txt",
            "/recidiviz/fake-file-2.txt": "2024-11-11T00:08:37:000000/recidiviz/fake-file-2.txt",
            "/recidiviz/fake-file-3.txt": "2024-11-11T00:08:38:000000/recidiviz/fake-file-3.txt",
        }

        with Session(bind=self.engine) as session:
            dag = self._create_dag()
            self._set_up_env(sftp_files_to_find=file_paths)

            result = self.run_dag_test(
                dag,
                session=session,
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    # we had files to download
                    r"remote_file_download\.do_not_mark_remote_files_downloaded",
                    # we found files, but they aren't stable
                    r"ingest_ready_file_upload\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # we've found & downloaded the files
            remote_file_metadata = {
                metadata[0]: metadata
                for metadata in session.execute(
                    text(
                        "SELECT remote_file_path, sftp_timestamp, file_discovery_time, file_download_time FROM direct_ingest_sftp_remote_file_metadata"
                    )
                )
            }

            # they are ingest ready, not yet downloaded
            ingest_file_metadata = {
                metadata[0]: metadata
                for metadata in session.execute(
                    text(
                        "SELECT remote_file_path, post_processed_normalized_file_path, file_discovery_time, file_upload_time FROM direct_ingest_sftp_ingest_ready_file_metadata"
                    )
                )
            }

            for file_path in file_paths:
                remote_file_path: str = assert_type(file_path["remote_file_path"], str)

                # remote file
                assert remote_file_path in remote_file_metadata
                remote_file = remote_file_metadata[remote_file_path]
                assert remote_file[1] == file_path["sftp_timestamp"]
                assert remote_file[2] == self.remote_discovery_time
                assert remote_file[3] == self.remote_download_time

                # ingest ready
                assert remote_file_path in ingest_file_metadata
                ingest_file = ingest_file_metadata[remote_file_path]
                assert (
                    ingest_file[1]
                    == remote_file_to_expected_post_processed_name[remote_file_path]
                )
                assert ingest_file[2] == self.ingest_discovery_time
                assert ingest_file[3] is None

            # locks
            assert not list(
                session.execute(
                    text("SELECT * FROM direct_ingest_raw_data_resource_lock")
                )
            )

            # here, we run the DAG with the same set of files found -- since we dedup
            # on remote_file_path and sftp_timestamp, we should not re-download the
            # files and instead determine we have properly discovered all files and
            # then move onto the file upload stage of the dag
            self._set_up_env(sftp_files_to_find=file_paths)

            result = self.run_dag_test(
                dag,
                session=session,
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    # we had no files to download this time
                    r"remote_file_download\.download_sftp_files",
                    r"remote_file_download\.post_process_downloaded_files",
                    r"remote_file_download\.mark_remote_files_downloaded",
                    # files are stable!
                    r"do_not_upload_ingest_ready_files",
                    # we uploaded files
                    r"do_not_mark_ingest_ready_files_uploaded",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # we've found & downloaded the files
            remote_file_metadata = {
                metadata[0]: metadata
                for metadata in session.execute(
                    text(
                        "SELECT remote_file_path, sftp_timestamp, file_discovery_time, file_download_time FROM direct_ingest_sftp_remote_file_metadata"
                    )
                )
            }
            # we've downloaded them
            ingest_file_metadata = {
                metadata[0]: metadata
                for metadata in session.execute(
                    text(
                        "SELECT remote_file_path, post_processed_normalized_file_path, file_discovery_time, file_upload_time FROM direct_ingest_sftp_ingest_ready_file_metadata"
                    )
                )
            }

            for file_path in file_paths:
                remote_file_path = assert_type(file_path["remote_file_path"], str)

                # remote files
                assert remote_file_path in remote_file_metadata
                remote_file = remote_file_metadata[file_path["remote_file_path"]]
                assert remote_file[1] == file_path["sftp_timestamp"]
                assert remote_file[2] == self.remote_discovery_time
                assert remote_file[3] == self.remote_download_time

                # ingest files
                assert remote_file_path in ingest_file_metadata
                ingest_file = ingest_file_metadata[remote_file_path]
                assert (
                    ingest_file[1]
                    == remote_file_to_expected_post_processed_name[remote_file_path]
                )
                assert ingest_file[2] == self.ingest_discovery_time
                assert ingest_file[3] == self.ingest_upload_time

            # locks
            locks = list(
                session.execute(
                    text(
                        "SELECT lock_actor, lock_resource, released, region_code FROM direct_ingest_raw_data_resource_lock order by region_code"
                    )
                )
            )

            assert locks[0][3] == "US_LL"
            assert locks[1][3] == "US_XX"
            for lock in locks:
                assert lock[0] == "PROCESS"
                assert lock[1] == "BUCKET"
                assert lock[2] is True
