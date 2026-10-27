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
"""Tests for run_sandbox_dataflow_pipeline_utils.py"""
import contextlib
import datetime
import io
import json
import os
import unittest
from unittest import mock

import httplib2
from googleapiclient.errors import HttpError

from recidiviz.pipelines.ingest.activity.pipeline_parameters import (
    IngestPipelineParameters,
)
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    LaunchedDataflowJob,
    check_can_poll_sandbox_dataflow_jobs,
    get_template_path,
    get_upload_pipeline_docker_image_cloud_build_config_path,
    run_sandbox_dataflow_pipeline,
    wait_for_sandbox_dataflow_jobs,
)

_MODULE = "recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils"

_INGEST_JOB = LaunchedDataflowJob(
    job_id="2026-10-09_08_00_00-1111111111111111111",
    job_name="my-prefix-us-xx-ingest-test",
    region="us-east1",
)
_METRIC_JOB = LaunchedDataflowJob(
    job_id="2026-10-09_08_30_00-2222222222222222222",
    job_name="my-prefix-us-xx-population-span-metrics-test",
    region="us-east1",
)


def _fake_dataflow_client(states_by_job_id: dict[str, list[str]]) -> mock.MagicMock:
    """Returns a stand-in for the Dataflow API client whose jobs.get reports, for each
    job id, the given states in order."""
    remaining_states = {
        job_id: list(states) for job_id, states in states_by_job_id.items()
    }

    def get(*, projectId: str, location: str, jobId: str) -> mock.MagicMock:
        del projectId, location
        request = mock.MagicMock()
        request.execute.return_value = {"currentState": remaining_states[jobId].pop(0)}
        return request

    client = mock.MagicMock()
    client.projects.return_value.locations.return_value.jobs.return_value.get.side_effect = (
        get
    )
    return client


class TestWaitForSandboxDataflowJobs(unittest.TestCase):
    """Tests for wait_for_sandbox_dataflow_jobs()"""

    def setUp(self) -> None:
        self.auth_patcher = mock.patch(
            f"{_MODULE}.google.auth.default", return_value=(mock.MagicMock(), None)
        )
        self.auth_patcher.start()
        self.sleep_patcher = mock.patch(f"{_MODULE}.time.sleep")
        self.mock_sleep = self.sleep_patcher.start()
        self.build_patcher = mock.patch(f"{_MODULE}.build")
        self.mock_build = self.build_patcher.start()

    def tearDown(self) -> None:
        self.auth_patcher.stop()
        self.sleep_patcher.stop()
        self.build_patcher.stop()

    def test_returns_once_every_job_is_done_and_prints_each_state_change(self) -> None:
        self.mock_build.return_value = _fake_dataflow_client(
            {
                _INGEST_JOB.job_id: ["JOB_STATE_RUNNING", "JOB_STATE_DONE"],
                _METRIC_JOB.job_id: [
                    "JOB_STATE_QUEUED",
                    "JOB_STATE_RUNNING",
                    "JOB_STATE_DONE",
                ],
            }
        )
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            wait_for_sandbox_dataflow_jobs(
                project_id="recidiviz-456", jobs=[_INGEST_JOB, _METRIC_JOB]
            )

        self.assertEqual(
            "Job my-prefix-us-xx-ingest-test: JOB_STATE_RUNNING\n"
            "Job my-prefix-us-xx-population-span-metrics-test: JOB_STATE_QUEUED\n"
            "Job my-prefix-us-xx-ingest-test: JOB_STATE_DONE\n"
            "Job my-prefix-us-xx-population-span-metrics-test: JOB_STATE_RUNNING\n"
            "Job my-prefix-us-xx-population-span-metrics-test: JOB_STATE_DONE\n",
            printed.getvalue(),
        )
        # Three polls: the second and third each follow one sleep.
        self.assertEqual(2, self.mock_sleep.call_count)

    def test_raises_naming_the_job_that_ended_without_succeeding(self) -> None:
        self.mock_build.return_value = _fake_dataflow_client(
            {
                _INGEST_JOB.job_id: ["JOB_STATE_DONE"],
                _METRIC_JOB.job_id: ["JOB_STATE_FAILED"],
            }
        )
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(
            RuntimeError,
            r"^Dataflow job \[my-prefix-us-xx-population-span-metrics-test\] ended in "
            r"state \[JOB_STATE_FAILED\]\. See https://console\.cloud\.google\.com/"
            r"dataflow/jobs/us-east1/2026-10-09_08_30_00-2222222222222222222"
            r"\?project=recidiviz-456$",
        ):
            wait_for_sandbox_dataflow_jobs(
                project_id="recidiviz-456", jobs=[_INGEST_JOB, _METRIC_JOB]
            )

    def test_raises_with_the_access_fix_when_the_account_cannot_read_jobs(
        self,
    ) -> None:
        client = mock.MagicMock()
        client.projects.return_value.locations.return_value.jobs.return_value.get.return_value.execute.side_effect = HttpError(
            resp=httplib2.Response({"status": "403"}),
            content=b'{"error": {"message": "Permission denied on resource project"}}',
        )
        self.mock_build.return_value = client
        with self.assertRaisesRegex(
            RuntimeError,
            r"^Cannot read Dataflow job \[my-prefix-us-xx-ingest-test\] in "
            r"\[recidiviz-456\]: the account needs the dataflow\.jobs\.get permission "
            r"on the project\. Ask the Data Platform team for access that includes "
            r"it, then rerun\. Dataflow said: \[.*\]$",
        ):
            wait_for_sandbox_dataflow_jobs(
                project_id="recidiviz-456", jobs=[_INGEST_JOB]
            )

    def test_raises_when_jobs_outlast_the_wait_limit(self) -> None:
        self.mock_build.return_value = _fake_dataflow_client(
            {_INGEST_JOB.job_id: ["JOB_STATE_RUNNING", "JOB_STATE_RUNNING"]}
        )
        with mock.patch(
            f"{_MODULE}._DATAFLOW_JOB_WAIT_LIMIT", datetime.timedelta(seconds=1)
        ), mock.patch(
            # Clock readings: when the wait starts, after the first check, after the
            # second.
            f"{_MODULE}.time.time",
            side_effect=[0, 0, 5],
        ), contextlib.redirect_stdout(
            io.StringIO()
        ), self.assertRaisesRegex(
            RuntimeError,
            r"^Dataflow jobs \['my-prefix-us-xx-ingest-test'\] are still running after "
            r"0:00:01\. Check them in the console at https://console\.cloud\.google\.com/"
            r"dataflow/jobs\?project=recidiviz-456$",
        ):
            wait_for_sandbox_dataflow_jobs(
                project_id="recidiviz-456", jobs=[_INGEST_JOB]
            )


class TestCheckCanPollSandboxDataflowJobs(unittest.TestCase):
    """Tests for check_can_poll_sandbox_dataflow_jobs()"""

    def setUp(self) -> None:
        mock.patch(
            f"{_MODULE}.google.auth.default", return_value=(mock.MagicMock(), None)
        ).start()
        self.mock_build = mock.patch(f"{_MODULE}.build").start()
        self.mock_test_permissions = (
            self.mock_build.return_value.projects.return_value.testIamPermissions
        )

    def tearDown(self) -> None:
        mock.patch.stopall()

    def test_passes_when_the_account_can_read_jobs(self) -> None:
        self.mock_test_permissions.return_value.execute.return_value = {
            "permissions": ["dataflow.jobs.get"]
        }
        check_can_poll_sandbox_dataflow_jobs("recidiviz-456")
        self.mock_test_permissions.assert_called_once_with(
            resource="recidiviz-456", body={"permissions": ["dataflow.jobs.get"]}
        )

    def test_raises_with_the_access_fix_when_it_cannot(self) -> None:
        self.mock_test_permissions.return_value.execute.return_value = {}
        with self.assertRaisesRegex(
            RuntimeError,
            r"^Cannot read Dataflow jobs in \[recidiviz-456\]: the account needs the "
            r"dataflow\.jobs\.get permission on the project\. Ask the Data Platform "
            r"team for access that includes it, then rerun\.$",
        ):
            check_can_poll_sandbox_dataflow_jobs("recidiviz-456")


class TestRunSandboxDataflowPipeline(unittest.TestCase):
    """Tests for run_sandbox_dataflow_pipeline()"""

    def setUp(self) -> None:
        self.params = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline=INGEST_PIPELINE_NAME,
            region="us-east1",
            input_dataset_overrides_json=json.dumps({}),
            output_sandbox_prefix="my_prefix",
            sandbox_username="my_username",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        self.patchers = [
            mock.patch(f"{_MODULE}.pipeline_cls_for_pipeline_name"),
            mock.patch(
                f"{_MODULE}.get_all_reference_query_input_datasets_for_pipeline",
                return_value=set(),
            ),
            mock.patch(
                f"{_MODULE}.get_template_path", return_value="/tmp/template.json"
            ),
            mock.patch(f"{_MODULE}.run_command"),
            mock.patch(f"{_MODULE}.fetch_google_auth_token", return_value="token"),
        ]
        for patcher in self.patchers:
            patcher.start()
        self.post_patcher = mock.patch(f"{_MODULE}.requests.post")
        self.mock_post = self.post_patcher.start()

    def tearDown(self) -> None:
        for patcher in self.patchers:
            patcher.stop()
        self.post_patcher.stop()

    def test_returns_the_job_dataflow_created(self) -> None:
        self.mock_post.return_value = mock.MagicMock(
            ok=True,
            json=mock.MagicMock(
                return_value={
                    "job": {
                        "id": "2026-10-09_08_00_00-1111111111111111111",
                        "name": self.params.job_name,
                    }
                }
            ),
        )
        with contextlib.redirect_stdout(io.StringIO()):
            launched = run_sandbox_dataflow_pipeline(self.params, skip_build=True)
        self.assertEqual(
            LaunchedDataflowJob(
                job_id="2026-10-09_08_00_00-1111111111111111111",
                job_name=self.params.job_name,
                region="us-east1",
            ),
            launched,
        )

    def test_returns_none_and_prints_the_response_when_dataflow_refuses_the_launch(
        self,
    ) -> None:
        self.mock_post.return_value = mock.MagicMock(
            ok=False, text='{"error": {"code": 429, "message": "Quota exceeded"}}'
        )
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            launched = run_sandbox_dataflow_pipeline(self.params, skip_build=True)

        self.assertIsNone(launched)
        # The launch body printed before this is long and not under test.
        self.assertTrue(
            printed.getvalue().endswith(
                "Job launch failed..\n"
                '{"error": {"code": 429, "message": "Quota exceeded"}}\n'
            )
        )


class FlexPipelineSandboxPathsTest(unittest.TestCase):
    """Tests to verify paths used in the sandbox script exist."""

    def test_template_paths_exist_for_all_pipelines(self) -> None:
        for pipeline_type in ["metrics", "supplemental", "ingest", "identity_ingest"]:
            template_path = get_template_path(pipeline_type)
            self.assertEqual(os.path.isfile(template_path), True)

    def test_cloudbuild_files_exist(self) -> None:
        cloudbuild_path = get_upload_pipeline_docker_image_cloud_build_config_path()
        self.assertEqual(os.path.isfile(cloudbuild_path), True)
