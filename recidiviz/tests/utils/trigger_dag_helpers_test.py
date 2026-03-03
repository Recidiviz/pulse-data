# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for trigger_dag_helpers."""
from unittest.mock import MagicMock, patch

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.trigger_dag_helpers import (
    _execute_and_poll,
    trigger_calculation_dag,
    trigger_dag_run,
    trigger_raw_data_import_dag,
    wait_for_dag_task_success,
)


def _make_exec_response(
    error: str = "",
    execution_id: str = "exec-123",
    pod: str = "pod-abc",
    pod_namespace: str = "default",
) -> MagicMock:
    response = MagicMock()
    response.error = error
    response.execution_id = execution_id
    response.pod = pod
    response.pod_namespace = pod_namespace
    return response


def _make_poll_response(
    output_lines: list[str],
    output_end: bool,
    exit_code: int = 0,
    exit_error: str = "",
) -> MagicMock:
    response = MagicMock()
    lines = []
    for i, content in enumerate(output_lines):
        line = MagicMock()
        line.content = content
        line.line_number = i + 1
        lines.append(line)
    response.output = lines
    response.output_end = output_end
    response.exit_info.exit_code = exit_code
    response.exit_info.error = exit_error
    return response


def test_execute_and_poll_success() -> None:
    # Arrange
    client = MagicMock()
    client.execute_airflow_command.return_value = _make_exec_response()
    client.poll_airflow_command.return_value = _make_poll_response(
        output_lines=["some output"], output_end=True
    )

    # Act
    result = _execute_and_poll(client, "env-path", "dags", "trigger", ["my_dag"])

    # Assert
    assert result == ["some output"]
    client.poll_airflow_command.assert_called_once()


def test_execute_and_poll_multiple_polls() -> None:
    # Arrange
    client = MagicMock()
    client.execute_airflow_command.return_value = _make_exec_response()
    client.poll_airflow_command.side_effect = [
        _make_poll_response(output_lines=["line 1"], output_end=False),
        _make_poll_response(output_lines=["line 2"], output_end=True),
    ]

    # Act
    with patch("time.sleep"):
        result = _execute_and_poll(client, "env-path", "dags", "trigger", ["my_dag"])

    # Assert
    assert result == ["line 1", "line 2"]
    assert client.poll_airflow_command.call_count == 2
    calls = client.poll_airflow_command.call_args_list
    assert calls[0].kwargs["request"]["next_line_number"] == 1
    assert calls[1].kwargs["request"]["next_line_number"] == 2


def test_execute_and_poll_execute_error() -> None:
    # Arrange
    client = MagicMock()
    client.execute_airflow_command.return_value = _make_exec_response(
        error="something went wrong"
    )

    # Act / Assert
    with pytest.raises(ValueError, match="something went wrong"):
        _execute_and_poll(client, "env-path", "dags", "trigger", ["my_dag"])

    client.poll_airflow_command.assert_not_called()


def test_execute_and_poll_nonzero_exit_code() -> None:
    # Arrange
    client = MagicMock()
    client.execute_airflow_command.return_value = _make_exec_response()
    client.poll_airflow_command.return_value = _make_poll_response(
        output_lines=["error output"],
        output_end=True,
        exit_code=1,
        exit_error="dag not found",
    )

    # Act / Assert
    with pytest.raises(ValueError, match="exit code 1"):
        _execute_and_poll(client, "env-path", "dags", "trigger", ["my_dag"])


def test_execute_and_poll_timeout() -> None:
    # Arrange
    client = MagicMock()
    client.execute_airflow_command.return_value = _make_exec_response()
    client.poll_airflow_command.return_value = _make_poll_response(
        output_lines=[], output_end=False
    )

    # Act / Assert
    with patch("time.sleep"), patch(
        "time.time", side_effect=[0, 0, 9999]
    ), pytest.raises(TimeoutError, match="Timed out"):
        _execute_and_poll(client, "env-path", "dags", "trigger", ["my_dag"])


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.EnvironmentsClient")
@patch("recidiviz.utils.trigger_dag_helpers._execute_and_poll")
def test_trigger_dag_run_returns_run_id(
    mock_execute_and_poll: MagicMock,
    _mock_client_class: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    # Act
    run_id = trigger_dag_run("my_dag", conf={"key": "value"})

    # Assert
    assert run_id.startswith("manual__")
    call_args = mock_execute_and_poll.call_args
    assert call_args.kwargs["parameters"] == [
        "my_dag",
        "--conf",
        '{"key": "value"}',
        "--run-id",
        run_id,
    ]


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.EnvironmentsClient")
@patch("recidiviz.utils.trigger_dag_helpers._execute_and_poll")
def test_wait_for_dag_task_success_immediate(
    mock_execute_and_poll: MagicMock,
    _mock_client_class: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    # Arrange
    mock_execute_and_poll.return_value = ["success"]

    # Act / Assert (no exception)
    wait_for_dag_task_success(
        "my_dag",
        "manual__2024-01-01T00:00:00+00:00",
        "update_schemas",
        timeout_seconds=300,
    )
    mock_execute_and_poll.assert_called_once_with(
        client=_mock_client_class.return_value,
        environment=_mock_client_class.return_value.environment_path.return_value,
        command="tasks",
        subcommand="state",
        parameters=["my_dag", "update_schemas", "manual__2024-01-01T00:00:00+00:00"],
    )


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.EnvironmentsClient")
@patch("recidiviz.utils.trigger_dag_helpers._execute_and_poll")
def test_wait_for_dag_task_success_eventually(
    mock_execute_and_poll: MagicMock,
    _mock_client_class: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    # Arrange
    mock_execute_and_poll.side_effect = [["running"], ["running"], ["success"]]

    # Act / Assert (no exception)
    with patch("time.sleep"):
        wait_for_dag_task_success(
            "my_dag",
            "manual__2024-01-01T00:00:00+00:00",
            "update_schemas",
            timeout_seconds=300,
        )

    assert mock_execute_and_poll.call_count == 3


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.EnvironmentsClient")
@patch("recidiviz.utils.trigger_dag_helpers._execute_and_poll")
def test_wait_for_dag_task_success_failure(
    mock_execute_and_poll: MagicMock,
    _mock_client_class: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    # Arrange
    mock_execute_and_poll.return_value = ["failed"]

    # Act / Assert
    with pytest.raises(ValueError, match="ended with state: 'failed'"):
        wait_for_dag_task_success(
            "my_dag",
            "manual__2024-01-01T00:00:00+00:00",
            "update_schemas",
            timeout_seconds=300,
        )


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.EnvironmentsClient")
@patch("recidiviz.utils.trigger_dag_helpers._execute_and_poll")
def test_wait_for_dag_task_success_timeout(
    mock_execute_and_poll: MagicMock,
    _mock_client_class: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    # Arrange
    mock_execute_and_poll.return_value = ["running"]

    # Act / Assert
    with patch("time.sleep"), patch("time.time", side_effect=[0, 9999]), pytest.raises(
        TimeoutError, match="Timed out"
    ):
        wait_for_dag_task_success(
            "my_dag",
            "manual__2024-01-01T00:00:00+00:00",
            "update_schemas",
            timeout_seconds=100,
        )


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.wait_for_dag_task_success")
@patch("recidiviz.utils.trigger_dag_helpers.trigger_dag_run")
def test_trigger_calculation_dag_success(
    mock_trigger_dag_run: MagicMock,
    mock_wait: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    mock_trigger_dag_run.return_value = "manual__2024-01-01T00:00:00+00:00"

    trigger_calculation_dag()

    mock_trigger_dag_run.assert_called_once_with(
        "recidiviz-staging_calculation_dag", conf={}
    )
    mock_wait.assert_called_once_with(
        "recidiviz-staging_calculation_dag",
        "manual__2024-01-01T00:00:00+00:00",
        "update_big_query_table_schemata",
        timeout_seconds=1200,
    )


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.trigger_dag_run")
def test_trigger_raw_data_import_dag_primary_no_state_filter(
    mock_trigger_dag_run: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    trigger_raw_data_import_dag(
        raw_data_instance=DirectIngestInstance.PRIMARY,
        state_code_filter=None,
    )

    mock_trigger_dag_run.assert_called_once_with(
        "recidiviz-staging_raw_data_import_dag",
        conf={"state_code_filter": None, "ingest_instance": "PRIMARY"},
    )


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.trigger_dag_run")
def test_trigger_raw_data_import_dag_primary_with_state_filter(
    mock_trigger_dag_run: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    trigger_raw_data_import_dag(
        raw_data_instance=DirectIngestInstance.PRIMARY,
        state_code_filter=StateCode.US_XX,
    )

    mock_trigger_dag_run.assert_called_once_with(
        "recidiviz-staging_raw_data_import_dag",
        conf={"state_code_filter": "US_XX", "ingest_instance": "PRIMARY"},
    )


@patch(
    "recidiviz.utils.trigger_dag_helpers.project_id", return_value="recidiviz-staging"
)
@patch("recidiviz.utils.trigger_dag_helpers.trigger_dag_run")
def test_trigger_raw_data_import_dag_secondary_with_state_filter(
    mock_trigger_dag_run: MagicMock,
    _mock_project_id: MagicMock,
) -> None:
    trigger_raw_data_import_dag(
        raw_data_instance=DirectIngestInstance.SECONDARY,
        state_code_filter=StateCode.US_XX,
    )

    mock_trigger_dag_run.assert_called_once_with(
        "recidiviz-staging_raw_data_import_dag",
        conf={"state_code_filter": "US_XX", "ingest_instance": "SECONDARY"},
    )


@patch("recidiviz.utils.trigger_dag_helpers.trigger_dag_run")
def test_trigger_raw_data_import_dag_secondary_no_state_filter_raises(
    mock_trigger_dag_run: MagicMock,
) -> None:
    with pytest.raises(ValueError, match="state-agnostic SECONDARY"):
        trigger_raw_data_import_dag(
            raw_data_instance=DirectIngestInstance.SECONDARY,
            state_code_filter=None,
        )

    mock_trigger_dag_run.assert_not_called()
