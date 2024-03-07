# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""recidiviz.deploy.utls.script_helpers.py tests"""
import logging
from contextlib import nullcontext
from typing import Any, Callable, Dict, List, Tuple
from unittest import mock

import pytest

from recidiviz.tools.utils.script_helpers import (
    interactive_loop_until_tasks_succeed,
    interactive_prompt_retry_on_exception,
    prompt_for_confirmation,
)


@pytest.mark.parametrize(
    ("kwparams"),
    [
        pytest.param(
            {
                "call_args": {},
                "user_input": "y",
                "logging_results": [],
                "input_mock_check": lambda m: m.mock_calls
                == [mock.call("test input [y/n]: \n")],
                "exit_mock_check": lambda m: not m.called,
            },
            id="yes",
        ),
        pytest.param(
            {
                "call_args": {},
                "user_input": "n",
                "logging_results": [
                    (
                        "root",
                        logging.WARNING,
                        "\nResponded with [n]. Confirmation aborted.",
                    )
                ],
                "input_mock_check": lambda m: m.mock_calls
                == [mock.call("test input [y/n]: \n")],
                "exit_mock_check": lambda m: m.mock_calls == [mock.call(1)],
            },
            id="no and exit",
        ),
        pytest.param(
            {
                "call_args": {"accepted_response_override": "YES"},
                "user_input": "yes",
                "logging_results": [],
                "input_mock_check": lambda m: m.mock_calls
                == [
                    mock.call(
                        "test input"
                        '\nPlease type "YES" to confirm. '
                        "(Anything else exits): \n"
                    )
                ],
                "exit_mock_check": lambda m: not m.called,
            },
            id="yes with override",
        ),
        pytest.param(
            {
                "call_args": {"dry_run": True},
                "user_input": None,
                "logging_results": [
                    (
                        "root",
                        logging.INFO,
                        (
                            "[DRY RUN] test input [y/n]: \n "
                            "**DRY RUN - SKIPPED CONFIRMATION**"
                        ),
                    )
                ],
                "input_mock_check": lambda m: not m.called,
                "exit_mock_check": lambda m: not m.called,
            },
            id="dry run",
        ),
    ],
)
@mock.patch("sys.exit")
@mock.patch("builtins.input")
def test_prompt_for_confirmation_legacy_behavior(
    input_mock: mock.MagicMock,
    exit_mock: mock.MagicMock,
    caplog: Any,
    kwparams: Dict[str, Any],
) -> None:
    input_mock.return_value = kwparams["user_input"]
    caplog.set_level(logging.INFO)
    prompt_for_confirmation("test input", **kwparams["call_args"])
    assert kwparams["input_mock_check"](input_mock)
    assert kwparams["exit_mock_check"](exit_mock)
    assert caplog.record_tuples == kwparams["logging_results"]


@pytest.mark.parametrize(
    ("user_input", "logging_results", "rv"),
    [
        ("y", [], True),
        (
            "n",
            [("root", logging.WARNING, "\nResponded with [n]. Confirmation aborted.")],
            False,
        ),
    ],
)
@mock.patch("builtins.input")
def test_prompt_for_confirmation_no_exit_boolean(
    input_mock: mock.MagicMock,
    caplog: Any,
    user_input: str,
    logging_results: List[Tuple[str, int, str]],
    rv: bool,
) -> None:
    input_mock.return_value = user_input
    assert rv == prompt_for_confirmation("test input", exit_on_cancel=False)
    assert input_mock.mock_calls == [mock.call("test input [y/n]: \n")]
    assert caplog.record_tuples == logging_results


@pytest.mark.parametrize(
    ("user_input", "logging_results", "exit_code", "exit_assertion"),
    [
        ("y", [], 0, lambda m: not m.called),
        (
            "n",
            [("root", logging.WARNING, "\nResponded with [n]. Confirmation aborted.")],
            0,
            lambda m: m.mock_calls == [mock.call(0)],
        ),
        (
            "n",
            [("root", logging.WARNING, "\nResponded with [n]. Confirmation aborted.")],
            100,
            lambda m: m.mock_calls == [mock.call(100)],
        ),
        (
            "n",
            [("root", logging.WARNING, "\nResponded with [n]. Confirmation aborted.")],
            -100,
            lambda m: m.mock_calls == [mock.call(-100)],
        ),
    ],
)
@mock.patch("sys.exit")
@mock.patch("builtins.input")
def test_prompt_for_confirmation_custom_exit_code(
    input_mock: mock.MagicMock,
    exit_mock: mock.MagicMock,
    caplog: Any,
    user_input: str,
    logging_results: List[Tuple[str, int, str]],
    exit_code: int,
    exit_assertion: Callable,
) -> None:
    input_mock.return_value = user_input
    prompt_for_confirmation("test input", exit_code=exit_code)
    assert input_mock.mock_calls == [mock.call("test input [y/n]: \n")]
    assert caplog.record_tuples == logging_results
    assert exit_assertion(exit_mock)


@pytest.mark.parametrize(
    ("prompt_rv", "calls", "expected", "exception_context"),
    [
        pytest.param(
            True,
            lambda m: m.mock_calls == [mock.call(), mock.call()],
            "A",
            nullcontext(),
            id="confirm retry",
        ),
        pytest.param(
            False,
            lambda m: m.mock_calls == [mock.call()],
            None,
            pytest.raises(KeyError),
            id="cancel retry",
        ),
    ],
)
@mock.patch(
    "recidiviz.tools.utils.script_helpers.prompt_for_confirmation", return_value=True
)
def test_interactive_prompt_retry_on_exception(
    prompt_mock: mock.MagicMock,
    prompt_rv: bool,
    calls: Callable,
    expected: Any,
    exception_context: Any,
) -> None:
    prompt_mock.return_value = prompt_rv
    exceptional_call_mock = mock.MagicMock(side_effect=[KeyError(), "A"])
    with exception_context:
        answer = interactive_prompt_retry_on_exception(
            fn=exceptional_call_mock,
            input_text="test input",
            accepted_response_override="yes",
            exit_on_cancel=False,
        )
        assert answer == expected
    assert calls(exceptional_call_mock)
    assert prompt_mock.mock_calls == [
        mock.call(
            input_text="test input",
            accepted_response_override="yes",
            dry_run=False,
            exit_on_cancel=False,
        )
    ]


TIMEOUT_ERROR = TimeoutError("Timeout!")
KEY_ERROR = KeyError("Key error!")
EXCEPTION = Exception("An exception!")
VALUE_ERROR = ValueError("Value error!")


class TestInteractiveLoopUntilTasksSucceed:
    """
    Namespace for verifying behavior of
    interactive_loop_until_tasks_succeed
    """

    @staticmethod
    @pytest.fixture(name="tasks")
    def tasks_fixture() -> Tuple[
        Dict[str, int],
        Dict[str, int],
        Dict[str, int],
        Dict[str, int],
    ]:
        return ({"one": 1}, {"two": 2}, {"three": 3}, {"four": 4})

    @staticmethod
    @pytest.fixture(name="caplog_info")
    def caplog_info_fixture(caplog: Any) -> Any:
        caplog.set_level(logging.INFO)
        return caplog

    @staticmethod
    def test_total_success_first_try(tasks: Tuple, caplog_info: Any) -> None:
        mock_tasks_fn = mock.MagicMock(return_value=(tuple(tasks), ()))
        interactive_loop_until_tasks_succeed(
            tasks_fn=mock_tasks_fn, tasks_kwargs=list(tasks)
        )
        assert mock_tasks_fn.mock_calls == [mock.call(tasks_kwargs=list(tasks))]
        assert caplog_info.record_tuples == [
            ("root", logging.INFO, "All tasks complete")
        ]

    @staticmethod
    @mock.patch("recidiviz.tools.utils.script_helpers.prompt_for_confirmation")
    def test_hard_fail_and_quit(
        mock_prompt: mock.MagicMock,
        tasks: Tuple,
        caplog_info: Any,
    ) -> None:
        mock_prompt.return_value = False
        mock_tasks_fn = mock.MagicMock(return_value=((), ()))
        interactive_loop_until_tasks_succeed(
            tasks_fn=mock_tasks_fn, tasks_kwargs=list(tasks)
        )
        assert mock_tasks_fn.mock_calls == [mock.call(tasks_kwargs=list(tasks))]
        assert mock_prompt.mock_calls == [
            mock.call(
                input_text="Should we rerun all tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            )
        ]
        assert caplog_info.record_tuples == [
            ("root", logging.ERROR, "Some results are not accounted for")
        ]

    @staticmethod
    @mock.patch("recidiviz.tools.utils.script_helpers.prompt_for_confirmation")
    def test_some_exceptions_and_quit(
        mock_prompt: mock.MagicMock,
        tasks: Tuple,
        caplog_info: Any,
    ) -> None:
        task_one, task_two, task_three, task_four = tasks
        mock_prompt.return_value = False
        mock_tasks_fn = mock.MagicMock(
            return_value=(
                (task_one, task_two),
                ((TIMEOUT_ERROR, task_three), (KEY_ERROR, task_four)),
            )
        )
        interactive_loop_until_tasks_succeed(
            tasks_fn=mock_tasks_fn, tasks_kwargs=list(tasks)
        )
        assert mock_tasks_fn.mock_calls == [mock.call(tasks_kwargs=list(tasks))]
        assert mock_prompt.mock_calls == [
            mock.call(
                input_text="Should we rerun the failed tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
        ]
        assert caplog_info.record_tuples == [
            (
                "root",
                logging.WARNING,
                "These tasks failed with the following exceptions:",
            ),
            ("root", logging.WARNING, "Timeout!    {'three': 3}"),
            ("root", logging.WARNING, "'Key error!'    {'four': 4}"),
        ]

    @staticmethod
    @mock.patch("recidiviz.tools.utils.script_helpers.prompt_for_confirmation")
    def test_hard_fail_then_exceptions_then_quit(
        mock_prompt: mock.MagicMock,
        tasks: Tuple,
        caplog_info: Any,
    ) -> None:
        task_one, task_two, task_three, task_four = tasks
        mock_prompt.side_effect = [True, False]
        mock_tasks_fn = mock.MagicMock(
            side_effect=[
                ((), ()),
                ((task_one, task_two, task_three), ((EXCEPTION, task_four),)),
            ]
        )
        interactive_loop_until_tasks_succeed(
            tasks_fn=mock_tasks_fn, tasks_kwargs=list(tasks)
        )
        assert mock_tasks_fn.mock_calls == [
            mock.call(tasks_kwargs=list(tasks)),
            mock.call(tasks_kwargs=list(tasks)),
        ]
        assert mock_prompt.mock_calls == [
            mock.call(
                input_text="Should we rerun all tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
            mock.call(
                input_text="Should we rerun the failed tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
        ]
        assert caplog_info.record_tuples == [
            ("root", logging.ERROR, "Some results are not accounted for"),
            (
                "root",
                logging.WARNING,
                "These tasks failed with the following exceptions:",
            ),
            ("root", logging.WARNING, "An exception!    {'four': 4}"),
        ]

    @staticmethod
    @mock.patch("recidiviz.tools.utils.script_helpers.prompt_for_confirmation")
    def test_hard_fail_then_exceptions_then_go_until_all_succeed(
        mock_prompt: mock.MagicMock,
        tasks: Tuple,
        caplog_info: Any,
    ) -> None:
        task_one, task_two, task_three, task_four = tasks
        mock_prompt.return_value = True
        mock_tasks_fn = mock.MagicMock(
            side_effect=[
                ((), ()),
                (
                    (task_one, task_three),
                    ((VALUE_ERROR, task_two), (EXCEPTION, task_four)),
                ),
                ((task_two,), ((TIMEOUT_ERROR, task_four),)),
                ((task_four,), ()),
            ]
        )
        interactive_loop_until_tasks_succeed(
            tasks_fn=mock_tasks_fn, tasks_kwargs=list(tasks)
        )
        assert mock_tasks_fn.mock_calls == [
            mock.call(tasks_kwargs=list(tasks)),
            mock.call(tasks_kwargs=list(tasks)),
            mock.call(tasks_kwargs=[task_two, task_four]),
            mock.call(tasks_kwargs=[task_four]),
        ]
        assert mock_prompt.mock_calls == [
            mock.call(
                input_text="Should we rerun all tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
            mock.call(
                input_text="Should we rerun the failed tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
            mock.call(
                input_text="Should we rerun the failed tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
        ]
        assert caplog_info.record_tuples == [
            ("root", logging.ERROR, "Some results are not accounted for"),
            (
                "root",
                logging.WARNING,
                "These tasks failed with the following exceptions:",
            ),
            ("root", logging.WARNING, "Value error!    {'two': 2}"),
            ("root", logging.WARNING, "An exception!    {'four': 4}"),
            (
                "root",
                logging.WARNING,
                "These tasks failed with the following exceptions:",
            ),
            ("root", logging.WARNING, "Timeout!    {'four': 4}"),
            ("root", logging.INFO, "All tasks complete"),
        ]
