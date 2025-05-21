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
"""Unit tests for script helpers"""
import logging
from contextlib import nullcontext
from typing import Any, Callable, Dict, Tuple
from unittest import TestCase, mock
from unittest.mock import MagicMock, call, patch

import pytest
from parameterized import param, parameterized

from recidiviz.tools.utils.script_helpers import (
    interactive_loop_until_tasks_succeed,
    interactive_prompt_retry_on_exception,
    prompt_for_confirmation,
)


class TestScriptHelpers(TestCase):
    """Unit tests w/ unittest for script helpers"""

    @parameterized.expand(
        [
            param.explicit(
                kwargs={
                    "call_args": {},
                    "user_input": ["y"],
                    "logging_results": [],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [],
                },
                args=(["yes"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {},
                    "user_input": ["n"],
                    "logging_results": [
                        "WARNING:root:\nResponded with [n]. Confirmation aborted.",
                    ],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [call(1)],
                },
                args=(["no and exit"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"accepted_response_override": "YES"},
                    "user_input": ["yes"],
                    "logging_results": [],
                    "input_calls": [
                        call(
                            "test input"
                            "\nPlease type [YES] to confirm, [n] to cancel:\n"
                        )
                    ],
                    "exit_calls": [],
                },
                args=(["yes with override"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"accepted_response_override": "YES"},
                    "user_input": [
                        "Y",
                        """

                    yes """,
                    ],
                    "logging_results": ["INFO:root:Invalid choice [y]"],
                    "input_calls": [
                        call(
                            "test input"
                            "\nPlease type [YES] to confirm, [n] to cancel:\n"
                        ),
                        call(
                            "test input"
                            "\nPlease type [YES] to confirm, [n] to cancel:\n"
                        ),
                    ],
                    "exit_calls": [],
                },
                args=(["yes with override"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"dry_run": True},
                    "user_input": [],
                    "logging_results": [
                        "INFO:root:[DRY RUN] test input [y/n]: \n **DRY RUN - SKIPPED CONFIRMATION**",
                    ],
                    "input_calls": [],
                    "exit_calls": [],
                },
                args=(["dry run"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"exit_code": 100, "exit_on_cancel": False},
                    "user_input": ["y"],
                    "logging_results": [],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [],
                },
                args=(["custom_exit_code_dont_exit"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"exit_code": 100, "exit_on_cancel": True},
                    "user_input": ["y"],
                    "logging_results": [],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [],
                },
                args=(["custom_exit_code_on_cancel_dont_exit"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"exit_code": 100, "exit_on_cancel": False},
                    "user_input": ["n"],
                    "logging_results": [
                        "WARNING:root:\nResponded with [n]. Confirmation aborted."
                    ],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [],
                },
                args=(["custom_exit_code_exit"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"exit_code": 100, "exit_on_cancel": True},
                    "user_input": ["n"],
                    "logging_results": [
                        "WARNING:root:\nResponded with [n]. Confirmation aborted."
                    ],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [call(100)],
                },
                args=(["custom_exit_code_on_cancel_exit"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"exit_code": 100, "exit_on_cancel": True},
                    "user_input": ["n"],
                    "logging_results": [
                        "WARNING:root:\nResponded with [n]. Confirmation aborted."
                    ],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [call(100)],
                },
                args=(["custom_exit_code_on_cancel_exit"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"exit_code": 100, "exit_on_cancel": True},
                    "user_input": ["n"],
                    "logging_results": [
                        "WARNING:root:\nResponded with [n]. Confirmation aborted."
                    ],
                    "input_calls": [call("test input [y/n]: \n")],
                    "exit_calls": [call(100)],
                },
                args=(["custom_exit_code_on_cancel_exit"]),
            ),
            param.explicit(
                kwargs={
                    "call_args": {"rejected_response_override": "NAH"},
                    "user_input": ["n", "nah"],
                    "logging_results": [
                        "INFO:root:Invalid choice [n]",
                        "WARNING:root:\nResponded with [nah]. Confirmation aborted.",
                    ],
                    "input_calls": [
                        call(
                            "test input\nPlease type [y] to confirm, [NAH] to cancel:\n"
                        ),
                        call(
                            "test input\nPlease type [y] to confirm, [NAH] to cancel:\n"
                        ),
                    ],
                    "exit_calls": [call(1)],
                },
                args=(["custom_rejected"]),
            ),
        ],
    )
    @patch("sys.exit")
    @patch("builtins.input")
    def test_prompt_for_confirmation(
        self,
        _test_name: str,
        input_mock: MagicMock,
        exit_mock: MagicMock,
        *,
        call_args: dict[str, Any],
        user_input: list[str],
        logging_results: list[str],
        input_calls: list[tuple],
        exit_calls: list[tuple],
    ) -> None:
        input_call_count = 0

        def _input_return(_prompt: str) -> str:
            nonlocal input_call_count
            input_call_count += 1
            return user_input[input_call_count - 1]

        log_assert = self.assertLogs if logging_results else self.assertNoLogs
        with log_assert(level="INFO") as capture:
            input_mock.side_effect = _input_return
            prompt_for_confirmation("test input", **call_args)
            assert input_mock.mock_calls == input_calls
            assert exit_mock.mock_calls == exit_calls
            if capture:
                assert capture.output == logging_results


@pytest.mark.parametrize(
    ("prompt_rv", "calls", "expected", "exception_context"),
    [
        pytest.param(
            True,
            lambda m: m.mock_calls == [call(), call()],
            "A",
            nullcontext(),
            id="confirm retry",
        ),
        pytest.param(
            False,
            lambda m: m.mock_calls == [call()],
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
        call(
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
        assert mock_tasks_fn.mock_calls == [call(tasks_kwargs=list(tasks))]
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
        assert mock_tasks_fn.mock_calls == [call(tasks_kwargs=list(tasks))]
        assert mock_prompt.mock_calls == [
            call(
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
        assert mock_tasks_fn.mock_calls == [call(tasks_kwargs=list(tasks))]
        assert mock_prompt.mock_calls == [
            call(
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
            call(tasks_kwargs=list(tasks)),
            call(tasks_kwargs=list(tasks)),
        ]
        assert mock_prompt.mock_calls == [
            call(
                input_text="Should we rerun all tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
            call(
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
            call(tasks_kwargs=list(tasks)),
            call(tasks_kwargs=list(tasks)),
            call(tasks_kwargs=[task_two, task_four]),
            call(tasks_kwargs=[task_four]),
        ]
        assert mock_prompt.mock_calls == [
            call(
                input_text="Should we rerun all tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
            call(
                input_text="Should we rerun the failed tasks?",
                accepted_response_override="yes",
                exit_on_cancel=True,
            ),
            call(
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
