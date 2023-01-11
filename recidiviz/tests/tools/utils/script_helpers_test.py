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
                == [mock.call("test input [y/n]: ")],
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
                == [mock.call("test input [y/n]: ")],
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
                        "(Anything else exits): "
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
                            "[DRY RUN] test input [y/n]:  "
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
    assert input_mock.mock_calls == [mock.call("test input [y/n]: ")]
    assert caplog.record_tuples == logging_results


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
