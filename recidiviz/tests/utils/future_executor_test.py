# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Implements tests for FutureExecutor. """
from typing import Any

from recidiviz.utils.future_executor import map_fn_with_progress_bar_results


def test_map_fn_with_progress_bar_results(capfd: Any) -> None:
    def fake_futures_function(number: int, raises: Any) -> int:
        if raises:
            raise raises
        return number

    ten_minutes = 10 * 60
    value_error = ValueError("An exception happened!")
    successes, exceptions = map_fn_with_progress_bar_results(
        fn=fake_futures_function,
        kwargs_list=[
            {"number": 1, "raises": False},
            {"number": 2, "raises": False},
            {"number": 3, "raises": value_error},
        ],
        max_workers=8,
        timeout=ten_minutes,
        progress_bar_message="test progress message",
    )
    assert sorted(successes, key=lambda x: x[0]) == [
        (1, {"number": 1, "raises": False}),
        (2, {"number": 2, "raises": False}),
    ]
    assert exceptions == [(value_error, {"number": 3, "raises": value_error})]
    assert capfd.readouterr().err == (
        "\x1b[?25l"
        "\r"
        "\rtest progress message |                                | 0/3"
        "\rtest progress message |##########                      | 1/3"
        "\rtest progress message |#####################           | 2/3"
        "\rtest progress message |################################| 3/3"
        "\n"
        "\x1b[?25h"
    )
