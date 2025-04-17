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
    def fake_futures_work_function(
        number_and_raises: tuple[int, ValueError | None]
    ) -> int:
        number, raises = number_and_raises
        if raises:
            raise raises
        return number

    ten_minutes = 10 * 60
    value_error = ValueError("An exception happened!")
    result = map_fn_with_progress_bar_results(
        work_fn=fake_futures_work_function,
        work_items=[
            (1, None),
            (2, None),
            (3, value_error),
        ],
        max_workers=8,
        overall_timeout_sec=ten_minutes,
        single_work_item_timeout_sec=ten_minutes,
        progress_bar_message="test progress message",
    )
    assert sorted(result.successes, key=lambda x: x[0]) == [
        ((1, None), 1),
        ((2, None), 2),
    ]
    assert result.exceptions == [((3, value_error), value_error)]
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
