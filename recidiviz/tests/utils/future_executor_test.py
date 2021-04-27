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
from typing import Optional, Dict, List
from unittest import TestCase, mock

from recidiviz.utils.future_executor import FutureExecutor, FutureExecutorProgress


def _mock_future(
    return_value: Optional[int] = None, exception: Optional[BaseException] = None
) -> Optional[int]:
    if exception:
        raise exception

    return return_value


class TestFutureExecutor(TestCase):
    """ Tests cases for the FutureExecutor """

    def test_progress(self) -> None:
        targets = [{"return_value": 0.1}, {"return_value": 0.2}, {"return_value": 0.3}]
        with FutureExecutor.build(
            _mock_future,
            targets,
            max_workers=1,
        ) as executor:
            self.assertEqual(
                [
                    FutureExecutorProgress(running=mock.ANY, completed=1, total=3),
                    FutureExecutorProgress(running=mock.ANY, completed=2, total=3),
                    FutureExecutorProgress(running=mock.ANY, completed=3, total=3),
                ],
                list(executor.progress()),
            )

    def test_progress_exception(self) -> None:
        # Mypy infers this to be `object` without the explicit typing
        targets: List[Dict] = [
            {"return_value": 0.1},
            {"return_value": 0.2},
            {"exception": ValueError("oh no!")},
        ]
        with self.assertRaises(ValueError) as cm:
            with FutureExecutor.build(
                _mock_future,
                targets,
                max_workers=1,
            ) as executor:
                self.assertEqual(
                    [
                        FutureExecutorProgress(running=mock.ANY, completed=1, total=3),
                        FutureExecutorProgress(running=mock.ANY, completed=2, total=3),
                        FutureExecutorProgress(running=mock.ANY, completed=3, total=3),
                    ],
                    list(executor.progress()),
                )
        self.assertEqual(cm.exception.args[0], "oh no!")

    def test_results(self) -> None:
        targets = [{"return_value": 1}, {"return_value": 2}, {"return_value": 999}]

        with FutureExecutor.build(_mock_future, targets, max_workers=1) as executor:
            self.assertEqual(executor.results(), [1, 2, 999])
