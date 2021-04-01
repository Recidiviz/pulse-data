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
from unittest import TestCase

from recidiviz.utils.future_executor import FutureExecutor, FutureExecutorProgress


class TestFutureExecutor(TestCase):
    def setUp(self) -> None:
        self.future = lambda return_value: return_value

    def test_progress(self) -> None:
        targets = [{"return_value": 0.1}, {"return_value": 0.2}, {"return_value": 0.3}]
        with FutureExecutor.build(
            self.future,
            targets,
            max_workers=1,
        ) as executor:
            self.assertEqual(
                list(executor.progress()),
                [
                    FutureExecutorProgress(running=2, completed=1, total=3),
                    FutureExecutorProgress(running=1, completed=2, total=3),
                    FutureExecutorProgress(running=0, completed=3, total=3),
                ],
            )

    def test_results(self) -> None:
        targets = [{"return_value": 1}, {"return_value": 2}, {"return_value": 999}]

        with FutureExecutor.build(self.future, targets, max_workers=1) as executor:
            self.assertEqual(executor.results(), [1, 2, 999])
