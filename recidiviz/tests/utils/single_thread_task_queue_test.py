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
"""Tests for SingleThreadTaskQueue."""
import unittest

from recidiviz.utils.single_thread_task_queue import SingleThreadTaskQueue


class _TestException(ValueError):
    pass


class SingleThreadTaskQueueTest(unittest.TestCase):
    """Tests for SingleThreadTaskQueue."""

    def test_join_terminates_and_raises_on_exception(self) -> None:
        task_queue = SingleThreadTaskQueue(name="test_queue")

        def raise_value_error() -> None:
            raise _TestException("FOO BAR")

        task_queue.add_task("my_task", raise_value_error)

        with self.assertRaises(_TestException):
            task_queue.join()

    def test_run_many_tasks(self) -> None:
        task_queue = SingleThreadTaskQueue(name="test_queue", max_tasks=1000)

        self.num_tasks = 0
        expected_num_tasks = 1000

        def do_task() -> None:
            self.num_tasks += 1

        for _ in range(expected_num_tasks):
            task_queue.add_task("task", do_task)

        task_queue.join()
        self.assertEqual(expected_num_tasks, self.num_tasks)

    def test_add_tasks_during_tasks(self) -> None:
        task_queue = SingleThreadTaskQueue(name="test_queue")

        self.num_tasks = 0

        def do_task() -> None:
            self.num_tasks += 1
            if self.num_tasks < 10:
                task_queue.add_task("task", do_task)

        task_queue.add_task("task", do_task)

        task_queue.join()
        self.assertEqual(10, self.num_tasks)
