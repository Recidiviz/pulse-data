# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""tests for airflow types defined in airflow_types.py"""
import json
from typing import Any, Type
from unittest import TestCase

import attr

from recidiviz.utils.airflow_types import (
    BaseError,
    BaseResult,
    BatchedTaskInstanceOutput,
    MappedBatchedTaskOutput,
)


@attr.define
class FakeResult(BaseResult):

    a: str
    b: int
    c: bool

    def serialize(self) -> str:
        return json.dumps([self.a, self.b, self.c])

    @staticmethod
    def deserialize(json_str: str) -> "FakeResult":
        data = json.loads(json_str)
        return FakeResult(a=data[0], b=data[1], c=data[2])


@attr.define
class FakeError(BaseError):

    error: str

    def serialize(self) -> str:
        return json.dumps([self.error])

    @staticmethod
    def deserialize(json_str: str) -> "FakeError":
        data = json.loads(json_str)
        return FakeError(error=data[0])


class TestAirflowTypes(TestCase):
    """Tests for airflow types defined in airflow_types.py"""

    def _validate_serialization(
        self, obj: Any, obj_type: Type, result_cls: Type, error_cls: Type
    ) -> None:
        serialized = obj.serialize()
        deserialized = obj_type.deserialize(serialized, result_cls, error_cls)

        self.assertEqual(obj, deserialized)

    def test_batched_task_instance_output(self) -> None:

        test_output = BatchedTaskInstanceOutput[FakeResult, FakeError](
            results=[FakeResult(a="a", b=1, c=True), FakeResult(a="b", b=2, c=True)],
            errors=[
                FakeError(error="fake"),
                FakeError(error="errors"),
                FakeError(error="only"),
            ],
        )

        self._validate_serialization(
            test_output, BatchedTaskInstanceOutput, FakeResult, FakeError
        )

    def test_mapped_batched_task_output(self) -> None:
        test_output_one = BatchedTaskInstanceOutput[FakeResult, FakeError](
            results=[FakeResult(a="a", b=1, c=True), FakeResult(a="b", b=2, c=True)],
            errors=[
                FakeError(error="fake"),
                FakeError(error="errors"),
                FakeError(error="only"),
            ],
        )

        test_output_two = BatchedTaskInstanceOutput[FakeResult, FakeError](
            results=[FakeResult(a="c", b=3, c=False), FakeResult(a="d", b=4, c=False)],
            errors=[
                FakeError(error="pls"),
                FakeError(error="only"),
                FakeError(error="non-real"),
            ],
        )

        test_output_three = BatchedTaskInstanceOutput[FakeResult, FakeError](
            results=[], errors=[]
        )

        test_mapped_output = MappedBatchedTaskOutput(
            task_instance_output_list=[
                test_output_one,
                test_output_two,
                test_output_three,
            ]
        )

        self.assertEqual(
            test_mapped_output.flatten_errors(),
            [*test_output_one.errors, *test_output_two.errors],
        )
        self.assertEqual(
            test_mapped_output.flatten_results(),
            [*test_output_one.results, *test_output_two.results],
        )
