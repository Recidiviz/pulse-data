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
"""Useful types and abstracitons for airflow tasks"""
import abc
import json
from typing import Generic, List, Type, TypeVar

import attr


class BaseResult:
    """Represents the result of a raw data import dag airflow task
    with methods to serialize/deserialize in order to be passed
    between tasks via xcom"""

    @abc.abstractmethod
    def serialize(self) -> str:
        """Method to serialize BaseResult to string"""

    @staticmethod
    @abc.abstractmethod
    def deserialize(json_str: str) -> "BaseResult":
        """Method to deserialize json string to BaseResult"""


class BaseError:
    @abc.abstractmethod
    def serialize(self) -> str:
        """Method to serialize RawDataImportError to string"""

    @staticmethod
    @abc.abstractmethod
    def deserialize(json_str: str) -> "BaseError":
        """Method to deserialize json string into RawDataImportError"""


T = TypeVar("T", bound="BaseResult")
E = TypeVar("E", bound="BaseError")


@attr.define
class BatchedTaskInstanceOutput(Generic[T, E]):
    """
    Represents the output from an Airflow task instance operating on a batch of input to increase parallelism
    with methods to serialize/deserialize in order to be passed between tasks via xcom.

    Attributes:
        results (List[T]): A list of results produced by the task instance.
        errors (List[E]): A list of errors encountered during the task execution.
    """

    results: List[T]
    errors: List[E]

    def serialize(self) -> str:
        result_dict = {
            "results": [result.serialize() for result in self.results],
            "errors": [error.serialize() for error in self.errors],
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(
        json_str: str, result_cls: Type[T], error_cls: Type[E]
    ) -> "BatchedTaskInstanceOutput":
        data = json.loads(json_str)
        return BatchedTaskInstanceOutput(
            results=[result_cls.deserialize(chunk) for chunk in data["results"]],
            errors=[error_cls.deserialize(error) for error in data["errors"]],
        )


@attr.define
class MappedBatchedTaskOutput(Generic[T, E]):
    """Represents output of a mapped airflow task (task created using the expand function).

    Attributes:
        task_instance_output_list (List[BatchedTaskInstanceOutput]): The output of all of the task instances in the mapped task.
    """

    task_instance_output_list: List[BatchedTaskInstanceOutput[T, E]]

    def flatten_errors(self) -> List[E]:
        return [
            error
            for task_instance_result in self.task_instance_output_list
            for error in task_instance_result.errors
        ]

    def flatten_results(self) -> List[T]:
        return [
            result
            for task_instance_result in self.task_instance_output_list
            for result in task_instance_result.results
        ]

    @staticmethod
    def deserialize(
        json_str_list: List[str], result_cls: Type[T], error_cls: Type[E]
    ) -> "MappedBatchedTaskOutput":
        return MappedBatchedTaskOutput[T, E](
            task_instance_output_list=[
                BatchedTaskInstanceOutput.deserialize(json_str, result_cls, error_cls)
                for json_str in json_str_list
            ]
        )
