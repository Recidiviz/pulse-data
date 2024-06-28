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
"""Types associated with raw data imports"""
import abc
import json
from enum import Enum, auto
from typing import Generic, List, Optional, Type, TypeVar

import attr

from recidiviz.big_query.big_query_utils import (
    is_big_query_valid_delimiter,
    is_big_query_valid_encoding,
    is_big_query_valid_line_terminator,
)
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.common.constants.csv import DEFAULT_CSV_ENCODING
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig


class PreImportNormalizationType(Enum):
    """The type of normalization needed to make a CSV conform to BigQuery's load job
    standards
    """

    # The encoding, delimiter, and/or line terminator must be updated to make this CSV BigQuery import compatible
    ENCODING_DELIMITER_AND_TERMINATOR_UPDATE = auto()

    # Only the encoding must be updated to make this CSV BQ import compatible
    ENCODING_UPDATE_ONLY = auto()

    @staticmethod
    def required_pre_import_normalization_type(
        config: DirectIngestRawFileConfig,
    ) -> Optional["PreImportNormalizationType"]:
        """Given a raw data |config|, returns the pre-import normalization type required
        to make this CSV conform to BigQuery's load job standards. If no normalization
        is needed, will return None."""

        requires_encoding_translation = not is_big_query_valid_encoding(config.encoding)
        final_encoding = (
            DEFAULT_CSV_ENCODING if requires_encoding_translation else config.encoding
        )

        if not is_big_query_valid_line_terminator(
            config.line_terminator
        ) or not is_big_query_valid_delimiter(config.separator, final_encoding):
            return PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE

        if requires_encoding_translation:
            return PreImportNormalizationType.ENCODING_UPDATE_ONLY

        return None


class BaseResult:
    """Represents the result of a raw data import dag airflow task
    with methods to serialize/deserialize in order to be passed
    between tasks via xcom"""

    @abc.abstractmethod
    def serialize(self) -> str:
        """Method to serialized object to string"""

    @staticmethod
    @abc.abstractmethod
    def deserialize(json_str: str) -> "BaseResult":
        """Method to deserialize json string to object"""


@attr.define
class RawFileProcessingError:
    file_path: str
    error_msg: str

    def serialize(self) -> str:
        result_dict = {"file_path": self.file_path, "error_msg": self.error_msg}
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawFileProcessingError":
        data = json.loads(json_str)
        return RawFileProcessingError(
            file_path=data["file_path"], error_msg=data["error_msg"]
        )


T = TypeVar("T", bound="BaseResult")


@attr.define
class BatchedTaskInstanceOutput(Generic[T]):
    """
    Represents the output from an Airflow task instance operating on a batch of input to increase parallelism
    with methods to serialize/deserialize in order to be passed between tasks via xcom.

    Attributes:
        results (List[T]): A list of results produced by the task instance.
        errors (List[RawFileProcessingError]): A list of errors encountered during the task execution.
    """

    results: List[T]
    errors: List[RawFileProcessingError]

    def serialize(self) -> str:
        result_dict = {
            "results": [result.serialize() for result in self.results],
            "errors": [error.serialize() for error in self.errors],
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str, result_cls: Type[T]) -> "BatchedTaskInstanceOutput":
        data = json.loads(json_str)
        return BatchedTaskInstanceOutput(
            results=[result_cls.deserialize(chunk) for chunk in data["results"]],
            errors=[
                RawFileProcessingError.deserialize(error) for error in data["errors"]
            ],
        )


@attr.define
class MappedBatchedTaskOutput:
    """Represents output of a mapped airflow task (task created using the expand function).

    Attributes:
        task_instance_output_list (List[BatchedTaskInstanceOutput]): The output of all of the task instances in the mapped task.
    """

    task_instance_output_list: List[BatchedTaskInstanceOutput]

    def flatten_errors(self) -> List[RawFileProcessingError]:
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
        json_str_list: List[str], result_cls: Type[T]
    ) -> "MappedBatchedTaskOutput":
        return MappedBatchedTaskOutput(
            task_instance_output_list=[
                BatchedTaskInstanceOutput.deserialize(json_str, result_cls)
                for json_str in json_str_list
            ]
        )


@attr.define
class RequiresPreImportNormalizationFileChunk(BaseResult):
    """Encapsulates the path, the chunk boundary and the type of normalization required
    for the CSV to conform to BigQuery's load job standards."""

    path: str
    normalization_type: Optional[PreImportNormalizationType]
    chunk_boundary: CsvChunkBoundary
    headers: List[str]

    def serialize(self) -> str:
        return json.dumps(
            {
                "path": self.path,
                "normalization_type": (
                    self.normalization_type.value if self.normalization_type else ""
                ),
                "chunk_boundary": self.chunk_boundary.serialize(),
                "headers": self.headers,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RequiresPreImportNormalizationFileChunk":
        data = json.loads(json_str)
        return RequiresPreImportNormalizationFileChunk(
            path=data["path"],
            normalization_type=PreImportNormalizationType(data["normalization_type"]),
            chunk_boundary=CsvChunkBoundary.deserialize(data["chunk_boundary"]),
            headers=data["headers"],
        )


@attr.define
class RequiresPreImportNormalizationFile(BaseResult):
    """Encapsulates the path, the headers, the chunk boundaries for the file
    and the type of normalization required for the CSV to conform to
    BigQuery's load job standards."""

    path: str
    normalization_type: Optional[PreImportNormalizationType]
    chunk_boundaries: List[CsvChunkBoundary]
    headers: List[str]

    def serialize(self) -> str:
        return json.dumps(
            {
                "path": self.path,
                "normalization_type": (
                    self.normalization_type.value if self.normalization_type else ""
                ),
                "chunk_boundaries": [
                    boundary.serialize() for boundary in self.chunk_boundaries
                ],
                "headers": self.headers,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RequiresPreImportNormalizationFile":
        data = json.loads(json_str)
        return RequiresPreImportNormalizationFile(
            path=data["path"],
            normalization_type=PreImportNormalizationType(data["normalization_type"]),
            chunk_boundaries=[
                CsvChunkBoundary.deserialize(boundary)
                for boundary in data["chunk_boundaries"]
            ],
            headers=data["headers"],
        )

    def to_file_chunks(
        self,
    ) -> List[RequiresPreImportNormalizationFileChunk]:
        """
        Extract individual RequiresPreImportNormalizationFileChunk objects from list of CSVBoundary objects.
        """
        individual_chunks = []
        for chunk_boundary in self.chunk_boundaries:
            chunk = RequiresPreImportNormalizationFileChunk(
                path=self.path,
                normalization_type=self.normalization_type,
                chunk_boundary=chunk_boundary,
                headers=self.headers,
            )
            individual_chunks.append(chunk)
        return individual_chunks


@attr.define
class NormalizedCsvChunkResult(BaseResult):
    """Encapsulates the output path, chunk boundary and checksum of the CSV chunk. This
    should relate 1-1 to a RequiresPreImportNormalizationFileChunk."""

    input_file_path: str
    output_file_path: str
    chunk_boundary: CsvChunkBoundary
    crc32c: int

    def get_chunk_boundary_size(self) -> int:
        return self.chunk_boundary.get_chunk_size()

    def serialize(self) -> str:
        result_dict = {
            "input_file_path": self.input_file_path,
            "output_file_path": self.output_file_path,
            "chunk_boundary": self.chunk_boundary.serialize(),
            "crc32c": self.crc32c,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "NormalizedCsvChunkResult":
        data = json.loads(json_str)
        return NormalizedCsvChunkResult(
            input_file_path=data["input_file_path"],
            output_file_path=data["output_file_path"],
            chunk_boundary=CsvChunkBoundary.deserialize(data["chunk_boundary"]),
            crc32c=data["crc32c"],
        )


@attr.define
class RequiresNormalizationFile:
    file_path: str
    file_tag: str


@attr.define
class ImportReadyNormalizedFile(BaseResult):
    input_file_path: str
    output_file_paths: List[str]

    def serialize(self) -> str:
        result_dict = {
            "input_file_path": self.input_file_path,
            "output_file_paths": self.output_file_paths,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "ImportReadyNormalizedFile":
        data = json.loads(json_str)
        return ImportReadyNormalizedFile(
            input_file_path=data["input_file_path"],
            output_file_paths=data["output_file_paths"],
        )


@attr.define
class LoadPrepSummary:
    """Summary from DirectIngestRawFileLoadManager.load_and_prep_paths step that will
    be combined with AppendSummary to build a row in direct_ingest_raw_data_import_session


    append_ready_table_address (str): temp BQ address of loaded, transformed and migrated
        raw data
    raw_rows_count (int): number of raw rows loaded from raw file paths before any
        transformations or filtering occured
    """

    append_ready_table_address: str
    raw_rows_count: int
