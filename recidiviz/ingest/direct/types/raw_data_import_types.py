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
import datetime
import json
from enum import Enum, auto
from functools import cached_property
from typing import Dict, List, Optional, Tuple

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import (
    is_big_query_valid_delimiter,
    is_big_query_valid_encoding,
    is_big_query_valid_line_terminator,
)
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators
from recidiviz.common.constants.csv import DEFAULT_CSV_ENCODING
from recidiviz.common.constants.operations.direct_ingest_raw_data_import_session import (
    DirectIngestRawDataImportSessionStatus,
)
from recidiviz.ingest.direct.gcs.filename_parts import (
    DirectIngestRawFilenameParts,
    filename_parts_from_path,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.utils.airflow_types import BaseError, BaseResult
from recidiviz.utils.types import assert_type


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


@attr.define
class RawDataImportError(BaseError):
    """The base error type for all errors that exist in the raw data import dag.

    Attributes:
        error_type (DirectIngestRawDataImportSessionStatus): the error type that
            corresponds to where the error occurred within the raw data import dag
        error_msg (str): an error message, including any relevant stack traces.

    """

    error_type: DirectIngestRawDataImportSessionStatus = attr.ib(
        validator=attr.validators.instance_of(DirectIngestRawDataImportSessionStatus)
    )
    error_msg: str = attr.ib(validator=attr_validators.is_str)

    @abc.abstractmethod
    def serialize(self) -> str:
        """Method to serialize RawDataImportError to string"""

    @staticmethod
    @abc.abstractmethod
    def deserialize(json_str: str) -> "RawDataImportError":
        """Method to deserialize json string into RawDataImportError"""


@attr.define
class RawFileProcessingError(RawDataImportError):
    """Represents an error that occurred during a raw data pre-import normalization
    import task.

    Attributes:
        file_path (GcsfsFilePath): the file_path where the error occurred
        error_type (DirectIngestRawDataImportSessionStatus): defaults to
            FAILED_PRE_IMPORT_NORMALIZATION_STEP.
        error_msg (str): an error message, including any relevant stack traces.

    """

    file_path: GcsfsFilePath = attr.ib(
        validator=attr.validators.instance_of(GcsfsFilePath)
    )
    error_type: DirectIngestRawDataImportSessionStatus = attr.ib(
        default=DirectIngestRawDataImportSessionStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
        validator=attr.validators.in_(DirectIngestRawDataImportSessionStatus),
    )

    def __str__(self) -> str:
        return f"{self.error_type.value} with {self.file_path} failed with:\n\n{self.error_msg}"

    def serialize(self) -> str:
        result_dict = {
            "file_path": self.file_path.abs_path(),
            "error_msg": self.error_msg,
            "error_type": self.error_type.value,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawFileProcessingError":
        data = json.loads(json_str)
        return RawFileProcessingError(
            error_type=DirectIngestRawDataImportSessionStatus(data["error_type"]),
            file_path=GcsfsFilePath.from_absolute_path(data["file_path"]),
            error_msg=data["error_msg"],
        )


@attr.define
class RawFileLoadAndPrepError(RawDataImportError):
    """Represents an error that occurred during a raw data big query load step, specifically
    during the load and prep stage where we load raw data files into big query.

    Attributes:
        file_tag (str): file_tag associated with the file_paths
        update_datetime(datetime.datetime): update_datetime associated with the
            file_paths
        file_paths (List[GcsfsFilePath]): file paths that failed to successfully load
            into temporary tables
        error_type (DirectIngestRawDataImportSessionStatus): defaults to FAILED_LOAD_STEP
        error_msg (str): an error message, including any relevant stack traces.
    """

    file_tag: str = attr.ib(validator=attr_validators.is_str)
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    file_paths: List[GcsfsFilePath]
    error_type: DirectIngestRawDataImportSessionStatus = attr.ib(
        default=DirectIngestRawDataImportSessionStatus.FAILED_LOAD_STEP,
        validator=attr.validators.in_(DirectIngestRawDataImportSessionStatus),
    )

    def __str__(self) -> str:
        return f"{self.error_type.value} for [{self.file_tag}] for [{self.file_paths}] failed with:\n\n{self.error_msg}"

    def serialize(self) -> str:
        result_dict = {
            "file_paths": [path.uri() for path in self.file_paths],
            "file_tag": self.file_tag,
            "update_datetime": self.update_datetime.isoformat(),
            "error_msg": self.error_msg,
            "error_type": self.error_type.value,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawFileLoadAndPrepError":
        data = json.loads(json_str)
        return RawFileLoadAndPrepError(
            error_type=DirectIngestRawDataImportSessionStatus(data["error_type"]),
            file_tag=data["file_tag"],
            update_datetime=datetime.datetime.fromisoformat(data["update_datetime"]),
            file_paths=data["file_paths"],
            error_msg=data["error_msg"],
        )


@attr.define
class RawDataAppendImportError(RawDataImportError):
    """Represents an error that occurred during the big query load step of the raw data
    import dag, specifically during the append stage.

    Attributes:
        raw_temp_table (BigQueryAddress): the address of the temp big query address that
            stores the raw data that failed append to the raw data table
        error_type (DirectIngestRawDataImportSessionStatus): defaults to FAILED_LOAD_STEP
        error_msg (str): an error message, including any relevant stack traces.
    """

    raw_temp_table: BigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddress)
    )
    error_type: DirectIngestRawDataImportSessionStatus = attr.ib(
        default=DirectIngestRawDataImportSessionStatus.FAILED_LOAD_STEP,
        validator=attr.validators.in_(DirectIngestRawDataImportSessionStatus),
    )

    def __str__(self) -> str:
        return f"{self.error_type.value} writing from {self.raw_temp_table.to_str()} to raw data table failed with:\n\n{self.error_msg}"

    def serialize(self) -> str:
        result_dict = {
            "raw_temp_table": self.raw_temp_table.to_str(),
            "error_msg": self.error_msg,
            "error_type": self.error_type.value,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawDataAppendImportError":
        data = json.loads(json_str)
        return RawDataAppendImportError(
            error_type=DirectIngestRawDataImportSessionStatus(data["error_type"]),
            raw_temp_table=BigQueryAddress.from_str(data["raw_temp_table"]),
            error_msg=data["error_msg"],
        )


@attr.define
class RequiresPreImportNormalizationFile(BaseResult):
    """Encapsulates the path, the headers, the chunk boundaries for the file
    and the type of normalization required for the CSV to conform to
    BigQuery's load job standards.

    Attributes:
        path (GcsfsFilePath): file path for the raw data file sent by the state that
            requires pre-import normalization
        pre_import_normalization_type (PreImportNormalizationType): the type of
            pre-import normalization required to make this raw data file readable by
            big query's load job.
        chunk_boundaries (List[CsvChunkBoundary]): the start_inclusive and end_exclusive
            byte offsets that specifies where each pre-import normalization process should
            start and stop reading from.
        headers (List[str]): the column headers for path
    """

    path: GcsfsFilePath = attr.ib(validator=attr.validators.instance_of(GcsfsFilePath))
    pre_import_normalization_type: PreImportNormalizationType = attr.ib(
        validator=attr.validators.in_(PreImportNormalizationType)
    )
    chunk_boundaries: List[CsvChunkBoundary] = attr.ib(
        validator=attr_validators.is_list_of(CsvChunkBoundary)
    )
    headers: List[str] = attr.ib(validator=attr_validators.is_list_of(str))

    def serialize(self) -> str:
        return json.dumps(
            {
                "path": self.path.abs_path(),
                "normalization_type": (self.pre_import_normalization_type.value),
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
            path=GcsfsFilePath.from_absolute_path(data["path"]),
            pre_import_normalization_type=PreImportNormalizationType(
                data["normalization_type"]
            ),
            chunk_boundaries=[
                CsvChunkBoundary.deserialize(boundary)
                for boundary in data["chunk_boundaries"]
            ],
            headers=data["headers"],
        )

    def to_file_chunks(self) -> List["RequiresPreImportNormalizationFileChunk"]:
        """Generates a list of RequiresPreImportNormalizationFileChunk objects, one per
        chunk_boundary.
        """
        individual_chunks = []
        for chunk_boundary in self.chunk_boundaries:
            chunk = RequiresPreImportNormalizationFileChunk(
                path=self.path,
                pre_import_normalization_type=self.pre_import_normalization_type,
                chunk_boundary=chunk_boundary,
                headers=self.headers,
            )
            individual_chunks.append(chunk)
        return individual_chunks


@attr.define
class RequiresPreImportNormalizationFileChunk(BaseResult):
    """Encapsulates the path, the chunk boundary and the type of normalization required
    for the CSV to conform to BigQuery's load job standards.

    Attributes:
        path (GcsfsFilePath): file path for the raw data file sent by the state that
            requires pre-import normalization
        pre_import_normalization_type (PreImportNormalizationType | None): the type
            of pre-import normalization required to make this raw data file readable
            by big query's load job.
        chunk_boundary (CsvChunkBoundary): the start_inclusive and end_exclusive byte
            offsets that specifies where the pre-import normalization process should
            start and stop reading from.
        headers (List[str]): the column headers for path

    """

    path: GcsfsFilePath = attr.ib(validator=attr.validators.instance_of(GcsfsFilePath))
    pre_import_normalization_type: Optional[PreImportNormalizationType]
    chunk_boundary: CsvChunkBoundary = attr.ib(
        validator=attr.validators.instance_of(CsvChunkBoundary)
    )
    headers: List[str] = attr.ib(validator=attr_validators.is_list_of(str))

    def serialize(self) -> str:
        return json.dumps(
            {
                "path": self.path.abs_path(),
                "normalization_type": (
                    self.pre_import_normalization_type.value
                    if self.pre_import_normalization_type
                    else ""
                ),
                "chunk_boundary": self.chunk_boundary.serialize(),
                "headers": self.headers,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RequiresPreImportNormalizationFileChunk":
        data = json.loads(json_str)
        return RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(data["path"]),
            pre_import_normalization_type=PreImportNormalizationType(
                data["normalization_type"]
            ),
            chunk_boundary=CsvChunkBoundary.deserialize(data["chunk_boundary"]),
            headers=data["headers"],
        )


@attr.define
class PreImportNormalizedCsvChunkResult(BaseResult):
    """Encapsulates the output path, chunk boundary and checksum of the CSV chunk. This
    should relate 1-1 to a RequiresPreImportNormalizationFileChunk.

    Attributes:
        input_file_path (GcsfsFilePath): file path for the raw data file sent by the
            state that requires pre-import normalization
        output_file_path (GcsfsFilePath): file path for the chunk of normalized data from
            input_file_path that was specified in chunk_boundary
        chunk_boundary (CsvChunkBoundary): the start_inclusive and end_exclusive byte
            offsets that specifies where the pre-import normalization process should
            start and stop reading from input_file_path
        crc32c (int): the checksum of the bytes read from input_file_path
    """

    input_file_path: GcsfsFilePath = attr.ib(
        validator=attr.validators.instance_of(GcsfsFilePath)
    )
    output_file_path: GcsfsFilePath = attr.ib(
        validator=attr.validators.instance_of(GcsfsFilePath)
    )
    chunk_boundary: CsvChunkBoundary = attr.ib(
        validator=attr.validators.instance_of(CsvChunkBoundary)
    )
    crc32c: int = attr.ib(validator=attr_validators.is_int)

    def get_chunk_boundary_size(self) -> int:
        return self.chunk_boundary.get_chunk_size()

    def serialize(self) -> str:
        result_dict = {
            "input_file_path": self.input_file_path.abs_path(),
            "output_file_path": self.output_file_path.abs_path(),
            "chunk_boundary": self.chunk_boundary.serialize(),
            "crc32c": self.crc32c,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "PreImportNormalizedCsvChunkResult":
        data = json.loads(json_str)
        return PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path(data["input_file_path"]),
            output_file_path=GcsfsFilePath.from_absolute_path(data["output_file_path"]),
            chunk_boundary=CsvChunkBoundary.deserialize(data["chunk_boundary"]),
            crc32c=data["crc32c"],
        )


@attr.define
class PreImportNormalizedFileResult(BaseResult):
    """The end result of the pre-import normalization process, containing both the
    original input file path, as well as a list of all normalized file chunk output
    paths

    Attributes:
        input_file_path (GcsfsFilePath): file path for the raw data file sent by the
            state that required pre-import normalization
        output_file_paths (List[GcsfsFilePath]): list of file paths for the chunks of
            normalized data from input_file_path that were normalized to be readable by
            the big query load job
    """

    input_file_path: GcsfsFilePath = attr.ib(
        validator=attr.validators.instance_of(GcsfsFilePath)
    )
    output_file_paths: List[GcsfsFilePath] = attr.ib(
        validator=attr_validators.is_list_of(GcsfsFilePath)
    )

    def serialize(self) -> str:
        result_dict = {
            "input_file_path": self.input_file_path.abs_path(),
            "output_file_paths": [path.abs_path() for path in self.output_file_paths],
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "PreImportNormalizedFileResult":
        data = json.loads(json_str)
        return PreImportNormalizedFileResult(
            input_file_path=GcsfsFilePath.from_absolute_path(data["input_file_path"]),
            output_file_paths=[
                GcsfsFilePath.from_absolute_path(path)
                for path in data["output_file_paths"]
            ],
        )


@attr.define
class ImportReadyFile(BaseResult):
    """Base information required for all tasks in the big query load step.

    Attributes:
        file_id (int): file_id of the conceptual file being loaded
        file_tag (str): file_tag of the file being loaded
        update_datetime (datetime.datetime): the update_datetime associated with this file_id
        file_paths (List[GcsfsFilePath]): the raw file paths in GCS associated with this
            file_id to be loaded into BigQuery
        original_file_paths (Optional[List[GcsfsFilePath]]): if pre-import normalization was
            required, the list file paths of the files sent to us by the state before
            pre-import normalization was performed. Otherwise, None.
    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    file_tag: str = attr.ib(validator=attr_validators.is_str)
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    file_paths: List[GcsfsFilePath] = attr.ib(
        validator=attr_validators.is_list_of(GcsfsFilePath)
    )
    original_file_paths: Optional[List[GcsfsFilePath]] = attr.ib(
        validator=attr.validators.optional(attr_validators.is_list_of(GcsfsFilePath))
    )

    def serialize(self) -> str:
        return json.dumps(
            {
                "file_id": self.file_id,
                "file_tag": self.file_tag,
                "update_datetime": self.update_datetime.isoformat(),
                "file_paths": [path.uri() for path in self.file_paths],
                "original_file_paths": (
                    None
                    if self.original_file_paths is None
                    else [path.uri() for path in self.original_file_paths]
                ),
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "ImportReadyFile":
        data = json.loads(json_str)
        return ImportReadyFile(
            file_id=data["file_id"],
            file_tag=data["file_tag"],
            update_datetime=datetime.datetime.fromisoformat(data["update_datetime"]),
            file_paths=[
                GcsfsFilePath.from_absolute_path(path) for path in data["file_paths"]
            ],
            original_file_paths=(
                None
                if data["original_file_paths"] is None
                else [
                    GcsfsFilePath.from_absolute_path(path)
                    for path in data["original_file_paths"]
                ]
            ),
        )

    @classmethod
    def from_bq_metadata(
        cls, bq_metadata: "RawBigQueryFileMetadataSummary"
    ) -> "ImportReadyFile":
        return ImportReadyFile(
            file_id=assert_type(bq_metadata.file_id, int),
            file_tag=bq_metadata.file_tag,
            file_paths=[gcs_file.path for gcs_file in bq_metadata.gcs_files],
            update_datetime=bq_metadata.update_datetime,
            original_file_paths=None,
        )

    @classmethod
    def from_bq_metadata_and_normalized_chunk_result(
        cls,
        bq_metadata: "RawBigQueryFileMetadataSummary",
        input_path_to_normalized_chunk_results: Dict[
            GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]
        ],
    ) -> "ImportReadyFile":
        return ImportReadyFile(
            file_id=assert_type(bq_metadata.file_id, int),
            file_tag=bq_metadata.file_tag,
            file_paths=[
                normalized_chunk.output_file_path
                for normalized_chunks in input_path_to_normalized_chunk_results.values()
                for normalized_chunk in normalized_chunks
            ],
            update_datetime=bq_metadata.update_datetime,
            original_file_paths=list(input_path_to_normalized_chunk_results),
        )


@attr.define
class AppendReadyFile(BaseResult):
    """Summary from DirectIngestRawFileLoadManager.load_and_prep_paths step that will
    be combined with AppendSummary to build a row in the
    direct_ingest_raw_data_import_session operations table.

    Attributes:
        append_ready_table_address (str): temp BQ address of loaded, transformed and
            migrated raw data
        raw_rows_count (int): number of raw rows loaded from raw file paths before any
            transformations or filtering occurred
    """

    import_ready_file: ImportReadyFile
    append_ready_table_address: BigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddress)
    )
    raw_rows_count: int = attr.ib(validator=attr_validators.is_int)

    def serialize(self) -> str:
        return json.dumps(
            [
                self.import_ready_file.serialize(),
                self.append_ready_table_address.to_str(),
                self.raw_rows_count,
            ]
        )

    @staticmethod
    def deserialize(json_str: str) -> "AppendReadyFile":
        data = json.loads(json_str)
        return AppendReadyFile(
            import_ready_file=ImportReadyFile.deserialize(data[0]),
            append_ready_table_address=BigQueryAddress.from_str(data[1]),
            raw_rows_count=data[2],
        )


@attr.define
class AppendSummary(BaseResult):
    """Summary from DirectIngestRawFileLoadManager.append_to_raw_data_table step that
    will be combined with AppendReadyFile to build a row in the
    direct_ingest_raw_data_import_session operations table.

    Attributes:
        file_id (int): file_id associated with this append summary
        net_new_or_updated_rows(int | None): the number of net new or updated rows added
            during the diffing process
        deleted_rows(int | None): the number of rows added with is_deleted as True during
            the historical diffing process

    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    net_new_or_updated_rows: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    deleted_rows: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    def serialize(self) -> str:
        return json.dumps(
            [
                self.file_id,
                self.net_new_or_updated_rows,
                self.deleted_rows,
            ]
        )

    @staticmethod
    def deserialize(json_str: str) -> "AppendSummary":
        data = json.loads(json_str)
        return AppendSummary(
            file_id=data[0],
            net_new_or_updated_rows=data[1],
            deleted_rows=data[2],
        )


@attr.define
class AppendReadyFileBatch(BaseResult):
    """Contains "batches" of AppendReadyFile objects grouped by file_tag.

    Attributes:
        append_ready_files_by_tag (Dict[str, List[AppendReadyFile]]): AppendReadyFile
            objects grouped by file tags to be consumed by a single task.

    """

    append_ready_files_by_tag: Dict[str, List[AppendReadyFile]] = attr.ib(
        validator=attr_validators.is_dict
    )

    def serialize(self) -> str:
        return json.dumps(
            {
                file_tag: [
                    append_ready_file.serialize()
                    for append_ready_file in append_ready_files_for_tag
                ]
                for file_tag, append_ready_files_for_tag in self.append_ready_files_by_tag.items()
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "AppendReadyFileBatch":
        data = json.loads(json_str)
        append_ready_files_by_tag = {
            file_tag: [
                AppendReadyFile.deserialize(append_ready_file_str)
                for append_ready_file_str in append_ready_files_str
            ]
            for file_tag, append_ready_files_str in assert_type(data, dict).items()
        }

        return AppendReadyFileBatch(append_ready_files_by_tag=append_ready_files_by_tag)


@attr.define
class RawGCSFileMetadataSummary(BaseResult):
    """This represents metadata about a single literal file (e.g. a CSV) that is stored
    in GCS

    Attributes:
        gcs_file_id (int): an id that corresponds to the literal file in Google Cloud
            Storage in direct_ingest_raw_gcs_file_metadata. A single file will always
            have a single gcs_file_id.
        file_id (int | None): a "conceptual" file id that corresponds to a single,
            conceptual file sent to us by the state. For raw files states send us in
            chunks (such as ContactNoteComment), each literal CSV that makes up the
            whole file will have a different gcs_file_id, but all of those entries will
            have the same file_id.
        path (GcsfsFilePath): the path in Google Cloud Storage of the file.
    """

    gcs_file_id: int = attr.ib(validator=attr_validators.is_int)
    file_id: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)
    path: GcsfsFilePath = attr.ib(validator=attr.validators.instance_of(GcsfsFilePath))

    @cached_property
    def parts(self) -> DirectIngestRawFilenameParts:
        return filename_parts_from_path(self.path)

    def serialize(self) -> str:
        return json.dumps([self.gcs_file_id, self.file_id, self.path.abs_path()])

    @staticmethod
    def deserialize(json_str: str) -> "RawGCSFileMetadataSummary":
        data = assert_type(json.loads(json_str), list)
        return RawGCSFileMetadataSummary(
            gcs_file_id=data[0],
            file_id=data[1],
            path=GcsfsFilePath.from_absolute_path(assert_type(data[2], str)),
        )

    @classmethod
    def from_gcs_metadata_table_row(
        cls, db_record: Tuple[int, Optional[int], str], abs_path: GcsfsFilePath
    ) -> "RawGCSFileMetadataSummary":
        return RawGCSFileMetadataSummary(
            gcs_file_id=db_record[0], file_id=db_record[1], path=abs_path
        )


@attr.define
class RawBigQueryFileMetadataSummary(BaseResult):
    """This represents metadata about a "conceptual" file_id that exists in BigQuery,
    made up of at least one literal csv file |gcs_files|.

    Attributes:
        gcs_files (list[RawGCSFileMetadataSummary]): a list of RawGCSFileMetadataSummary
            objects that correspond to the literal csv files that comprise this single,
            conceptual file.
        file_tag (str): the shared file_tag from |gcs_files| cached on this object.
        file_id (int | None): a "conceptual" file id that corresponds to a single,
            conceptual file sent to us by the state. For raw files states send us in
            chunks (such as ContactNoteComment), each literal CSV that makes up the
            whole file will have a different gcs_file_id, but all of those entries will
            have the same file_id.
        update_datetime (datetime.datetime): the max update_datetime from |gcs_files|
    """

    gcs_files: List[RawGCSFileMetadataSummary] = attr.ib(
        validator=attr_validators.is_list_of(RawGCSFileMetadataSummary)
    )
    file_tag: str = attr.ib(validator=attr_validators.is_str)
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    file_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    def serialize(self) -> str:
        return json.dumps(
            {
                "file_tag": self.file_tag,
                "file_id": self.file_id,
                "gcs_files": [file.serialize() for file in self.gcs_files],
                "update_datetime": self.update_datetime.isoformat(),
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RawBigQueryFileMetadataSummary":
        data = assert_type(json.loads(json_str), dict)
        return RawBigQueryFileMetadataSummary(
            file_tag=data["file_tag"],
            file_id=data["file_id"],
            gcs_files=[
                RawGCSFileMetadataSummary.deserialize(gcs_file_str)
                for gcs_file_str in data["gcs_files"]
            ],
            update_datetime=datetime.datetime.fromisoformat(data["update_datetime"]),
        )

    @classmethod
    def from_gcs_files(
        cls, gcs_files: List[RawGCSFileMetadataSummary]
    ) -> "RawBigQueryFileMetadataSummary":
        return RawBigQueryFileMetadataSummary(
            file_id=gcs_files[0].file_id,
            file_tag=gcs_files[0].parts.file_tag,
            gcs_files=gcs_files,
            update_datetime=max(
                gcs_files,
                key=lambda x: x.parts.utc_upload_datetime,
            ).parts.utc_upload_datetime,
        )
