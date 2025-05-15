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
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import (
    is_big_query_valid_delimiter,
    is_big_query_valid_encoding,
    is_big_query_valid_line_terminator,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.common import attr_validators
from recidiviz.common.constants.csv import DEFAULT_CSV_ENCODING
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.gcs.filename_parts import (
    DirectIngestRawFilenameParts,
    filename_parts_from_path,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.utils.airflow_types import BaseError, BaseResult
from recidiviz.utils.types import assert_type

# --------------------------------------------------------------------------------------
# v                        raw data import error types                                 v
# --------------------------------------------------------------------------------------


@attr.define
class RawDataImportError(BaseError):
    """The base error type for all errors that exist in the raw data import dag.

    Attributes:
        error_type (DirectIngestRawFileImportStatus): the error type that
            corresponds to where the error occurred within the raw data import dag
        error_msg (str): an error message, including any relevant stack traces.

    """

    error_type: DirectIngestRawFileImportStatus = attr.ib(
        validator=attr.validators.instance_of(DirectIngestRawFileImportStatus)
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
        original_file_path (GcsfsFilePath): the path to the original file sent to us
            by the state associated with this error
        temporary_file_paths (Optional[List[GcsfsFilePath]]): any temporary files
            created during pre-import normalization
        error_type (DirectIngestRawFileImportStatus): defaults to
            FAILED_PRE_IMPORT_NORMALIZATION_STEP.
        error_msg (str): an error message, including any relevant stack traces.

    """

    original_file_path: GcsfsFilePath = attr.ib(
        validator=attr.validators.instance_of(GcsfsFilePath)
    )
    temporary_file_paths: Optional[List[GcsfsFilePath]] = attr.ib(
        validator=attr.validators.optional(attr_validators.is_list_of(GcsfsFilePath))
    )
    error_type: DirectIngestRawFileImportStatus = attr.ib(
        default=DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
        validator=attr.validators.in_(DirectIngestRawFileImportStatus),
    )

    def __str__(self) -> str:
        return f"{self.error_type.value} with {self.original_file_path.abs_path()} failed with:\n\n{self.error_msg}"

    @cached_property
    def parts(self) -> DirectIngestRawFilenameParts:
        return filename_parts_from_path(self.original_file_path)

    def serialize(self) -> str:
        result_dict = {
            "original_file_path": self.original_file_path.abs_path(),
            "temporary_file_paths": (
                [path.abs_path() for path in self.temporary_file_paths]
                if self.temporary_file_paths
                else None
            ),
            "error_msg": self.error_msg,
            "error_type": self.error_type.value,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawFileProcessingError":
        data = json.loads(json_str)
        return RawFileProcessingError(
            error_type=DirectIngestRawFileImportStatus(data["error_type"]),
            original_file_path=GcsfsFilePath.from_absolute_path(
                data["original_file_path"]
            ),
            temporary_file_paths=(
                [
                    GcsfsFilePath.from_absolute_path(path)
                    for path in data["temporary_file_paths"]
                ]
                if data["temporary_file_paths"]
                else None
            ),
            error_msg=data["error_msg"],
        )


@attr.define
class RawFileLoadAndPrepError(RawDataImportError):
    """Represents an error that occurred during a raw data big query load step, specifically
    during the load and prep stage where we load raw data files into big query.

    Attributes:
        file_id (int): file_id for the file paths that failed to import to big query
        file_tag (str): file_tag associated with the file_paths
        update_datetime(datetime.datetime): update_datetime associated with the
            file_paths
        original_file_paths (List[GcsfsFilePath]): the paths to the original file sent to
            us by the state associated with this error
        pre_import_normalized_file_paths (Optional[List[GcsfsFilePath]]): temporary files
            created during pre-import normalization
        temp_table (Optional[BigQueryAddress]): temporary big query table created during
            the load step
        error_type (DirectIngestRawFileImportStatus): defaults to FAILED_LOAD_STEP
        error_msg (str): an error message, including any relevant stack traces.
    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    file_tag: str = attr.ib(validator=attr_validators.is_str)
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    pre_import_normalized_file_paths: Optional[List[GcsfsFilePath]] = attr.ib(
        validator=attr.validators.optional(attr_validators.is_list_of(GcsfsFilePath))
    )
    original_file_paths: List[GcsfsFilePath] = attr.ib(
        validator=attr_validators.is_list_of(GcsfsFilePath)
    )
    temp_table: Optional[BigQueryAddress] = attr.ib(
        validator=attr.validators.optional(attr.validators.instance_of(BigQueryAddress))
    )
    error_type: DirectIngestRawFileImportStatus = attr.ib(
        default=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
        validator=attr.validators.in_(DirectIngestRawFileImportStatus),
    )

    def __str__(self) -> str:
        file_paths_str = "\n* ".join(
            [path.abs_path() for path in self.original_file_paths]
        )
        return f"{self.error_type.value} for [{self.file_tag}] for [{file_paths_str}] failed with:\n\n{self.error_msg}"

    def serialize(self) -> str:
        result_dict = {
            "file_id": self.file_id,
            "original_paths": [path.uri() for path in self.original_file_paths],
            "normalized_paths": (
                None
                if self.pre_import_normalized_file_paths is None
                else [path.uri() for path in self.pre_import_normalized_file_paths]
            ),
            "file_tag": self.file_tag,
            "update_datetime": self.update_datetime.isoformat(),
            "error_msg": self.error_msg,
            "error_type": self.error_type.value,
            "temp_table": None if self.temp_table is None else self.temp_table.to_str(),
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawFileLoadAndPrepError":
        data = json.loads(json_str)
        return RawFileLoadAndPrepError(
            file_id=data["file_id"],
            error_type=DirectIngestRawFileImportStatus(data["error_type"]),
            temp_table=(
                None
                if data["temp_table"] is None
                else BigQueryAddress.from_str(data["temp_table"])
            ),
            file_tag=data["file_tag"],
            update_datetime=datetime.datetime.fromisoformat(data["update_datetime"]),
            original_file_paths=[
                GcsfsFilePath.from_absolute_path(path)
                for path in data["original_paths"]
            ],
            pre_import_normalized_file_paths=(
                None
                if data["normalized_paths"] is None
                else [
                    GcsfsFilePath.from_absolute_path(path)
                    for path in data["normalized_paths"]
                ]
            ),
            error_msg=data["error_msg"],
        )


@attr.define
class RawDataAppendImportError(RawDataImportError):
    """Represents an error that occurred during the big query load step of the raw data
    import dag, specifically during the append stage.

    Attributes:
        file_id (int): file_id that failed to append to the raw data table
        raw_temp_table (BigQueryAddress): the address of the temp big query address that
            stores the raw data that failed append to the raw data table
        error_type (DirectIngestRawFileImportStatus): defaults to FAILED_LOAD_STEP
        error_msg (str): an error message, including any relevant stack traces.
    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    raw_temp_table: BigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddress)
    )
    error_type: DirectIngestRawFileImportStatus = attr.ib(
        default=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
        validator=attr.validators.in_(DirectIngestRawFileImportStatus),
    )

    def __str__(self) -> str:
        return f"{self.error_type.value} writing from {self.raw_temp_table.to_str()} to raw data table failed with:\n\n{self.error_msg}"

    @property
    def file_tag(self) -> str:
        return self.raw_temp_table.table_id.split("_")[0]

    def serialize(self) -> str:
        result_dict = {
            "file_id": self.file_id,
            "raw_temp_table": self.raw_temp_table.to_str(),
            "error_msg": self.error_msg,
            "error_type": self.error_type.value,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawDataAppendImportError":
        data = json.loads(json_str)
        return RawDataAppendImportError(
            error_type=DirectIngestRawFileImportStatus(data["error_type"]),
            raw_temp_table=BigQueryAddress.from_str(data["raw_temp_table"]),
            error_msg=data["error_msg"],
            file_id=data["file_id"],
        )


@attr.define
class RawDataFilesSkippedError(BaseError):
    """Represents any sort of error where there's bad / inconsistent state that prevents
    us from importing a given file or set of files, but which doesn't actually have to
    do with the contents of the files itself
    """

    file_paths: List[GcsfsFilePath] = attr.ib(
        validator=attr_validators.is_list_of(GcsfsFilePath)
    )
    skipped_message: str = attr.ib(validator=attr_validators.is_str)
    file_tag: str = attr.ib(validator=attr_validators.is_str)
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )

    def __str__(self) -> str:
        file_paths_str = "\n* ".join([path.abs_path() for path in self.file_paths])
        return f"Skipping import for [{self.file_tag}] for [{file_paths_str}] w/ message: \n {self.skipped_message}]"

    @classmethod
    def from_gcs_files_and_message(
        cls,
        *,
        skipped_message: str,
        gcs_files: list["RawGCSFileMetadata"],
        file_tag: str,
    ) -> "RawDataFilesSkippedError":
        return RawDataFilesSkippedError(
            file_paths=[gcs_file.path for gcs_file in gcs_files],
            skipped_message=skipped_message,
            file_tag=file_tag,
            update_datetime=max(
                gcs_file.parts.utc_upload_datetime for gcs_file in gcs_files
            ),
        )

    def serialize(self) -> str:
        result_dict = {
            "file_paths": [path.uri() for path in self.file_paths],
            "skipped_message": self.skipped_message,
            "file_tag": self.file_tag,
            "update_datetime": self.update_datetime.isoformat(),
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "RawDataFilesSkippedError":
        data = json.loads(json_str)
        return RawDataFilesSkippedError(
            file_paths=[
                GcsfsFilePath.from_absolute_path(path) for path in data["file_paths"]
            ],
            skipped_message=data["skipped_message"],
            file_tag=data["file_tag"],
            update_datetime=datetime.datetime.fromisoformat(data["update_datetime"]),
        )


# --------------------------------------------------------------------------------------
# ^                        raw data import error types                                 ^
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# v                        step 2: processing logic types                              v
# --------------------------------------------------------------------------------------


@attr.define
class RawDataResourceLock(BaseResult):
    """Represent information about a raw data resource lock

    Attributes:
        lock_id (int): the pk of the raw data resource lock
        lock_resource (DirectIngestRawDataResourceLockResource): the raw data infra
            resource that this lock corresponds to
        released (bool): whether or not this lock has been released
    """

    lock_id: int = attr.ib(validator=attr_validators.is_int)
    lock_resource: DirectIngestRawDataResourceLockResource = attr.ib(
        validator=attr.validators.in_(DirectIngestRawDataResourceLockResource)
    )
    released: bool = attr.ib(validator=attr_validators.is_bool)

    def serialize(self) -> str:
        return json.dumps(
            {
                "lock_id": self.lock_id,
                "lock_resource": self.lock_resource.value,
                "released": self.released,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RawDataResourceLock":
        data = json.loads(json_str)
        return RawDataResourceLock(
            lock_id=data["lock_id"],
            lock_resource=DirectIngestRawDataResourceLockResource(
                data["lock_resource"]
            ),
            released=data["released"],
        )

    @classmethod
    def from_table_row(cls, row: Tuple[int, str, bool]) -> "RawDataResourceLock":
        return cls(
            lock_id=row[0],
            lock_resource=DirectIngestRawDataResourceLockResource(row[1]),
            released=row[2],
        )


@attr.define
class RawGCSFileMetadata(BaseResult):
    """This represents metadata about a single literal file (e.g. a CSV) that is stored
    in GCS.

    n.b. this object's schema reflects the database schema defined in
    recidiviz/persistence/database/schema/operations/schema.py, but this object is
    populated by raw sql queries in the airflow context.

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

    @property
    def abs_path(self) -> str:
        return self.path.abs_path()

    def serialize(self) -> str:
        return json.dumps([self.gcs_file_id, self.file_id, self.path.abs_path()])

    @staticmethod
    def deserialize(json_str: str) -> "RawGCSFileMetadata":
        data = assert_type(json.loads(json_str), list)
        return RawGCSFileMetadata(
            gcs_file_id=data[0],
            file_id=data[1],
            path=GcsfsFilePath.from_absolute_path(assert_type(data[2], str)),
        )

    @classmethod
    def from_gcs_metadata_table_row(
        cls, db_record: Tuple[int, Optional[int], str], abs_path: GcsfsFilePath
    ) -> "RawGCSFileMetadata":
        return RawGCSFileMetadata(
            gcs_file_id=db_record[0], file_id=db_record[1], path=abs_path
        )


@attr.define
class RawBigQueryFileMetadata(BaseResult):
    """This represents metadata about a "conceptual" file_id that exists in BigQuery,
    made up of at least one literal csv file |gcs_files|.

    n.b. this object's schema reflects the database schema defined in
    recidiviz/persistence/database/schema/operations/schema.py, but this object is
    populated by raw sql queries in the airflow context.

    Attributes:
        gcs_files (List[RawGCSFileMetadata]): a list of RawGCSFileMetadata
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

    gcs_files: List[RawGCSFileMetadata] = attr.ib(
        validator=attr_validators.is_list_of(RawGCSFileMetadata)
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
    def deserialize(json_str: str) -> "RawBigQueryFileMetadata":
        data = assert_type(json.loads(json_str), dict)
        return RawBigQueryFileMetadata(
            file_tag=data["file_tag"],
            file_id=data["file_id"],
            gcs_files=[
                RawGCSFileMetadata.deserialize(gcs_file_str)
                for gcs_file_str in data["gcs_files"]
            ],
            update_datetime=datetime.datetime.fromisoformat(data["update_datetime"]),
        )

    @classmethod
    def from_gcs_files(
        cls, gcs_files: List[RawGCSFileMetadata]
    ) -> "RawBigQueryFileMetadata":
        return RawBigQueryFileMetadata(
            file_id=gcs_files[0].file_id,
            file_tag=gcs_files[0].parts.file_tag,
            gcs_files=gcs_files,
            update_datetime=max(
                gcs_files,
                key=lambda x: x.parts.utc_upload_datetime,
            ).parts.utc_upload_datetime,
        )


@attr.define
class RawFileBigQueryLoadConfig(BaseResult):
    """Represents the configuration that will be used to create the temporary table
    in Big Query for the initial raw data load.

    Attributes:
        schema_fields: List[bigquery.SchemaField]: represents the column names and
            their descriptions in the order that they appear in the raw file, or as they
            appear in the raw file config if the headers are not present in the raw file.
            All fields are nullable and are of type STRING.
        skip_leading_rows: int: the number of rows to skip when loading the raw file to BQ.
            If the raw file contains headers and does not require normalization, the file
            will be uploaded as is, so this value should be 1 so the header row is skipped.
            If the raw file contains headers and requires normalization, the first normalized
            chunk will begin at the second row of the file, so this value should be 0.
            If the raw file does not contain headers, this should be 0."""

    schema_fields: List[bigquery.SchemaField] = attr.ib(
        validator=attr_validators.is_list_of(bigquery.SchemaField)
    )
    skip_leading_rows: int = attr.ib(validator=attr_validators.is_int)

    def serialize(self) -> str:
        return json.dumps(
            {
                "schema_fields": [
                    {
                        "column": schema.name,
                        "description": schema.description,
                    }
                    for schema in self.schema_fields
                ],
                "skip_leading_rows": self.skip_leading_rows,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RawFileBigQueryLoadConfig":
        data = json.loads(json_str)
        return RawFileBigQueryLoadConfig(
            schema_fields=[
                RawDataTableBigQuerySchemaBuilder.raw_file_column_as_bq_field(
                    schema["column"], schema["description"]
                )
                for schema in data["schema_fields"]
            ],
            skip_leading_rows=data["skip_leading_rows"],
        )

    @classmethod
    def from_raw_file_config(
        cls,
        raw_file_config: DirectIngestRawFileConfig,
    ) -> "RawFileBigQueryLoadConfig":
        raw_file_contains_headers = not raw_file_config.infer_columns_from_config
        raw_file_requires_normalization = (
            PreImportNormalizationType.required_pre_import_normalization_type(
                raw_file_config
            )
            is not None
        )

        return cls(
            schema_fields=RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=raw_file_config,
                include_recidiviz_managed_fields=False,
            ),
            skip_leading_rows=(
                1
                if raw_file_contains_headers and not raw_file_requires_normalization
                else 0
            ),
        )

    @classmethod
    def from_headers_and_raw_file_config(
        cls,
        file_headers: List[str],
        raw_file_config: DirectIngestRawFileConfig,
    ) -> "RawFileBigQueryLoadConfig":
        raw_file_contains_headers = not raw_file_config.infer_columns_from_config
        raw_file_requires_normalization = (
            PreImportNormalizationType.required_pre_import_normalization_type(
                raw_file_config
            )
            is not None
        )

        return cls(
            schema_fields=RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config_from_columns(
                raw_file_config=raw_file_config,
                include_recidiviz_managed_fields=False,
                columns=file_headers,
            ),
            skip_leading_rows=(
                1
                if raw_file_contains_headers and not raw_file_requires_normalization
                else 0
            ),
        )


# --------------------------------------------------------------------------------------
# ^                        step 2: processing logic types                              ^
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# v                   step 3: pre-import normalization types                           v
# --------------------------------------------------------------------------------------


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
class RequiresPreImportNormalizationFile(BaseResult):
    """Encapsulates the path, the chunk boundaries for the file
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
    """

    path: GcsfsFilePath = attr.ib(validator=attr.validators.instance_of(GcsfsFilePath))
    pre_import_normalization_type: PreImportNormalizationType = attr.ib(
        validator=attr.validators.in_(PreImportNormalizationType)
    )
    chunk_boundaries: List[CsvChunkBoundary] = attr.ib(
        validator=attr_validators.is_list_of(CsvChunkBoundary)
    )

    @cached_property
    def parts(self) -> DirectIngestRawFilenameParts:
        return filename_parts_from_path(self.path)

    def serialize(self) -> str:
        return json.dumps(
            {
                "path": self.path.abs_path(),
                "normalization_type": (self.pre_import_normalization_type.value),
                "chunk_boundaries": [
                    boundary.serialize() for boundary in self.chunk_boundaries
                ],
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
    """

    path: GcsfsFilePath = attr.ib(validator=attr.validators.instance_of(GcsfsFilePath))
    pre_import_normalization_type: Optional[PreImportNormalizationType] = attr.ib(
        validator=attr.validators.optional(
            attr.validators.in_(PreImportNormalizationType)
        )
    )
    chunk_boundary: CsvChunkBoundary = attr.ib(
        validator=attr.validators.instance_of(CsvChunkBoundary)
    )

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
        num_unparseable_bytes (int): the number of un-parseable bytes found in this chunk
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
    byte_decoding_errors: list[str] = attr.ib(validator=attr_validators.is_list_of(str))

    def get_chunk_boundary_size(self) -> int:
        return self.chunk_boundary.get_chunk_size()

    def serialize(self) -> str:
        result_dict = {
            "input_file_path": self.input_file_path.abs_path(),
            "output_file_path": self.output_file_path.abs_path(),
            "chunk_boundary": self.chunk_boundary.serialize(),
            "crc32c": self.crc32c,
            "byte_decoding_errors": self.byte_decoding_errors,
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
            byte_decoding_errors=data["byte_decoding_errors"],
        )


# --------------------------------------------------------------------------------------
# ^                   step 3: pre-import normalization types                           ^
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# v                       step 4: big query load types                                 v
# --------------------------------------------------------------------------------------


@attr.define
class ImportReadyFile(BaseResult):
    """Encapsulates the information required for the big query load step. At this stage,
    |paths_to_load| is guaranteed to conform to big query load job's standards.

    Attributes:
        file_id (int): file_id of the conceptual file being loaded
        file_tag (str): file_tag of the file being loaded
        update_datetime (datetime.datetime): the update_datetime associated with this
            file_id
        pre_import_normalized_file_paths (List[GcsfsFilePath] | None): if pre-import
            normalization was required, these paths will be loaded into big query;
            otherwise |original_file_paths| will be loaded into big query and this will
            be None.
        original_file_paths (List[GcsfsFilePath]): files sent to us by the state. if
            pre-import normalization was not required, these paths will be loaded into
            big query; otherwise, |pre_import_normalized_file_paths| will be loaded into
            big query.
    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    file_tag: str = attr.ib(validator=attr_validators.is_str)
    update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    pre_import_normalized_file_paths: Optional[List[GcsfsFilePath]] = attr.ib(
        validator=attr.validators.optional(attr_validators.is_list_of(GcsfsFilePath))
    )
    original_file_paths: List[GcsfsFilePath] = attr.ib(
        validator=attr_validators.is_list_of(GcsfsFilePath)
    )
    bq_load_config: RawFileBigQueryLoadConfig = attr.ib(
        validator=attr.validators.instance_of(RawFileBigQueryLoadConfig),
    )

    @property
    def paths_to_load(self) -> List[GcsfsFilePath]:
        """Returns the paths to load into big query. If pre-import normalization was
        required (i.e. |pre_import_normalized_file_paths| is populated), returns
        |pre_import_normalized_file_paths|. Otherwise, returns |original_file_paths|.
        """
        return (
            self.pre_import_normalized_file_paths
            if self.pre_import_normalized_file_paths
            else self.original_file_paths
        )

    def serialize(self) -> str:
        return json.dumps(
            {
                "file_id": self.file_id,
                "file_tag": self.file_tag,
                "update_datetime": self.update_datetime.isoformat(),
                "original_paths": [path.uri() for path in self.original_file_paths],
                "normalized_paths": (
                    None
                    if self.pre_import_normalized_file_paths is None
                    else [path.uri() for path in self.pre_import_normalized_file_paths]
                ),
                "bq_load_config": self.bq_load_config.serialize(),
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "ImportReadyFile":
        data = json.loads(json_str)
        return ImportReadyFile(
            file_id=data["file_id"],
            file_tag=data["file_tag"],
            update_datetime=datetime.datetime.fromisoformat(data["update_datetime"]),
            original_file_paths=[
                GcsfsFilePath.from_absolute_path(path)
                for path in data["original_paths"]
            ],
            pre_import_normalized_file_paths=(
                None
                if data["normalized_paths"] is None
                else [
                    GcsfsFilePath.from_absolute_path(path)
                    for path in data["normalized_paths"]
                ]
            ),
            bq_load_config=RawFileBigQueryLoadConfig.deserialize(
                data["bq_load_config"]
            ),
        )

    @classmethod
    def from_bq_metadata_load_config_and_normalized_chunk_result(
        cls,
        bq_metadata: "RawBigQueryFileMetadata",
        bq_schema: "RawFileBigQueryLoadConfig",
        input_path_to_normalized_chunk_results: Dict[
            GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]
        ],
    ) -> "ImportReadyFile":
        return ImportReadyFile(
            file_id=assert_type(bq_metadata.file_id, int),
            file_tag=bq_metadata.file_tag,
            pre_import_normalized_file_paths=[
                normalized_chunk.output_file_path
                for normalized_chunks in input_path_to_normalized_chunk_results.values()
                for normalized_chunk in normalized_chunks
            ],
            update_datetime=bq_metadata.update_datetime,
            original_file_paths=list(input_path_to_normalized_chunk_results),
            bq_load_config=bq_schema,
        )

    @classmethod
    def from_bq_metadata_and_load_config(
        cls,
        bq_metadata: "RawBigQueryFileMetadata",
        bq_schema: "RawFileBigQueryLoadConfig",
    ) -> "ImportReadyFile":
        return ImportReadyFile(
            file_id=assert_type(bq_metadata.file_id, int),
            file_tag=bq_metadata.file_tag,
            original_file_paths=[gcs_file.path for gcs_file in bq_metadata.gcs_files],
            update_datetime=bq_metadata.update_datetime,
            pre_import_normalized_file_paths=None,
            bq_load_config=bq_schema,
        )


@attr.define
class AppendReadyFile(BaseResult):
    """Summary from DirectIngestRawFileLoadManager.load_and_prep_paths step that will
    be combined with AppendSummary to build a row in the
    direct_ingest_raw_file_import_run operations table.

    Attributes:
        import_ready_file (ImportReadyFile): metadata required for load_and_prep_paths
        append_ready_table_address (str): temp BQ address of loaded, transformed and
            migrated raw data
        raw_rows_count (int): number of raw rows loaded from raw file paths before any
            transformations or filtering occurred
    """

    import_ready_file: ImportReadyFile = attr.ib(
        validator=attr.validators.instance_of(ImportReadyFile)
    )
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
    direct_ingest_raw_file_import_run operations table.

    Attributes:
        file_id (int): file_id associated with this append summary
        net_new_or_updated_rows(int | None): the number of net new or updated rows added
            during the diffing process
        deleted_rows(int | None): the number of rows added with is_deleted as True during
            the historical diffing process
        historical_diffs_active (bool): whether or not a historical diff was executed
            during the append process

    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    historical_diffs_active: bool = attr.ib(validator=attr_validators.is_bool)
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
                self.historical_diffs_active,
            ]
        )

    @staticmethod
    def deserialize(json_str: str) -> "AppendSummary":
        data = json.loads(json_str)
        return AppendSummary(
            file_id=data[0],
            net_new_or_updated_rows=data[1],
            deleted_rows=data[2],
            historical_diffs_active=bool(data[3]),
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


# --------------------------------------------------------------------------------------
# ^                       step 4: big query load types                                 ^
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# v                      step 5: clean and storage types                               v
# --------------------------------------------------------------------------------------


@attr.define
class RawFileImport(BaseResult):
    """Metadata about an import session needed to write a new row to the
    direct_ingest_raw_file_import table.

    n.b. this object's schema reflects the database schema defined in
    recidiviz/persistence/database/schema/operations/schema.py, but this object is
    populated by raw sql queries in the airflow context.

    Attributes:
        file_id (int): conceptual file_id associated with the import
        import_status (DirectIngestRawFileImportStatus): the imports status
        historical_diffs_active (bool): whether or not historical diffs were performed
            during the import of this file (or would have been had the import
            successfully made it to that stage)
        error_message (str | None): error message, populated if import_status is fail-y
        raw_rows (int | None): total number of rows sent by the state across all files
            for this file_id
        net_new_or_updated_rows (int | None): if |historical_diffs_active| is True,
            the number of new or updated rows added to the raw data table by the
            historical diffing process
        deleted_rows (int | None): if |historical_diffs_active| is True, the number
            of rows with is_deleted=True added to the raw data table by the historical
            diffing process
    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    import_status: DirectIngestRawFileImportStatus = attr.ib(
        validator=attr.validators.in_(DirectIngestRawFileImportStatus)
    )
    historical_diffs_active: bool = attr.ib(validator=attr_validators.is_bool)
    error_message: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)
    raw_rows: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    net_new_or_updated_rows: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    deleted_rows: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    def error_message_quote_safe(self) -> Optional[str]:
        return self.error_message.replace("'", "''") if self.error_message else None

    def serialize(self) -> str:
        return json.dumps(
            {
                "file_id": self.file_id,
                "import_status": self.import_status.value,
                "historical_diffs_active": self.historical_diffs_active,
                "raw_rows": self.raw_rows,
                "net_new_or_updated_rows": self.net_new_or_updated_rows,
                "deleted_rows": self.deleted_rows,
                "error_message": self.error_message,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RawFileImport":
        data = assert_type(json.loads(json_str), dict)
        return RawFileImport(
            file_id=data["file_id"],
            import_status=DirectIngestRawFileImportStatus(data["import_status"]),
            historical_diffs_active=data["historical_diffs_active"],
            raw_rows=data["raw_rows"],
            net_new_or_updated_rows=data["net_new_or_updated_rows"],
            deleted_rows=data["deleted_rows"],
            error_message=data["error_message"],
        )

    @classmethod
    def from_load_results(
        cls, append_ready_file: AppendReadyFile, append_summary: AppendSummary
    ) -> "RawFileImport":
        return RawFileImport(
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            file_id=append_summary.file_id,
            raw_rows=append_ready_file.raw_rows_count,
            net_new_or_updated_rows=append_summary.net_new_or_updated_rows,
            deleted_rows=append_summary.deleted_rows,
            historical_diffs_active=append_summary.historical_diffs_active,
            error_message=None,
        )


@attr.define
class RawBigQueryFileProcessedTime(BaseResult):
    """Metadata for successfully imported files needed to set the proper
    file_processed_time on direct_ingest_raw_big_query_file_metadata table

    Attributes:
        file_id (int): conceptual file_id to update with |file_processed_time|
        file_processed_time (datetime.datetime): time at which the file import finished
    """

    file_id: int = attr.ib(validator=attr_validators.is_int)
    file_processed_time: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )

    def serialize(self) -> str:
        return json.dumps(
            {
                "file_id": self.file_id,
                "file_processed_time": self.file_processed_time.isoformat(),
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RawBigQueryFileProcessedTime":
        data = assert_type(json.loads(json_str), dict)
        return RawBigQueryFileProcessedTime(
            file_id=data["file_id"],
            file_processed_time=datetime.datetime.fromisoformat(
                data["file_processed_time"]
            ),
        )

    @classmethod
    def from_file_id_and_import_run_end(
        cls, file_id: int, import_run_end: datetime.datetime
    ) -> "RawBigQueryFileProcessedTime":
        return RawBigQueryFileProcessedTime(
            file_id=file_id, file_processed_time=import_run_end
        )


# --------------------------------------------------------------------------------------
# ^                      step 5: clean and storage types                               ^
# --------------------------------------------------------------------------------------
