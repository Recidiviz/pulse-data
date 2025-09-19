# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utilities for a raw file sandbox import."""
import base64
import datetime
import logging
import traceback
from collections import defaultdict
from enum import Enum
from typing import Dict, List, Optional, Tuple

import attr
import google_crc32c

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import (
    GcsfsCsvChunkBoundaryFinder,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_temporary_output_directory_path,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_header_reader import (
    DirectIngestRawFileHeaderReader,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager import (
    DirectIngestRawFileLoadManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_pre_import_normalizer import (
    DirectIngestRawFilePreImportNormalizer,
)
from recidiviz.ingest.direct.raw_data.raw_data_import_delegate_factory import (
    RawDataImportDelegateFactory,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    ImportReadyFile,
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawFileBigQueryLoadConfig,
    RawFileImport,
    RawGCSFileMetadata,
    RequiresPreImportNormalizationFile,
)
from recidiviz.utils.crc32c import digest_ordered_checksum_and_size_pairs

DEFAULT_SANDBOX_CHUNK_SIZE = 10 * 1024 * 1024  # 10 mb


def _id_for_file(state_code: StateCode, file_name: str) -> int:
    """Create an id for the file based on the file name.

    In production, the file id is an automatically incremented id created by Postgres.
    Here we generate one by hashing the state code and file name, which are the
    components of the primary key in the Postgres table.
    """
    checksum = google_crc32c.Checksum()
    checksum.update(state_code.value.encode())
    checksum.update(file_name.encode())
    return int.from_bytes(checksum.digest(), byteorder="big")


class SandboxImportStatus(Enum):
    SKIPPED = "skipped"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@attr.define(kw_only=True)
class SandboxConceptualFileImportResult:
    """Represents the import result of a conceptual raw data file"""

    paths: List[GcsfsFilePath] = attr.ib(
        validator=attr_validators.is_list_of(GcsfsFilePath)
    )
    status: SandboxImportStatus = attr.ib(
        validator=attr.validators.in_(SandboxImportStatus)
    )
    raw_rows_count: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    net_new_or_updated_rows_count: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    deleted_rows_count: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    error_message: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    def __attrs_post_init__(self) -> None:
        if self.raw_rows_count is not None and self.error_message is not None:
            raise ValueError(
                "Cannot have both an error message and a number of successful row count"
            )

    @property
    def suffix(self) -> str:
        if self.error_message and self.raw_rows_count:
            raise ValueError(
                "If we have a raw rows count, we shouldn't have an error message"
            )
        if self.error_message is not None:
            return f": {self.error_message}"
        if (
            self.net_new_or_updated_rows_count is not None
            and self.deleted_rows_count is not None
        ):
            return f": {self.raw_rows_count} raw rows, {self.net_new_or_updated_rows_count} new/updated, {self.deleted_rows_count} deleted"
        if self.raw_rows_count is not None:
            return f": {self.raw_rows_count} rows"

        return ""

    def format_for_print(self) -> str:
        return f"[{self.status.name}] {','.join([path.blob_name for path in self.paths])} {self.suffix}"

    @classmethod
    def from_skipped_file_error(
        cls, *, skipped_error: RawDataFilesSkippedError
    ) -> "SandboxConceptualFileImportResult":
        return SandboxConceptualFileImportResult(
            paths=skipped_error.file_paths,
            status=SandboxImportStatus.SKIPPED,
            error_message=skipped_error.skipped_message,
        )


@attr.define
class SandboxImportRun:

    status_to_imports: Dict[
        SandboxImportStatus, List[SandboxConceptualFileImportResult]
    ] = attr.ib(validator=attr_validators.is_dict)


def _validate_checksums(
    *,
    fs: GCSFileSystem,
    path: GcsfsFilePath,
    chunk_results: List[PreImportNormalizedCsvChunkResult],
) -> None:
    """Computes the combined checksum of |chunk_results| and compares it against the
    checksum |path|.
    """
    chunk_checksums_and_sizes = [
        (chunk_result.crc32c, chunk_result.get_chunk_boundary_size())
        for chunk_result in chunk_results
    ]
    chunk_combined_digest = digest_ordered_checksum_and_size_pairs(
        chunk_checksums_and_sizes
    )
    chunk_combined_checksum = base64.b64encode(chunk_combined_digest).decode("utf-8")

    full_file_checksum = fs.get_crc32c(path)

    if chunk_combined_checksum != full_file_checksum:
        raise ValueError(
            f"Checksum mismatch for {path.abs_path()}: {chunk_combined_checksum} != {full_file_checksum}"
        )


def _validate_headers(
    *,
    fs: GCSFileSystem,
    raw_file_config: DirectIngestRawFileConfig,
    bq_metadata: RawBigQueryFileMetadata,
    infer_schema_from_csv: bool,
) -> List[str]:
    """Executes header validation for all the gcs paths for |bq_metadata|."""
    for file in bq_metadata.gcs_files:
        header = DirectIngestRawFileHeaderReader(
            fs=fs,
            file_config=raw_file_config,
            infer_schema_from_csv=infer_schema_from_csv,
        ).read_and_validate_column_headers(file.path)

    return header


def _pre_import_norm_for_path(
    *,
    fs: GCSFileSystem,
    path: GcsfsFilePath,
    raw_file_config: DirectIngestRawFileConfig,
    pre_import_norm_type: PreImportNormalizationType,
    sandbox_dataset_prefix: str,
) -> List[PreImportNormalizedCsvChunkResult]:
    """Executes pre-import normalization for |path|, writing the resulting path to the
    temporary files bucket subdir named |sandbox_dataset_prefix|."""

    normalizer = DirectIngestRawFilePreImportNormalizer(
        fs,
        raw_file_config.state_code,
        temp_output_dir=gcsfs_direct_ingest_temporary_output_directory_path(
            subdir=sandbox_dataset_prefix
        ),
    )

    chunk_boundaries = GcsfsCsvChunkBoundaryFinder(
        fs=fs,
        line_terminator=raw_file_config.line_terminator,
        separator=raw_file_config.separator,
        encoding=raw_file_config.encoding,
        quoting_mode=raw_file_config.quoting_mode,
        chunk_size=DEFAULT_SANDBOX_CHUNK_SIZE,
    ).get_chunks_for_gcs_path(path)

    requires_pre_import_norm_file = RequiresPreImportNormalizationFile(
        path=path,
        pre_import_normalization_type=pre_import_norm_type,
        chunk_boundaries=chunk_boundaries,
    )

    chunk_results = [
        normalizer.normalize_chunk_for_import(file_chunk)
        for file_chunk in requires_pre_import_norm_file.to_file_chunks()
    ]

    _validate_checksums(fs=fs, path=path, chunk_results=chunk_results)

    return chunk_results


def _do_pre_import_normalization(
    *,
    fs: GCSFileSystem,
    raw_file_config: DirectIngestRawFileConfig,
    bq_metadata: RawBigQueryFileMetadata,
    load_config: RawFileBigQueryLoadConfig,
    pre_import_norm_type: PreImportNormalizationType,
    sandbox_dataset_prefix: str,
) -> ImportReadyFile:
    """Executes pre-import normalization on each gcs file in |bq_metadata|."""
    path_to_chunk_result = {
        file.path: _pre_import_norm_for_path(
            fs=fs,
            path=file.path,
            raw_file_config=raw_file_config,
            pre_import_norm_type=pre_import_norm_type,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )
        for file in bq_metadata.gcs_files
    }

    return ImportReadyFile.from_bq_metadata_load_config_and_normalized_chunk_result(
        bq_metadata=bq_metadata,
        bq_schema=load_config,
        input_path_to_normalized_chunk_results=path_to_chunk_result,
    )


def _import_bq_metadata_to_sandbox(
    *,
    fs: GCSFileSystem,
    bq_client: BigQueryClient,
    region_config: DirectIngestRegionRawFileConfig,
    bq_metadata: RawBigQueryFileMetadata,
    sandbox_dataset_prefix: str,
    infer_schema_from_csv: bool,
    skip_blocking_validations: bool,
    skip_raw_data_migrations: bool,
    persist_intermediary_tables: bool,
    skip_raw_data_pruning: bool,
) -> RawFileImport:
    """Imports a single |bq_metadata| into a sandbox raw data table."""

    logging.info(
        "Starting [%s] sandbox import for [%s] w/ update_datetime [%s]",
        sandbox_dataset_prefix,
        bq_metadata.file_tag,
        bq_metadata.update_datetime.isoformat(),
    )

    raw_file_config = region_config.raw_file_configs[bq_metadata.file_tag]

    # first, validate headers
    headers = _validate_headers(
        fs=fs,
        raw_file_config=raw_file_config,
        bq_metadata=bq_metadata,
        infer_schema_from_csv=infer_schema_from_csv,
    )
    load_config = RawFileBigQueryLoadConfig.from_headers_and_raw_file_config(
        file_headers=headers,
        raw_file_config=raw_file_config,
    )

    # then, prep file for import
    import_ready_file: ImportReadyFile
    if (
        pre_import_norm_type := PreImportNormalizationType.required_pre_import_normalization_type(
            raw_file_config
        )
    ) is not None:
        import_ready_file = _do_pre_import_normalization(
            fs=fs,
            raw_file_config=raw_file_config,
            bq_metadata=bq_metadata,
            load_config=load_config,
            pre_import_norm_type=pre_import_norm_type,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )
    else:
        import_ready_file = ImportReadyFile.from_bq_metadata_and_load_config(
            bq_metadata=bq_metadata, bq_schema=load_config
        )

    # last, import file!
    loader = DirectIngestRawFileLoadManager(
        raw_data_instance=DirectIngestInstance.PRIMARY,
        region_raw_file_config=region_config,
        fs=fs,
        big_query_client=bq_client,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    append_ready = loader.load_and_prep_paths(
        import_ready_file,
        temp_table_prefix=sandbox_dataset_prefix,
        skip_blocking_validations=skip_blocking_validations,
        skip_raw_data_migrations=skip_raw_data_migrations,
        persist_intermediary_tables=persist_intermediary_tables,
    )
    append_summary = loader.append_to_raw_data_table(
        append_ready,
        # persist intermediary tables (the __transformed table) if persist_intermediary_tables
        # is true or if infer_schema_from_csv is true and we want a table that directly
        # reflects the raw file irrespective of what the config says
        persist_intermediary_tables=persist_intermediary_tables
        or infer_schema_from_csv,
        skip_raw_data_pruning=skip_raw_data_pruning,
    )

    return RawFileImport.from_load_results(append_ready, append_summary)


def _build_gcs_metadata(
    state_code: StateCode, files_to_import: List[GcsfsFilePath]
) -> List[RawGCSFileMetadata]:
    """Builds a RawGCSFileMetadata for each path in |files_to_import|."""
    return [
        RawGCSFileMetadata(
            gcs_file_id=_id_for_file(state_code, path.blob_name),
            file_id=None,
            path=path,
        )
        for path in files_to_import
    ]


def _build_bq_metadata(
    gcs_metadata: List[RawGCSFileMetadata],
    region_raw_file_config: DirectIngestRegionRawFileConfig,
    allow_incomplete_chunked_files: bool,
) -> Tuple[List[RawBigQueryFileMetadata], List[SandboxConceptualFileImportResult]]:
    """Builds "conceptual" RawBigQueryFileMetadata files from the literal gcs files
    represented in RawGCSFileMetadata objects.
    """

    delegate = RawDataImportDelegateFactory.build(
        region_code=region_raw_file_config.region_code
    )
    conceptual_files: List[RawBigQueryFileMetadata] = []
    skipped_files: List[SandboxConceptualFileImportResult] = []
    chunked_files: Dict[
        str, Dict[datetime.date, List[RawGCSFileMetadata]]
    ] = defaultdict(lambda: defaultdict(list))

    for metadata in gcs_metadata:

        if region_raw_file_config.raw_file_configs[
            metadata.parts.file_tag
        ].is_chunked_file:
            chunked_files[metadata.parts.file_tag][
                metadata.parts.utc_upload_datetime.date()
            ].append(metadata)
            continue

        metadata.file_id = metadata.gcs_file_id
        conceptual_files.append(
            RawBigQueryFileMetadata(
                file_id=metadata.gcs_file_id,
                gcs_files=[metadata],
                file_tag=metadata.parts.file_tag,
                update_datetime=metadata.parts.utc_upload_datetime,
            )
        )

    if chunked_files:

        for file_tag, upload_date_to_gcs_files in chunked_files.items():

            for upload_date in sorted(upload_date_to_gcs_files):

                gcs_files = upload_date_to_gcs_files[upload_date]

                if allow_incomplete_chunked_files:
                    logging.info(
                        "Found %s paths for %s on %s -- grouping %s",
                        len(gcs_files),
                        file_tag,
                        upload_date.isoformat(),
                        [f.path.file_name for f in gcs_files],
                    )
                    conceptual_files.append(
                        RawBigQueryFileMetadata(
                            gcs_files=gcs_files,
                            file_tag=file_tag,
                            update_datetime=max(
                                gcs_files,
                                key=lambda x: x.parts.utc_upload_datetime,
                            ).parts.utc_upload_datetime,
                        )
                    )

                else:
                    (
                        conceptual_files_for_file_tag,
                        skipped_errors_for_file_tag,
                    ) = delegate.coalesce_chunked_files(
                        file_tag=file_tag, gcs_files=gcs_files
                    )

                    conceptual_files.extend(conceptual_files_for_file_tag)
                    if skipped_errors_for_file_tag:
                        skipped_files.extend(
                            [
                                SandboxConceptualFileImportResult.from_skipped_file_error(
                                    skipped_error=skipped_error_for_file_tag
                                )
                                for skipped_error_for_file_tag in skipped_errors_for_file_tag
                            ]
                        )

    return conceptual_files, skipped_files


def import_raw_files_to_sandbox(
    *,
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    files_to_import: List[GcsfsFilePath],
    big_query_client: BigQueryClient,
    fs: DirectIngestGCSFileSystem,
    region_config: DirectIngestRegionRawFileConfig,
    infer_schema_from_csv: bool,
    skip_blocking_validations: bool,
    skip_raw_data_migrations: bool,
    persist_intermediary_tables: bool,
    allow_incomplete_chunked_files: bool,
    skip_raw_data_pruning: bool,
) -> SandboxImportRun:
    """Executes a sandbox import for |files_to_import|."""
    status_to_imports = defaultdict(list)

    gcs_metadata = _build_gcs_metadata(state_code, files_to_import)
    bq_metadata, skipped_files = _build_bq_metadata(
        gcs_metadata,
        region_config,
        allow_incomplete_chunked_files=allow_incomplete_chunked_files,
    )

    if skipped_files:
        status_to_imports[SandboxImportStatus.SKIPPED] = skipped_files

    for metadata in bq_metadata:
        try:
            result = _import_bq_metadata_to_sandbox(
                fs=fs,
                bq_client=big_query_client,
                region_config=region_config,
                bq_metadata=metadata,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
                infer_schema_from_csv=infer_schema_from_csv,
                skip_blocking_validations=skip_blocking_validations,
                skip_raw_data_migrations=skip_raw_data_migrations,
                persist_intermediary_tables=persist_intermediary_tables,
                skip_raw_data_pruning=skip_raw_data_pruning,
            )
            status_to_imports[SandboxImportStatus.SUCCEEDED].append(
                SandboxConceptualFileImportResult(
                    paths=[gcs_file.path for gcs_file in metadata.gcs_files],
                    status=SandboxImportStatus.SUCCEEDED,
                    error_message=None,
                    raw_rows_count=result.raw_rows,
                    net_new_or_updated_rows_count=result.net_new_or_updated_rows,
                    deleted_rows_count=result.deleted_rows,
                )
            )

        except Exception as e:
            status_to_imports[SandboxImportStatus.FAILED].append(
                SandboxConceptualFileImportResult(
                    paths=[gcs_file.path for gcs_file in metadata.gcs_files],
                    status=SandboxImportStatus.FAILED,
                    error_message=f"{str(e)}: {traceback.format_exc()}",
                )
            )

    return SandboxImportRun(status_to_imports=status_to_imports)
