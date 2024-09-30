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
"""Tasks for filtering down results."""
from itertools import groupby
from typing import Dict, List, Tuple

from airflow.decorators import task

from recidiviz.airflow.dags.raw_data.metadata import CHUNKING_ERRORS, CHUNKING_RESULTS
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawFileProcessingError,
    RequiresPreImportNormalizationFile,
)
from recidiviz.utils.airflow_types import MappedBatchedTaskOutput
from recidiviz.utils.types import assert_type


@task
def filter_chunking_results_by_errors(
    mapped_file_chunking_result: List[str],
) -> Dict[str, List[str]]:
    """Filters out successful results from the chunking step who have an error from the
    chunking step with the same file_tag an update_datetime before it, adding these
    skipped errors to the chunking errors from |mapped_file_chunking_result|.
    """
    chunking_results = MappedBatchedTaskOutput.deserialize(
        mapped_file_chunking_result,
        result_cls=RequiresPreImportNormalizationFile,
        error_cls=RawFileProcessingError,
    )

    filtered_chunks, skipped_errors = filter_chunking_results_by_processing_errors(
        chunking_results.flatten_results(), chunking_results.flatten_errors()
    )

    all_errors: List[RawFileProcessingError] = [
        *skipped_errors,
        *chunking_results.flatten_errors(),
    ]

    return {
        CHUNKING_RESULTS: [chunk.serialize() for chunk in filtered_chunks],
        CHUNKING_ERRORS: [error.serialize() for error in all_errors],
    }


def filter_chunking_results_by_processing_errors(
    results: List[RequiresPreImportNormalizationFile],
    errors: List[RawFileProcessingError],
) -> Tuple[List[RequiresPreImportNormalizationFile], List[RawFileProcessingError]]:
    """Filters out successful results from the chunking step who have an error from the
    chunking step with the same file_tag an update_datetime before it.
    """

    filtered_results: List[RequiresPreImportNormalizationFile] = []
    skipped_errors: List[RawFileProcessingError] = []

    blocking_errors_by_file_tag = {
        file_tag: min(group, key=lambda x: x.parts.utc_upload_datetime)
        for file_tag, group in groupby(
            sorted(errors, key=lambda x: x.parts.file_tag),
            lambda x: x.parts.file_tag,
        )
    }

    for result in results:
        if (
            result.parts.file_tag in blocking_errors_by_file_tag
            and blocking_errors_by_file_tag[
                result.parts.file_tag
            ].parts.utc_upload_datetime
            < result.parts.utc_upload_datetime
        ):
            blocking_error = blocking_errors_by_file_tag[result.parts.file_tag]
            skipped_errors.append(
                RawFileProcessingError(
                    original_file_path=result.path,
                    temporary_file_paths=None,
                    error_msg=f"Blocked Import: failed due to import-blocking failure from {blocking_error.original_file_path} \n\n: {blocking_error.error_msg}",
                )
            )
            continue
        filtered_results.append(result)

    return filtered_results, skipped_errors


def filter_header_results_by_processing_errors(
    bq_metadata: List[RawBigQueryFileMetadata],
    file_ids_to_headers: Dict[int, List[str]],
    errors: List[RawFileProcessingError],
) -> Tuple[Dict[int, List[str]], List[RawFileProcessingError]]:
    """Filters out successful results from the header validation step who have an error
    from the header validation step with the same file_tag an update_datetime before it.
    """

    filtered_file_ids_to_headers: Dict[int, List[str]] = {}
    skipped_errors: List[RawFileProcessingError] = []

    blocking_errors_by_file_tag = {
        file_tag: min(group, key=lambda x: x.parts.utc_upload_datetime)
        for file_tag, group in groupby(
            sorted(errors, key=lambda x: x.parts.file_tag),
            lambda x: x.parts.file_tag,
        )
    }

    for metadata in bq_metadata:
        if (
            metadata.file_tag in blocking_errors_by_file_tag
            and blocking_errors_by_file_tag[metadata.file_tag].parts.utc_upload_datetime
            < metadata.update_datetime
        ):
            blocking_error = blocking_errors_by_file_tag[metadata.file_tag]
            for gcs_file in metadata.gcs_files:
                skipped_errors.append(
                    RawFileProcessingError(
                        original_file_path=gcs_file.path,
                        temporary_file_paths=None,
                        error_msg=f"Blocked Import: failed due to import-blocking failure from {blocking_error.original_file_path} \n\n: {blocking_error.error_msg}",
                    )
                )
            continue
        file_id: int = assert_type(metadata.file_id, int)
        filtered_file_ids_to_headers[file_id] = file_ids_to_headers[file_id]

    return filtered_file_ids_to_headers, skipped_errors
