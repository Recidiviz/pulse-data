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
"""Python logic for managing and handling raw file metadata"""
from types import ModuleType
from typing import Dict, List, Optional

from airflow.decorators import task

from recidiviz.airflow.dags.raw_data.metadata import (
    IMPORT_READY_FILES,
    REQUIRES_NORMALIZATION_FILES,
    REQUIRES_NORMALIZATION_FILES_BQ_METADATA,
)
from recidiviz.airflow.dags.raw_data.utils import get_direct_ingest_region_raw_config
from recidiviz.ingest.direct.types.raw_data_import_types import (
    ImportReadyFile,
    PreImportNormalizationType,
    RawBigQueryFileMetadataSummary,
    RequiresNormalizationFile,
)


@task
def split_by_pre_import_normalization_type(
    region_code: str,
    serialized_bq_metadata: List[str],
    region_module_override: Optional[ModuleType] = None,
) -> Dict[str, List[str]]:
    """Determines the pre-import normalization needs of each element of
    |serialized_bq_metadata|, returning a dictionary with three keys:
        - import_ready_files: list of serialized ImportReadyFile that will skip the
            pre-import normalization steps
        - requires_normalization_files: list of RequiresNormalizationFile to be
            processed in parallel by pre-import normalization step
        - requires_normalization_files_big_query_metadata: list of serialized
            RawBigQueryFileMetadataSummary that will be combined downstream with the
            resultant ImportReadyNormalizedFile objects to to build ImportReadyFile
            objects
    """

    bq_metadata = [
        RawBigQueryFileMetadataSummary.deserialize(serialized_metadata)
        for serialized_metadata in serialized_bq_metadata
    ]

    region_config = get_direct_ingest_region_raw_config(
        region_code=region_code, region_module_override=region_module_override
    )

    import_ready_files: List[ImportReadyFile] = []
    pre_import_normalization_required_big_query_metadata: List[
        RawBigQueryFileMetadataSummary
    ] = []
    pre_import_normalization_required_files: List[RequiresNormalizationFile] = []

    for metadata in bq_metadata:
        pre_import_normalization_type = (
            PreImportNormalizationType.required_pre_import_normalization_type(
                region_config.raw_file_configs[metadata.file_tag]
            )
        )

        if pre_import_normalization_type:
            pre_import_normalization_required_big_query_metadata.append(metadata)
            for gcs_file in metadata.gcs_files:
                pre_import_normalization_required_files.append(
                    # TODO(#30170) switch to using pre-import normalization
                    RequiresNormalizationFile(
                        path=gcs_file.path.abs_path(),
                        normalization_type=pre_import_normalization_type,
                    )
                )
        else:
            import_ready_files.append(ImportReadyFile.from_bq_metadata(metadata))

    return {
        IMPORT_READY_FILES: [file.serialize() for file in import_ready_files],
        REQUIRES_NORMALIZATION_FILES_BQ_METADATA: [
            metadata.serialize()
            for metadata in pre_import_normalization_required_big_query_metadata
        ],
        REQUIRES_NORMALIZATION_FILES: [
            file.serialize() for file in pre_import_normalization_required_files
        ],
    }
