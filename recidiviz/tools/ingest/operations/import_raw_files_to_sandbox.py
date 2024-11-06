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
"""Script for testing that a set of raw files can be properly imported to BQ.

Given a set of state raw data files that have already been uploaded to a testing bucket,
imports those files to a sandbox BigQuery dataset using the specification in the raw
data config files.

This should be used to verify that a set of raw files properly parse. The resulting data
in the sandbox dataset can be compared against existing raw data tables more easily
(e.g. by comparing columns in the INFORMATION_SCHEMA.COLUMNS) tables.

Usage:

python -m recidiviz.tools.ingest.operations.import_raw_files_to_sandbox \
    --state-code US_PA --sandbox-dataset-prefix my_prefix \
    --source-bucket recidiviz-staging-my-test-bucket \
    [--file-tag-filter-regex (tagA|otherTagB)] \
    [--allow-incomplete-configs False]
"""

import argparse
import datetime
import logging
import os
import re
from enum import Enum
from typing import List, Optional, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.gating import is_raw_data_import_dag_enabled
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import import (
    SandboxConceptualFileImportResult,
    SandboxImportRun,
    SandboxImportStatus,
    import_raw_files_to_sandbox,
    legacy_import_raw_files_to_bq_sandbox,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import FILE_ID_COL_NAME
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.source_tables.collect_all_source_table_configs import (
    ONE_DAY_MS,
    SourceTableLabel,
    raw_data_temp_load_dataset,
)
from recidiviz.source_tables.source_table_config import (
    RawDataSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


# TODO(#28239) remove enum once raw data import dag is rolled out
class SandboxImportInfraType(Enum):
    LEGACY = "legacy"
    NEW = "new"


def get_unprocessed_raw_files_in_bucket(
    fs: DirectIngestGCSFileSystem,
    bucket_path: GcsfsBucketPath,
    region_raw_file_config: DirectIngestRegionRawFileConfig,
    file_tag_filters: Optional[List[str]],
) -> Tuple[List[GcsfsFilePath], List[SandboxConceptualFileImportResult]]:
    """Returns a list of paths to unprocessed raw files in the provided bucket that have
    registered file tags for a given region.
    """
    unprocessed_paths = fs.get_unprocessed_raw_file_paths(bucket_path)
    unprocessed_raw_files = []
    skipped_files = []
    for path in unprocessed_paths:
        parts = filename_parts_from_path(path)
        if parts.file_tag in region_raw_file_config.raw_file_tags:
            if file_tag_filters is not None and parts.file_tag not in file_tag_filters:
                skipped_files.append(
                    SandboxConceptualFileImportResult(
                        paths=[path],
                        status=SandboxImportStatus.SKIPPED,
                        error_message="Excluded by file_tag_filters",
                    )
                )
            else:
                unprocessed_raw_files.append(path)
        else:
            skipped_files.append(
                SandboxConceptualFileImportResult(
                    paths=[path],
                    status=SandboxImportStatus.SKIPPED,
                    error_message="Unrecognized file tag",
                )
            )

    return unprocessed_raw_files, skipped_files


def source_table_collection_for_paths(
    *,
    state_code: StateCode,
    files_to_import: List[GcsfsFilePath],
    create_tables_ahead_of_time: bool,
    sandbox_dataset_prefix: str,
    region_config: DirectIngestRegionRawFileConfig,
    infra_type: SandboxImportInfraType,
) -> List[SourceTableCollection]:
    """Builds a SourceTableCollection object that contains sandbox raw data tables we
    can create ahead of time.
    """
    labels: list[SourceTableLabel] = [
        RawDataSourceTableLabel(
            state_code=state_code, ingest_instance=DirectIngestInstance.PRIMARY
        ),
        StateSpecificSourceTableLabel(state_code=state_code),
    ]

    sandbox_raw_data_collection = SourceTableCollection(
        dataset_id=raw_tables_dataset_for_region(
            state_code=state_code,
            instance=DirectIngestInstance.PRIMARY,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        ),
        # update config is regenerable if you want to be able to update the schema in
        # the same sandbox prefix between runs
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[
            RawDataSourceTableLabel(
                state_code=state_code, ingest_instance=DirectIngestInstance.PRIMARY
            ),
            StateSpecificSourceTableLabel(state_code=state_code),
        ],
        description=f"Sandbox raw data tables from {StateCode.get_state(state_code)} with dataset prefix: {sandbox_dataset_prefix}",
    )

    if create_tables_ahead_of_time:
        file_tags = {
            filename_parts_from_path(file).file_tag for file in files_to_import
        }

        for file_tag in file_tags:
            sandbox_raw_data_collection.add_source_table(
                file_tag,
                description=f"Sandbox raw data file for {file_tag}",
                schema_fields=RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                    raw_file_config=region_config.raw_file_configs[file_tag],
                ),
                clustering_fields=[FILE_ID_COL_NAME],
            )

    table_collections = [sandbox_raw_data_collection]

    if infra_type == SandboxImportInfraType.NEW:
        table_collections.append(
            SourceTableCollection(
                dataset_id=raw_data_temp_load_dataset(
                    state_code,
                    DirectIngestInstance.PRIMARY,
                    sandbox_dataset_prefix=sandbox_dataset_prefix,
                ),
                labels=labels,
                default_table_expiration_ms=ONE_DAY_MS,
                description=(
                    "Contains intermediate results of a sandbox raw data import process"
                    "that will be queried during the sandbox import process."
                ),
            )
        )

    return table_collections


def do_sandbox_raw_file_import(
    *,
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    source_bucket: GcsfsBucketPath,
    file_tag_filter_regex: Optional[str],
    infra_type: Optional[SandboxImportInfraType],
    infer_schema_from_csv: bool,
    skip_blocking_validations: bool,
    skip_raw_data_migrations: bool,
    persist_intermediary_tables: bool,
    allow_incomplete_chunked_files: bool,
) -> None:
    """Imports a set of raw data files in the given source bucket into a sandbox
    dataset.
    """

    if infra_type is None:
        infra_type = (
            SandboxImportInfraType.NEW
            if is_raw_data_import_dag_enabled(state_code, DirectIngestInstance.PRIMARY)
            else SandboxImportInfraType.LEGACY
        )

    region_raw_file_config = DirectIngestRegionRawFileConfig(
        region_code=state_code.value.lower()
    )

    file_tag_filters = None
    if file_tag_filter_regex:
        file_tag_filters = []
        for raw_file_tag in region_raw_file_config.raw_file_tags:
            if re.search(file_tag_filter_regex, raw_file_tag):
                file_tag_filters.append(raw_file_tag)

    fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
    bq_client = BigQueryClientImpl()

    files_to_import, skipped_files = get_unprocessed_raw_files_in_bucket(
        fs,
        source_bucket,
        region_raw_file_config,
        file_tag_filters,
    )

    logging.info("******************** Sandbox Plan ***********************")
    logging.info(
        "[%s] Unprocessed files skipped: \n\t-%s",
        len(skipped_files),
        "\n\t-".join(
            [skipped_file.format_for_print() for skipped_file in skipped_files]
        ),
    )
    logging.info(
        "[%s] Unprocessed files to import: \n\t-%s",
        len(files_to_import),
        "\n\t-".join([path.blob_name for path in files_to_import]),
    )

    prompt_for_confirmation(
        f"Proceed with sandbox import of [{len(files_to_import)}] files?"
    )

    # In the LEGACY infra, infer_schema_from_csv means we cannot build the raw data
    # table ahead of time as we do not know before reading the actual file what the
    # schema will be. If infer_schema_from_csv is True, BQ will create the table
    # automatically during the load job using the provided schema.
    #
    # In the NEW infra, we always create a temporary table that matches the schema
    # of the raw data file and then map that schema onto the current raw data schema,
    # so you will be always be able to see both the table with the schema that matches
    # the file exactly, as well as that schema mapped to the current raw config schema.
    create_tables_ahead_of_time = (
        infra_type == SandboxImportInfraType.NEW or infer_schema_from_csv
    )

    source_table_collections_for_sandbox = source_table_collection_for_paths(
        state_code=state_code,
        files_to_import=files_to_import,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        create_tables_ahead_of_time=create_tables_ahead_of_time,
        region_config=region_raw_file_config,
        infra_type=infra_type,
    )

    update_manager = SourceTableUpdateManager()
    update_manager.update_async(
        source_table_collections=source_table_collections_for_sandbox,
        log_file=os.path.join(
            os.path.dirname(__file__),
            f"logs/{sandbox_dataset_prefix}_import_raw_files_to_sandbox_{datetime.datetime.now().isoformat()}.log",
        ),
        log_output=True,
    )

    sandbox_import_result: SandboxImportRun
    match infra_type:
        case SandboxImportInfraType.LEGACY:
            sandbox_import_result = legacy_import_raw_files_to_bq_sandbox(
                state_code=state_code,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
                files_to_import=files_to_import,
                infer_schema_from_csv=infer_schema_from_csv,
                big_query_client=BigQueryClientImpl(),
                fs=fs,
            )
        case SandboxImportInfraType.NEW:
            sandbox_import_result = import_raw_files_to_sandbox(
                state_code=state_code,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
                files_to_import=files_to_import,
                big_query_client=bq_client,
                fs=fs,
                region_config=region_raw_file_config,
                infer_schema_from_csv=infer_schema_from_csv,
                skip_blocking_validations=skip_blocking_validations,
                skip_raw_data_migrations=skip_raw_data_migrations,
                persist_intermediary_tables=persist_intermediary_tables,
                allow_incomplete_chunked_files=allow_incomplete_chunked_files,
            )

    logging.info("************************** RESULTS **************************")
    for status in list(SandboxImportStatus):
        for file_result in sandbox_import_result.status_to_imports.get(status, []):
            logging.info("\t-%s", file_result.format_for_print())
    logging.info("*************************************************************")


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state-code",
        help="State that these raw files are for, in the form US_XX.",
        type=str,
        choices=[state.value for state in StateCode],
        required=True,
    )

    parser.add_argument(
        "--sandbox-dataset-prefix",
        help="A prefix to append to all names of the raw data dataset this data will be"
        "loaded into.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--source-bucket",
        type=str,
        required=True,
        help="A sandbox GCS bucket where raw files live. Files in this bucket must "
        "already have normalized file names.",
    )

    parser.add_argument(
        "--file-tag-filter-regex",
        default=None,
        help="Regex file tag filter - when set, will only import files whose tags "
        "contain a match to this regex.",
    )

    # TODO(#28239) remove flag once raw data import dag is rolled out
    parser.add_argument(
        "--infra-type",
        help=(
            "The infra that will be used to import the raw data files. If none is "
            "specified, defaults to using whatever infra is enabled in PRIMARY for the "
            "provided state_code."
        ),
        type=str,
        choices=[type_.value for type_ in SandboxImportInfraType],
        required=False,
    )

    # TODO(#28239) clean up doc when raw data infra is rolled out
    parser.add_argument(
        "--infer-schema-from-csv",
        help=(
            "For files with a header row, will infers the raw file's schema from the "
            "header row of the csv instead of using the raw file config to determine the "
            "schema. If running on the new infra, we will additionally persist the "
            "__transformed table whose schema will match the raw data file and has a "
            "default TTL of 1 day."
        ),
        action="store_true",
    )

    parser.add_argument(
        "--skip-blocking-validations",
        help=(
            "Skips import blocking validations during the big query load step. These "
            "validations validate things like columns types, datetime parsers and "
            "historical file counts."
        ),
        action="store_true",
    )

    # TODO(#28239) clean up doc when raw data infra is rolled out
    parser.add_argument(
        "--skip-raw-data-migrations",
        help=(
            "Skips raw data migrations during the big query load step. Only applies to"
            "the new raw data infra."
        ),
        action="store_true",
    )

    parser.add_argument(
        "--persist-intermediary-tables",
        help=(
            "Ensures that intermediary tables, namely the temporary raw file table that "
            "directly reflects the raw file and the transformed table with raw data "
            "migrations applied, are persisted after a successful sandbox import. Both "
            "of these tables have an default TTL of 1 day."
        ),
        action="store_true",
    )

    parser.add_argument(
        "--allow-incomplete-chunked-files",
        help=(
            "Allows incomplete chunked files, meaning that we don't enforce that the "
            "expected_number_of_chunks specified for in chunked file's config matches"
            "the number of chunks for each day when grouping files marked is_chunked_file "
            "in the raw file config."
        ),
        action="store_true",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args = parse_arguments()
    with local_project_id_override(GCP_PROJECT_STAGING):
        do_sandbox_raw_file_import(
            state_code=StateCode(known_args.state_code),
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            source_bucket=GcsfsBucketPath(known_args.source_bucket),
            file_tag_filter_regex=known_args.file_tag_filter_regex,
            infra_type=SandboxImportInfraType(known_args.infra_type),
            infer_schema_from_csv=known_args.infer_schema_from_csv,
            skip_blocking_validations=known_args.skip_blocking_validations,
            skip_raw_data_migrations=known_args.skip_raw_data_migrations,
            persist_intermediary_tables=known_args.persist_intermediary_tables,
            allow_incomplete_chunked_files=known_args.allow_incomplete_chunked_files,
        )
