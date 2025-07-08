#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
"""Tool to refresh a state's raw config column known values from existing raw data in BigQuery.

This reads the raw files in BigQuery for the given state if the existing config for that file
has at least one column with a known_values entry (can be empty to indicate
this column is to be treated as an enum column, i.e. `known_values: []`),
then populates the yaml config with any values for those enum columns, while maintaining
existing documentation.

After running the script, be sure to either fill in or delete any blank descriptions.

Example Usage:
    python -m recidiviz.tools.ingest.development.fetch_column_values_for_state --state-code US_ND \
        --project-id recidiviz-staging \
        --issue-id 1234 \
        [--file-tag-filters RAW_TABLE_1 RAW_TABLE_2] \
        [--sandbox_dataset_prefix SANDBOX_DATASET_PREFIX]
"""
import argparse
import logging
import sys
from typing import List, Optional

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants import states
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.development.raw_data_config_writer import (
    RawDataConfigWriter,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


def _get_known_values(
    column: RawTableColumnInfo,
    file_tag: str,
    state_code: StateCode,
    project_id: str,
    bq_client: BigQueryClientImpl,
    default_description: str,
    sandbox_dataset_prefix: Optional[str],
) -> RawTableColumnInfo:
    """Creates a list of possible enum values for a column in a given table"""
    # Only fetch known values for enum columns
    if not column.is_enum:
        return column

    raw_data_dataset = raw_tables_dataset_for_region(
        state_code=state_code,
        # Raw data is only directly uploaded by states to PRIMARY.
        instance=DirectIngestInstance.PRIMARY,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    query_string = f"""
SELECT
 DISTINCT {column.name} as values
FROM
 `{project_id}.{raw_data_dataset}.{file_tag}`
WHERE {column.name} IS NOT NULL
ORDER BY
  {column.name}
"""
    query_job = bq_client.run_query_async(query_str=query_string, use_query_cache=True)
    distinct_values = [row["values"] for row in query_job]
    existing_values = [enum.value for enum in column.known_values_nonnull]

    new_known_values_list = column.known_values_nonnull.copy()
    for value in distinct_values:
        if value not in existing_values:
            new_known_values_list.append(
                ColumnEnumValueInfo(value=value, description=default_description)
            )

    new_known_values_list.sort(
        key=lambda e: (
            e.value.isdigit(),
            int(e.value) if e.value.isdigit() else e.value.lower(),
        )
    )
    return RawTableColumnInfo(
        name=column.name,
        state_code=state_code,
        file_tag=file_tag,
        description=column.description,
        field_type=column.field_type,
        is_pii=False,
        known_values=new_known_values_list,
    )


def _update_enum_known_values(
    original_config: DirectIngestRawFileConfig,
    project_id: str,
    bq_client: BigQueryClientImpl,
    default_description: str,
    sandbox_dataset_prefix: Optional[str],
) -> DirectIngestRawFileConfig:
    new_columns = [
        _get_known_values(
            column,
            original_config.file_tag,
            original_config.state_code,
            project_id,
            bq_client,
            default_description,
            sandbox_dataset_prefix,
        )
        for column in original_config.current_columns
    ]

    return DirectIngestRawFileConfig(
        state_code=original_config.state_code,
        file_tag=original_config.file_tag,
        file_path=original_config.file_path,
        file_description=original_config.file_description,
        data_classification=original_config.data_classification,
        primary_key_cols=original_config.primary_key_cols,
        columns=new_columns,
        encoding=original_config.encoding,
        separator=original_config.separator,
        custom_line_terminator=original_config.custom_line_terminator,
        ignore_quotes=original_config.ignore_quotes,
        supplemental_order_by_clause=original_config.supplemental_order_by_clause,
        export_lookback_window=original_config.export_lookback_window,
        no_valid_primary_keys=original_config.no_valid_primary_keys,
        infer_columns_from_config=original_config.infer_columns_from_config,
        table_relationships=original_config.table_relationships,
        update_cadence=original_config.update_cadence,
        is_code_file=original_config.is_code_file,
        is_chunked_file=original_config.is_chunked_file,
    )


def main(
    state_code: str,
    project_id: str,
    file_tags: List[str],
    issue_id: int,
    sandbox_dataset_prefix: Optional[str],
) -> None:
    """Update columns in raw data configs with known values fetched from BigQuery."""
    region_config = get_region_raw_file_config(state_code)

    default_config = region_config.default_config()

    bq_client = BigQueryClientImpl()

    raw_file_configs: List[DirectIngestRawFileConfig] = list(
        region_config.raw_file_configs.values()
    )

    if file_tags:
        raw_file_configs = [
            config for config in raw_file_configs if config.file_tag in file_tags
        ]

    # Only refresh if a config has enum columns
    for original_raw_file_config in [
        config for config in raw_file_configs if config.has_enums
    ]:
        updated_raw_file_config = _update_enum_known_values(
            original_config=original_raw_file_config,
            project_id=project_id,
            bq_client=bq_client,
            default_description="TO" + "DO(#" + f"{issue_id}) Document this value.",
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )
        raw_data_config_writer = RawDataConfigWriter()
        raw_data_config_writer.output_to_file(
            raw_file_config=updated_raw_file_config,
            output_path=original_raw_file_config.file_path,
            default_encoding=default_config.default_encoding,
            default_separator=default_config.default_separator,
            default_ignore_quotes=default_config.default_ignore_quotes,
            default_export_lookback_window=default_config.default_export_lookback_window,
            default_no_valid_primary_keys=default_config.default_no_valid_primary_keys,
            default_custom_line_terminator=default_config.default_custom_line_terminator,
            default_update_cadence=default_config.default_update_cadence,
            default_infer_columns_from_config=default_config.default_infer_columns_from_config,
            default_import_blocking_validation_exemptions=default_config.default_import_blocking_validation_exemptions,
        )


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the named arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state-code",
        dest="state_code",
        help="State to which this config belongs in the form US_XX.",
        type=str,
        choices=[state.value for state in states.StateCode],
        required=True,
    )

    parser.add_argument(
        "--project-id",
        dest="project_id",
        help="Which project to read raw data from.",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--issue-id",
        dest="issue_id",
        help="The ID for the issue tracking the documentation of the newly added fields.",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--file-tag-filters",
        dest="file_tags",
        default=[],
        nargs="+",
        help="List of file tags to fetch columns for. If not set, will update for all tables in the raw data configs.",
        required=False,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        help="Prefix of sandbox dataset to search for raw data tables. If not set, "
        "will search in us_xx_raw_data for a given state US_XX. If set, will search in "
        "my_prefix_us_xx_raw_data.",
        type=str,
        default=None,
        required=False,
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)

    with metadata.local_project_id_override(args.project_id):
        main(
            args.state_code,
            args.project_id,
            args.file_tags,
            args.issue_id,
            args.sandbox_dataset_prefix,
        )
