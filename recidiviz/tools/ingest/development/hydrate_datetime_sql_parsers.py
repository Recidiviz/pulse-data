#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
"""Tool to add a state's raw config column datetime_sql_parsers given existing raw data in BigQuery.

This reads the raw files in BigQuery for the given state if the existing config for that file
has at least one column with field_type: datetime and no specified datetime parsers.
It populates the YAML file for that column with a value for the datetime_sql_parser
that matches every non-null row in the file, or leaves it blank if no value could be derived.

Example Usage:
    python -m recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers --state-code US_ND \
        --project-id recidiviz-staging \
        [--file-tag-filters RAW_TABLE_1 RAW_TABLE_2] \
        [--sandbox_dataset_prefix SANDBOX_DATASET_PREFIX] \
        [--parser="PARSER"]
"""
import argparse
import logging
import re
import sys
from typing import List, Optional

import attr
from more_itertools import one

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DATETIME_SQL_REGEX,
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
from recidiviz.utils.string import StrictStringFormatter

DEFAULT_PARSERS_TO_TRY = [
    # m/d/y format parsers
    "SAFE.PARSE_DATETIME('%m/%d/%y', {col_name})",
    "SAFE.PARSE_DATETIME('%m/%d/%Y', {col_name})",
    "SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', {col_name})",
    # https://docs.python.org/3/library/time.html#time.strftime
    # When used with the strptime() function, the %p directive only affects the output
    # hour field if the %I directive is used to parse the hour.
    "SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S%p', {col_name})",
    # y-m-d format parsers
    "SAFE.PARSE_DATETIME('%Y-%m-%d', {col_name})",
    "SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', {col_name})",
    "SAFE.PARSE_DATETIME('%Y-%m-%d %H', {col_name})",
    "SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', {col_name})",
    "SAFE.PARSE_DATETIME('%Y-%m-%d %I:%M:%S%p', {col_name})",
]


def _update_parsers_in_column(
    project_id: str,
    file_tag: str,
    column: RawTableColumnInfo,
    raw_data_dataset: str,
    bq_client: BigQueryClientImpl,
    parsers_to_try: List[str],
) -> RawTableColumnInfo:
    """
    Tries to update the given column based on the given set of datetime parsers
    to try, by adding a datetime parser to the column if (1) it is a datetime,
    (2) it did not already have a parser, and (3) that parser works for all
    non-null values in that column

    Returns the new column object, updated with a parser if one worked.
    """
    # Assemble one query to check all parsers against this column
    formatter = StrictStringFormatter()
    column_clauses = [
        f"COUNT({column.name}) AS nonnull_values",
        f"MAX(IF({column.name} IS NOT NULL, {column.name}, NULL)) AS example_nonnull_value",
    ]
    for i, datetime_sql_parser in enumerate(parsers_to_try):
        parse_date_clause = formatter.format(datetime_sql_parser, col_name=column.name)

        column_clauses.extend(
            [
                f"COUNT({parse_date_clause}) AS nonnull_parsed_values{i}",
                f"MAX(IF({column.name} IS NOT NULL AND {parse_date_clause} IS NULL, {column.name}, NULL)) AS example_unparsed_value{i}",
            ]
        )

    columns = ",\n     ".join(column_clauses)
    query_string = f"""
    SELECT
     {columns}
    FROM
     `{project_id}.{raw_data_dataset}.{file_tag}`;
    """
    query_job = bq_client.run_query_async(query_str=query_string, use_query_cache=True)
    query_results = one(query_job.result())
    num_nonnull_values = query_results["nonnull_values"]
    if not num_nonnull_values:
        print(f"File {file_tag}, column {column.name}: All values are null - skipping")
        return column

    found_partial_parsers = False
    for i, datetime_sql_parser in enumerate(parsers_to_try):
        num_nonnull_parsed_values = query_results[f"nonnull_parsed_values{i}"]
        example_unparsed_value = query_results[f"example_unparsed_value{i}"]

        if num_nonnull_values == num_nonnull_parsed_values:
            print(
                f"File {file_tag}, column {column.name}: Updated parser to "
                f"{datetime_sql_parser}"
            )
            return attr.evolve(column, datetime_sql_parsers=[datetime_sql_parser])

        if num_nonnull_parsed_values:
            # If we get here, we found some but not all values that matched this parser
            found_partial_parsers = True
            print(
                f"File {file_tag}, column {column.name}: Found "
                f"{num_nonnull_parsed_values} out of {num_nonnull_values} matching "
                f"{datetime_sql_parser}. First non-matching value: "
                f"{example_unparsed_value}."
            )

    if not found_partial_parsers:
        example_nonnull_value = query_results["example_nonnull_value"]
        print(
            f"File {file_tag}, column {column.name}: Did not find any working parsers. "
            f"Example non-null value: {example_nonnull_value}"
        )
    return column


def update_parsers_in_region(
    state_code: StateCode,
    project_id: str,
    file_tags: List[str],
    sandbox_dataset_prefix: Optional[str],
    parser: Optional[str],
) -> None:
    """Update datetime columns' datetime parsers in raw data configs"""
    # Get all the configs to update
    region_config = get_region_raw_file_config(state_code.value)
    all_raw_file_configs: List[DirectIngestRawFileConfig] = list(
        region_config.raw_file_configs.values()
    )
    datetime_configs = [
        config for config in all_raw_file_configs if config.current_datetime_cols
    ]
    configs_to_update = [] if file_tags else datetime_configs
    for file_tag in file_tags:
        config = region_config.raw_file_configs[file_tag]
        if config not in all_raw_file_configs:
            print(f"File {file_tag} doesn't exist in state {state_code}.")
        elif config not in datetime_configs:
            print(f"File {file_tag} doesn't have datetime cols in state {state_code}.")
        else:
            configs_to_update.append(config)

    if not configs_to_update:
        sys.exit(1)

    # Get the parsers to try
    if parser:
        # use the user-provided parser
        parsers_to_try = [parser]
    elif regional_parsers := region_config.get_datetime_parsers():
        # if no parser was provided, get all datetime parsers in use across the whole state
        parsers_to_try = list(regional_parsers)
    else:
        # if the state has no parsers, use the default list
        parsers_to_try = DEFAULT_PARSERS_TO_TRY

    # Test each datetime parser against each table in need of parsers
    bq_client = BigQueryClientImpl()
    raw_data_dataset = raw_tables_dataset_for_region(
        state_code=state_code,
        # Raw data is only directly uploaded by states to PRIMARY.
        instance=DirectIngestInstance.PRIMARY,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )
    raw_data_config_writer = RawDataConfigWriter()
    default_config = region_config.default_config()
    for original_config in configs_to_update:
        new_columns = [
            (
                column
                if not column.is_datetime or column.datetime_sql_parsers
                else _update_parsers_in_column(
                    project_id,
                    original_config.file_tag,
                    column,
                    raw_data_dataset,
                    bq_client,
                    parsers_to_try,
                )
            )
            for column in original_config.current_columns
        ]
        if original_config.current_columns != new_columns:
            updated_config = attr.evolve(original_config, columns=new_columns)
            raw_data_config_writer.output_to_file(
                raw_file_config=updated_config,
                output_path=original_config.file_path,
                default_encoding=default_config.default_encoding,
                default_separator=default_config.default_separator,
                default_ignore_quotes=default_config.default_ignore_quotes,
                default_export_lookback_window=default_config.default_export_lookback_window,
                default_no_valid_primary_keys=default_config.default_no_valid_primary_keys,
                default_line_terminator=default_config.default_line_terminator,
                default_update_cadence=default_config.default_update_cadence,
                default_infer_columns_from_config=default_config.default_infer_columns_from_config,
                default_import_blocking_validation_exemptions=default_config.default_import_blocking_validation_exemptions,
            )
            print(f"File {original_config.file_tag}: Updates persisted")


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the named arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state-code",
        dest="state_code",
        help="State to which this config belongs in the form US_XX.",
        type=StateCode,
        choices=list(StateCode),
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

    parser.add_argument(
        "--parser",
        help="Parser to try. Should be in a format using SAFE.PARSE_DATETIME"
        "with an expression involving the string literal {col_name}.",
        type=str,
        default=None,
        required=False,
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


def main() -> None:
    """
    Parse/validate arguments and hydrate the requested files.
    """
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)

    if args.parser is not None and not re.match(
        DATETIME_SQL_REGEX, args.parser.strip()
    ):
        print("The provided datetime parser does not match the SQL parser format")
        sys.exit(1)

    with metadata.local_project_id_override(args.project_id):
        update_parsers_in_region(
            args.state_code,
            args.project_id,
            args.file_tags,
            args.sandbox_dataset_prefix,
            args.parser,
        )


if __name__ == "__main__":
    main()
