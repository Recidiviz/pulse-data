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
    python -m recidiviz.tools.ingest.development.fetch_column_values_for_state --state-code US_ND --project-id recidiviz-staging
"""
import argparse
import logging
import sys

from typing import List

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants import states
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    RawTableColumnInfo,
    DirectIngestRawFileConfig,
    ColumnEnumValueInfo,
)
from recidiviz.ingest.direct.query_utils import (
    get_region_raw_file_config,
)
from recidiviz.tests.ingest.direct.direct_ingest_util import PLACEHOLDER_TO_DO_STRING
from recidiviz.tools.ingest.development.raw_data_config_writer import (
    RawDataConfigWriter,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION


def get_known_values(
    column: RawTableColumnInfo,
    file_tag: str,
    region_code: str,
    project_id: str,
    bq_client: BigQueryClientImpl,
) -> RawTableColumnInfo:
    """Creates a list of possible enum values for a column in a given table"""
    # Only fetch known values for enum columns
    if not column.is_enum:
        return column

    raw_data_dataset = f"{region_code.lower()}_raw_data"

    query_string = f"""
SELECT
 DISTINCT {column.name} as values
FROM
 `{project_id}.{raw_data_dataset}.{file_tag}`
WHERE {column.name} IS NOT NULL
ORDER BY
  {column.name}
"""
    query_job = bq_client.run_query_async(query_string)
    distinct_values = [row["values"] for row in query_job]
    existing_values = [enum.value for enum in column.known_values_nonnull]

    new_known_values_list = column.known_values_nonnull.copy()
    for value in distinct_values:
        if value not in existing_values:
            new_known_values_list.append(
                ColumnEnumValueInfo(value=value, description=PLACEHOLDER_TO_DO_STRING)
            )
    new_known_values_list.sort()
    return RawTableColumnInfo(
        name=column.name,
        description=column.description,
        is_datetime=column.is_datetime,
        known_values=new_known_values_list,
    )


def update_enum_known_values(
    original_config: DirectIngestRawFileConfig,
    region_code: str,
    project_id: str,
    bq_client: BigQueryClientImpl,
) -> DirectIngestRawFileConfig:
    new_columns = [
        get_known_values(
            column, original_config.file_tag, region_code, project_id, bq_client
        )
        for column in original_config.columns
    ]

    return DirectIngestRawFileConfig(
        file_tag=original_config.file_tag,
        file_path=original_config.file_path,
        file_description=original_config.file_description,
        primary_key_cols=original_config.primary_key_cols,
        columns=new_columns,
        encoding=original_config.encoding,
        separator=original_config.separator,
        ignore_quotes=original_config.ignore_quotes,
        supplemental_order_by_clause=original_config.supplemental_order_by_clause,
        always_historical_export=original_config.always_historical_export,
    )


def main(state_code: str, project_id: str) -> None:
    region_config = get_region_raw_file_config(state_code)

    default_config = region_config.default_config()

    bq_client = BigQueryClientImpl()
    # Only refresh if a config has enum columns
    for original_raw_file_config in [
        config for config in region_config.raw_file_configs.values() if config.has_enums
    ]:
        updated_raw_file_config = update_enum_known_values(
            original_raw_file_config, state_code, project_id, bq_client
        )
        raw_data_config_writer = RawDataConfigWriter()
        raw_data_config_writer.output_to_file(
            updated_raw_file_config,
            original_raw_file_config.file_path,
            default_config.default_encoding,
            default_config.default_separator,
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

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)

    with metadata.local_project_id_override(args.project_id):
        main(args.state_code, args.project_id)
