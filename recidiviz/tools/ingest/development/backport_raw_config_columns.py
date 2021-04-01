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
"""Tool to backport a state's raw config columns from existing raw data in BigQuery.

This reads all of the raw files in BigQuery for the given state and prints out YAML-formatted raw file config for
all columns in all files. This does not populate all of the column configuration but does at least rapidly generate
the entire set of columns for all raw files.

This script is only useful for bootstrapping raw file column config for pre-existing but incompletely-configured states.
Thus it probably only needs to be used a few times.

Example Usage:
    python -m recidiviz.tools.ingest.development.backport_raw_config_columns --state-code US_ND --project-id recidiviz-staging
"""
import argparse
import logging
import sys

from typing import List, Dict

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants import states
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    RawTableColumnInfo,
)
from recidiviz.utils import metadata


def _get_columns_by_file(
    state_code: str, project_id: str
) -> Dict[str, List[RawTableColumnInfo]]:
    """Creates a list of RawTableColumnInfo for each raw file in a given state"""
    columns_by_file: Dict[str, List[RawTableColumnInfo]] = {}

    raw_data_dataset = f"{state_code.lower()}_raw_data"

    query_string = f"""
SELECT
 * EXCEPT(is_generated, generation_expression, is_stored, is_updatable)
FROM
 `{project_id}.{raw_data_dataset}.INFORMATION_SCHEMA.COLUMNS`
ORDER BY
  table_name ASC, ordinal_position ASC
"""

    bq_client = BigQueryClientImpl()
    query_job = bq_client.run_query_async(query_string)
    for row in query_job:
        column_name = row["column_name"]
        if column_name in {"file_id", "update_datetime"}:
            continue

        file_name = row["table_name"]
        is_datetime = row["data_type"].upper() == "DATETIME"

        if file_name not in columns_by_file:
            columns_by_file[file_name] = []

        column_info = RawTableColumnInfo(
            name=column_name, is_datetime=is_datetime, description="TKTK"
        )
        columns_by_file[file_name].append(column_info)

    return columns_by_file


def _generate_skeleton_config_for_file(
    file_name: str, columns: List[RawTableColumnInfo]
) -> str:
    config = f"# {file_name}\n\ncolumns:\n"

    def _generate_column_config_string(column: RawTableColumnInfo) -> str:
        config_string = f"  - name: {column.name}"
        if column.is_datetime:
            config_string += "\n    is_datetime: True"
        config_string += f"\n    description: |-\n      {column.description}"
        return config_string

    config += "\n".join([_generate_column_config_string(column) for column in columns])

    return config


def _generate_skeleton_config(state_code: str, project_id: str) -> str:
    columns_by_file = _get_columns_by_file(state_code, project_id)

    config_strings_by_file = []
    for file_name, columns in columns_by_file.items():
        config_strings_by_file.append(
            _generate_skeleton_config_for_file(file_name, columns)
        )

    return "\n\n".join(config_strings_by_file)


def main(state_code: str, project_id: str) -> None:
    config = _generate_skeleton_config(state_code, project_id)
    print(config)


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
        choices=["recidiviz-staging", "recidiviz-123"],
        required=True,
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)

    with metadata.local_project_id_override(args.project_id):
        main(args.state_code, args.project_id)
