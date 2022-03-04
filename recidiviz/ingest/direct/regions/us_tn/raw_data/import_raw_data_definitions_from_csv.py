#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
""" Script that imports CSVs with the following formatting (and no header row) into raw YAML files:

        table, table_description, column_name, column_type, column_description,
        table, table_description, column_name2, column_type2, column_description2,
        table, table_description, column_name2, column_type3, column_description3,
        table2, table_description2, column_name2, column_type3, column_description3,
        ...
    Note: this sets primary keys to `[]` with a TO DO above it, and requires manually setting the primary keys
    after the script runs.

    Usage:
        python -m recidiviz.ingest.direct.regions.us_tn.raw_data.import_raw_data_definitions_from_csv \
        --absolute_file_path /path/to/file/TableDescriptions.csv
"""
import argparse
import csv
import os
import re
import sys
from typing import Dict, List, Tuple

import recidiviz
from recidiviz.tools.docs.utils import persist_file_contents

ENTITY_DOCS_ROOT = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "ingest",
    "direct",
    "regions",
    "us_tn",
    "raw_data",
)


def import_csv_to_raw_yaml(absolute_file_path: str) -> None:
    """First iterate through the CSV and clean up and collect the row's information into a dictionary.
    Then iterate through the dictionary and build a string that contains the contents of the new YAML file.
    Finally, Persist the the string into the corresponding YAML file."""

    def strip_extra_whitespaces(contents: str) -> str:
        # Convert multiple consecutive spaces within a string into a single space.
        stripped_extra_whitespaces = re.sub(r" +", " ", contents)

        # Remove extra whitespace that may be at the end of the string.
        if stripped_extra_whitespaces[len(stripped_extra_whitespaces) - 1] == " ":
            return stripped_extra_whitespaces[:-1]

        return stripped_extra_whitespaces

    all_tables: Dict[Tuple[str, str], Dict[str, Tuple[str, str]]] = {}
    with open(
        absolute_file_path,
        newline="",
        encoding="utf-8-sig",
    ) as enum_csv:
        reader = csv.reader(enum_csv)
        for row in reader:
            table_name = strip_extra_whitespaces(row[0])
            table_description = strip_extra_whitespaces(row[1])
            column_name = strip_extra_whitespaces(row[2])
            column_data_type = strip_extra_whitespaces(row[3])
            definition = strip_extra_whitespaces(row[4])
            # If adding a new (table, table_description) to all_tables, initialize its associated value to an empty
            # dictionary.
            # pylint: disable=consider-iterating-dictionary
            if (table_name, table_description) not in all_tables.keys():
                all_tables[(table_name, table_description)] = {}
            all_tables[(table_name, table_description)][column_name] = (
                column_data_type,
                definition,
            )

    for (table_name, table_description) in all_tables:
        yaml_file_path = os.path.join(ENTITY_DOCS_ROOT, f"us_tn_{table_name}.yaml")
        new_file = ""
        new_file += f"file_tag: {table_name}\n"
        new_file += f"file_description: |-\n  {table_description}\n"
        new_file += "# TO DO: FILL OUT PRIMARY KEYS\nprimary_key_cols: []\n"
        new_file += "columns:\n"
        for column_name in all_tables[(table_name, table_description)]:
            table = all_tables[(table_name, table_description)]
            (column_data_type, definition) = table[column_name]
            new_file += f"  - name: {column_name}\n"
            if "date" in column_data_type:
                new_file += "    is_datetime: True\n"
            new_file += f"    description: |-\n      {definition}\n"
        persist_file_contents(new_file, yaml_file_path)

    print(
        "Completed importing table/column descriptions from CSV into raw data YAML "
        f"files for {len(all_tables.keys())} table(s)."
    )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--absolute_file_path",
        dest="absolute_file_path",
        help="The absolute file path of the CSV that is imported.",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    known_args, _ = parse_arguments(sys.argv)
    import_csv_to_raw_yaml(known_args.absolute_file_path)
