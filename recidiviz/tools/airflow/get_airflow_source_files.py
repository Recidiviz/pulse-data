# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Returns a comma separated list of Airflow source files in a json object."""
import argparse
import json
import logging
import os.path

import recidiviz
from recidiviz.tools.airflow.copy_source_files_to_experiment_composer import (
    get_airflow_source_file_paths,
)
from recidiviz.utils.params import str_to_bool


def main(dry_run: bool, output_path: str) -> None:
    """
    Gets the list of Airflow source files and outputs it as json map of source file path to destination file path.
    Outputs to stdout for use in terraform. Dry run mode prints the source files instead of outputting the json.
    """
    source_files = [
        os.path.relpath(file, os.path.dirname(os.path.dirname(recidiviz.__file__)))
        for file in get_airflow_source_file_paths()
    ]

    source_files_to_destination = {
        file: os.path.basename(file) if file.endswith("dag.py") else file
        for file in source_files
    }
    if not dry_run:
        json_str = json.dumps(source_files_to_destination)
        if output_path:
            with open(output_path, mode="w", encoding="utf-8") as output_file:
                output_file.write(json_str)
        else:
            print(json_str)
    else:
        logging.info("Dry run mode, listing source files.")
        for source, destination in source_files_to_destination.items():
            logging.info("Source file: %s, destination: %s", source, destination)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs in dry-run mode, prints the source files it would list.",
    )

    parser.add_argument(
        "--output-path",
        type=str,
        required=False,
        help="If specified, outputs the source file json to the provided path",
    )

    args = parser.parse_args()
    main(args.dry_run, args.output_path)
