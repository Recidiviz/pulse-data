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
import logging
import os.path
import shutil
from glob import glob

import yaml

import recidiviz
from recidiviz.tools.file_dependencies import (
    EntrypointDependencies,
    convert_path_to_recidiviz_module,
)
from recidiviz.utils.params import str_to_bool

DAGS_FOLDER = "dags"
ROOT = os.path.dirname(recidiviz.__file__)

SOURCE_FILE_YAML_PATH = os.path.join(
    ROOT,
    "tools/deploy/terraform/config/cloud_composer_source_files_to_copy.yaml",
)


def _get_paths_list_from_file_pattern(file_pattern: tuple[str, str]) -> list[str]:
    path, pattern = file_pattern
    return glob(f"{path.replace('recidiviz', ROOT)}/{pattern}", recursive=True)


def get_airflow_source_file_paths() -> list[str]:
    """Lists all paths that are airflow source files."""
    # copy all dag dependency files
    dag_files: list[str] = _get_paths_list_from_file_pattern(
        ("recidiviz/airflow/dags", "*dag*.py")
    )
    explicitly_listed_dependency_files: list[str] = []

    with open(SOURCE_FILE_YAML_PATH, encoding="utf-8") as f:
        file_patterns = yaml.safe_load(f)
        for file_pattern in file_patterns:
            explicitly_listed_dependency_files.extend(
                _get_paths_list_from_file_pattern(file_pattern)
            )

    dependencies = EntrypointDependencies()
    for dag_file in dag_files:
        dependencies.add_dependencies_for_entrypoint(
            convert_path_to_recidiviz_module(dag_file)
        )

    for explicitly_listed_dependency_file in explicitly_listed_dependency_files:
        if explicitly_listed_dependency_file.endswith(".py"):
            dependencies.add_dependencies_for_entrypoint(
                convert_path_to_recidiviz_module(explicitly_listed_dependency_file)
            )

    return (
        list(dependencies.all_module_dependency_source_files)
        + explicitly_listed_dependency_files
    )


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
        for source, destination in source_files_to_destination.items():
            output_file = f"{output_path}/{destination}"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            shutil.copy2(source, output_file)
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
