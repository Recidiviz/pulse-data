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
"""Script that aids Airflow development by copying over any Terraform defined
source files to the Airflow experiment environment in staging.

Typically, we should run the `environment_control` tool directly to update files.

python -m recidiviz.tools.airflow.environment_control update_files

    to copy all files:
    python -m recidiviz.tools.airflow.copy_source_files_to_experiment_composer \
        --gcs_uri gs://... --dry-run False

    to copy all files (to go/airflow-experiment-2):
    python -m recidiviz.tools.airflow.copy_source_files_to_experiment_composer \
        --gcs_uri gs://... --dry-run False

    to copy only specific files:
    python -m recidiviz.tools.airflow.copy_source_files_to_experiment_composer \
       --dry-run False --gcs_uri gs://... \
       --files recidiviz/airflow/dags/calculation_dag.py recidiviz/airflow/dags/operators/recidiviz_dataflow_operator.py
"""
import argparse
import logging
import os
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Tuple

import yaml

import recidiviz
from recidiviz.common.file_system import is_valid_code_path
from recidiviz.tools.file_dependencies import (
    EntrypointDependencies,
    convert_path_to_recidiviz_module,
)
from recidiviz.tools.gsutil_shell_helpers import gsutil_cp
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

DAGS_FOLDER = "dags"
ROOT = os.path.dirname(recidiviz.__file__)

SOURCE_FILE_YAML_PATH = os.path.join(
    ROOT,
    "tools/deploy/terraform/config/cloud_composer_source_files_to_copy.yaml",
)


def gcloud_path_for_local_path(local_path: str) -> str:
    dags_local_path = f"{ROOT}/airflow/dags/"
    if local_path.endswith("dag.py"):
        prefix_to_replace = dags_local_path
        prefix_to_replace_with = ""
    else:
        prefix_to_replace = f"{ROOT}/"
        prefix_to_replace_with = "recidiviz/"
    return os.path.join(prefix_to_replace_with, local_path[len(prefix_to_replace) :])


def upload_file(local_path: str, gcs_url: str, message: str) -> None:
    logging.info(message)
    gsutil_cp(local_path, gcs_url)


def _get_paths_list_from_file_pattern(file_pattern: Tuple[str, str]) -> List[str]:
    path, pattern = file_pattern
    return glob(f"{path.replace('recidiviz', ROOT)}/{pattern}", recursive=True)


def get_airflow_source_file_paths() -> List[str]:
    """Lists all paths that are airflow source files."""
    # copy all dag dependency files
    dag_files: List[str] = _get_paths_list_from_file_pattern(
        ("recidiviz/airflow/dags", "*dag*.py")
    )
    explicitly_listed_dependency_files: List[str] = []

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


def copy_source_files_to_experiment(
    gcs_uri: str,
    dry_run: bool,
    file_filter: Optional[List[str]] = None,
) -> None:
    """
    Takes in arguments and copies appropriate files to the appropriate environment.
    """
    thread_pool = ThreadPool(processes=10)

    if file_filter:
        local_path_list = [
            f"{ROOT}/{file.removeprefix('recidiviz/')}" for file in file_filter
        ]
    else:
        local_path_list = get_airflow_source_file_paths()

    logging.info("Copying %s files to bucket", len(local_path_list))
    # TODO(#23871): Add ability to delete files from bucket that are no longer in source.

    for local_path in local_path_list:
        gcloud_path = gcloud_path_for_local_path(local_path)
        gcs_url = f"{gcs_uri}/{gcloud_path}"
        message = f"COPY [{local_path}] to [{gcs_url}]"
        if not is_valid_code_path(local_path):
            continue
        if dry_run:
            logging.info("%s %s", "[DRY RUN]", message)
        else:
            thread_pool.apply_async(upload_file, args=(local_path, gcs_url, message))

    thread_pool.close()
    thread_pool.join()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs copy in dry-run mode, only prints the file copies it would do.",
    )

    parser.add_argument(
        "--files",
        nargs="+",
        help="If provided, only upload these files",
    )

    parser.add_argument(
        "--gcs_uri",
        required=True,
        type=str,
        help="Specifies the bucket to copy files to",
    )

    args = parser.parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        copy_source_files_to_experiment(
            args.gcs_uri, args.dry_run, file_filter=args.files
        )
