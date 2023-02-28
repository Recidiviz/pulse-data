# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

    to copy all files (to go/airflow-experiment):
    python -m recidiviz.tools.airflow.copy_source_files_to_experiment_composer
        --environment experiment-v2 --dry-run False

    to copy all files (to go/airflow-experiment-2):
    python -m recidiviz.tools.airflow.copy_source_files_to_experiment_composer
        --environment experiment-2 --dry-run False

    to copy only specific files:
    python -m recidiviz.tools.airflow.copy_source_files_to_experiment_composer
       --dry-run False --environment experiment-v2
       --files recidiviz/airflow/dags/calculation_dag.py recidiviz/airflow/dags/operators/recidiviz_dataflow_operator.py
"""
import argparse
import logging
import os
from glob import glob

import yaml

import recidiviz
from recidiviz.common.file_system import is_valid_code_path
from recidiviz.tools.gsutil_shell_helpers import gsutil_cp
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

DAGS_FOLDER = "dags"
ROOT = os.path.dirname(recidiviz.__file__)

AIRFLOW_SOURCE_FILES = os.path.join(
    ROOT,
    "tools/deploy/terraform/config/cloud_composer_airflow_files_to_copy.yaml",
)
OTHER_SOURCE_FILES = os.path.join(
    ROOT,
    "tools/deploy/terraform/config/cloud_composer_source_files_to_copy.yaml",
)

SOURCE_FILE_YAML_PATHS = [AIRFLOW_SOURCE_FILES, OTHER_SOURCE_FILES]

EXPERIMENT_V2 = "us-east1-experiment-v2-fbddbedc-bucket"
EXPERIMENT_2 = "us-central1-experiment-2-8bb6ce5a-bucket"


def gcloud_path_for_local_path(local_path: str) -> str:
    dags_local_path = f"{ROOT}/airflow/dags/"
    if local_path.startswith(dags_local_path):
        prefix_to_replace = dags_local_path
        prefix_to_replace_with = ""
    else:
        prefix_to_replace = f"{ROOT}/"
        prefix_to_replace_with = "recidiviz/"

    return os.path.join(prefix_to_replace_with, local_path[len(prefix_to_replace) :])


def main(files: list[str], environment: str, dry_run: bool) -> None:
    """
    Takes in arguments and copies appropriate files to the appropriate environment.
    """
    experiment_cloud_composer_bucket = EXPERIMENT_V2

    if environment == "experiment-2":
        experiment_cloud_composer_bucket = EXPERIMENT_2

    if files:
        local_path_list = files
    else:
        local_path_list = []
        for source_file_yaml in SOURCE_FILE_YAML_PATHS:
            with open(source_file_yaml, encoding="utf-8") as f:
                file_patterns = yaml.safe_load(f)
                for path, pattern in file_patterns:
                    local_path_list.extend(
                        glob(f"{path.replace('recidiviz', ROOT)}/{pattern}")
                    )

    for local_path in local_path_list:
        gcloud_path = gcloud_path_for_local_path(local_path)
        gcs_url = f"gs://{experiment_cloud_composer_bucket}/{DAGS_FOLDER}/{gcloud_path}"
        message = f"COPY [{local_path}] to [{gcs_url}]"
        if not is_valid_code_path(local_path):
            continue
        if dry_run:
            logging.info("%s %s", "[DRY RUN]", message)
        else:
            logging.info(message)
            gsutil_cp(local_path, gcs_url)


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
        "--environment",
        default="experiment-v2",
        required=True,
        choices=["experiment-v2", "experiment-2"],
        help="Specifies which experiment environment to copy files to. 'experiment-v2' points to go/airflow-experiment and 'experiment-2' points to go/airflow-experiment-2",
    )

    args = parser.parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        main(args.files, args.environment, args.dry_run)
