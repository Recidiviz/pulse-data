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

python -m recidiviz.tools.airflow.copy_source_files_to_experiment_composer --dry-run True"""
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

EXPERIMENT_CLOUD_COMPOSER_BUCKET = "us-east1-experiment-v2-fbddbedc-bucket"
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

FILE_TO_PATH_REPLACEMENT = [
    (AIRFLOW_SOURCE_FILES, f"{ROOT}/airflow/dags/", ""),
    (OTHER_SOURCE_FILES, f"{ROOT}/", "recidiviz/"),
]


def main(dry_run: bool) -> None:
    for source_file, path_replacement, path_to_replace_with in FILE_TO_PATH_REPLACEMENT:
        with open(source_file, encoding="utf-8") as f:
            file_patterns = yaml.safe_load(f)
            for path, pattern in file_patterns:
                for file in glob(f"{path.replace('recidiviz', ROOT)}/{pattern}"):
                    gcloud_path = file.replace(path_replacement, path_to_replace_with)
                    gcs_url = f"gs://{EXPERIMENT_CLOUD_COMPOSER_BUCKET}/{DAGS_FOLDER}/{gcloud_path}"
                    message = f"COPY [{file}] to [{gcs_url}]"
                    if not is_valid_code_path(file):
                        continue
                    if dry_run:
                        logging.info("%s %s", "[DRY RUN]", message)
                    else:
                        logging.info(message)
                        gsutil_cp(
                            file,
                            gcs_url,
                        )


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
    args = parser.parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        main(args.dry_run)
