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
"""
import argparse
import logging
import subprocess
import tempfile

from recidiviz.tools.airflow.get_airflow_source_files import (
    main as get_airflow_source_files,
)
from recidiviz.tools.gsutil_shell_helpers import gcloud_storage_rsync_airflow_command
from recidiviz.utils.environment import local_only
from recidiviz.utils.params import str_to_bool


@local_only
def copy_source_files_to_experiment(
    gcs_uri: str,
    dry_run: bool,
) -> None:
    """
    Takes in arguments and copies appropriate files to the appropriate environment.
    """
    with tempfile.TemporaryDirectory() as directory:
        get_airflow_source_files(dry_run=False, output_path=directory)
        subprocess.run(
            gcloud_storage_rsync_airflow_command(
                directory, gcs_uri, dry_run=dry_run, use_gsutil=True
            ),
            stdout=subprocess.PIPE,
            check=True,
        )  # nosec B603


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
        "--gcs_uri",
        required=True,
        type=str,
        help="Specifies the bucket to copy files to",
    )

    args = parser.parse_args()
    copy_source_files_to_experiment(args.gcs_uri, args.dry_run)
