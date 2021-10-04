# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Script for syncing static files to Google Cloud Storage.

Run the following command within a pipenv shell to execute:

python -m recidiviz.tools.deploy.deploy_static_files --project_id [PROJECT_ID] --dry_run
"""
import argparse
import logging
import os
import subprocess
import sys
from typing import List, Tuple

import recidiviz
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

RECIDIVIZ_ROOT = os.path.abspath(os.path.join(recidiviz.__file__, "../.."))


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument("--dry_run", dest="dry_run", action="store_true")

    parser.set_defaults(dry_run=False)

    return parser.parse_known_args(argv)


def sync_reporting_static_files(project_id: str, dry_run: bool) -> None:
    source_path = os.path.abspath(
        os.path.join(RECIDIVIZ_ROOT, "recidiviz/reporting/context/static")
    )
    dest_path = f"gs://{project_id}-report-images/po_monthly_report/static"

    logging.info("Updating static files for the reporting app in %s ...", project_id)
    logging.info("Dry run: %s", dry_run)

    cmd_args = ["gsutil", "rsync", "-r"]
    if dry_run:
        cmd_args.append("-n")
    cmd_args += [source_path, dest_path]

    subprocess.run(cmd_args, check=True)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    known_args, _ = parse_arguments(sys.argv)

    sync_reporting_static_files(known_args.project_id, known_args.dry_run)
