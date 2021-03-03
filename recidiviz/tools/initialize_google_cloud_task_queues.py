# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Script run on deploy that initializes all task queues with appropriate
configurations."""

import argparse
import logging
from recidiviz.utils import metadata

from recidiviz.common.google_cloud import google_cloud_task_queue_config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--project_id", required=True, help="Project to initialize queues for"
    )
    parser.add_argument(
        "--google_auth_token",
        required=True,
        help="Auth token (obtained via " "`gcloud auth print-access-token`).",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with metadata.local_project_id_override(args.project_id):
        google_cloud_task_queue_config.initialize_queues(
            google_auth_token=args.google_auth_token,
        )
