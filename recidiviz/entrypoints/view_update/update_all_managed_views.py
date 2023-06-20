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
"""Script for manage view updates to occur - to be called only within the Airflow DAG's
KubernetesPodOperator."""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.view_update_manager import execute_update_all_managed_views
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses arguments for the managed views update process."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        help="The project that the refresh should occur in",
        choices=GCP_PROJECTS,
        required=True,
    )
    parser.add_argument(
        "--sandbox_prefix",
        help="The sandbox prefix for which the refresh needs to write to",
        type=str,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(project_id_override=known_args.project_id):
        execute_update_all_managed_views(
            known_args.project_id, known_args.sandbox_prefix
        )
