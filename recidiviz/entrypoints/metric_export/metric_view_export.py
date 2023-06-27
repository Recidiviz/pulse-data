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
"""Script for a Metric View Export to occur for a given export config to be called only
within the Airflow DAG's KubernetesPodOperator."""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export.view_export_manager import execute_metric_view_data_export
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses arguments for the Metric View Export process."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        help="The project that the export should occur in",
        choices=GCP_PROJECTS,
        required=True,
    )
    parser.add_argument(
        "--state_code",
        help="The state code that the export should occur for",
        type=StateCode,
        choices=list(StateCode),
    )
    parser.add_argument(
        "--export_job_name",
        help="The export job name that the export should occur for",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--sandbox_prefix",
        help="The sandbox prefix for which the export needs to write to",
        type=str,
    )
    parser.add_argument(
        "--destination_override",
        help="The destination override for which the export needs to write to",
        type=str,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args, unknown_args = parse_arguments(sys.argv[1:])

    with local_project_id_override(args.project_id):
        execute_metric_view_data_export(
            state_code=args.state_code,
            export_job_name=args.export_job_name,
            sandbox_prefix=args.sandbox_prefix,
            destination_override=args.destination_override,
        )
