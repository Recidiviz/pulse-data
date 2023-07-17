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

from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export.view_export_manager import execute_metric_view_data_export


def parse_arguments() -> argparse.Namespace:
    """Parses arguments for the Metric View Export process."""
    parser = argparse.ArgumentParser()

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

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()

    execute_metric_view_data_export(
        state_code=args.state_code,
        export_job_name=args.export_job_name,
        sandbox_prefix=args.sandbox_prefix,
        destination_override=args.destination_override,
    )
