# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Script for generating metric export timeliness monitoring time series.

Example usage (run from `pipenv shell`):

python -m recidiviz.entrypoints.monitoring.report_metric_export_timeliness \
    --project_id recidiviz-123
"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.monitoring.export_timeliness import report_export_timeliness_metrics
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        help="The project_id where the GCS metric export files exist",
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        report_export_timeliness_metrics()
