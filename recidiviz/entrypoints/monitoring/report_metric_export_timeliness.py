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


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    known_args, _ = parse_arguments(sys.argv)

    report_export_timeliness_metrics()
