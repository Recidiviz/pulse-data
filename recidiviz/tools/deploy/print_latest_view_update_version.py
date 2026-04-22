# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Prints the `data_platform_version` and `success_timestamp` of the most recent
successful row in `view_update_metadata.per_view_update_stats` for a project.

Output is tab-separated on a single line:
    <data_platform_version>\\t<success_timestamp_utc>

Used by `print_deployed_version.sh`; you can also invoke it directly:

    uv run python -m recidiviz.tools.deploy.print_latest_view_update_version \\
        --project_id recidiviz-123
"""
import argparse
import datetime
import logging
import sys

from recidiviz.tools.load_views_to_sandbox import get_latest_view_update_info
from recidiviz.utils.metadata import local_project_id_override


def main() -> int:
    info = get_latest_view_update_info()
    timestamp = info.success_timestamp.astimezone(datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    print(f"{info.data_platform_version}\t{timestamp}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()
    with local_project_id_override(args.project_id):
        sys.exit(main())
