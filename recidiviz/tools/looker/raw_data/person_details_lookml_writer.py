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
"""A script for building a set of LookML views, explores and dashboards 
for raw data tables and writing them to files.

Run the following to write files to the specified directory DIR:
python -m recidiviz.tools.looker.raw_data.person_details_lookml_writer --looker_repo_root [DIR]
"""

import argparse

from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    generate_lookml_views,
)


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--looker_repo_root",
        dest="looker_dir",
        help="Specifies local path to the Looker repo, where all files will be saved to",
        type=str,
        required=True,
    )

    return parser.parse_args()


def main(looker_dir: str) -> None:
    # TODO(#21937): Generate views here
    # TODO(#21938): Generate explores here
    # TODO(#21939): Generate dashboards here
    generate_lookml_views(looker_dir)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.looker_dir)
