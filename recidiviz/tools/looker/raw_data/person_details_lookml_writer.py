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
import os

from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    generate_lookml_views,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation


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


def remove_lookml_files_from(directory: str) -> None:
    """
    Removes all LookML view files from the given directory
    """
    for path, _, filenames in os.walk(directory):
        for file in filenames:
            if file.endswith(".lkml"):
                os.remove(os.path.join(path, file))


def write_lookml_files(looker_dir: str) -> None:
    """
    Write state raw data LookML views, explores and dashboards to the given directory,
    which should be a path to the local copy of the looker repo
    """
    if os.path.basename(looker_dir).lower() != "looker" and not prompt_for_confirmation(
        f"Warning: .lkml files will be deleted/overwritten in {looker_dir}\nProceed?"
    ):
        return
    # Remove existing LookML files in case the configs list has changed
    remove_lookml_files_from(looker_dir)

    # TODO(#21937): Generate views here
    # TODO(#21938): Generate explores here
    # TODO(#21939): Generate dashboards here
    generate_lookml_views(looker_dir)


if __name__ == "__main__":
    args = parse_arguments()
    write_lookml_files(args.looker_dir)
