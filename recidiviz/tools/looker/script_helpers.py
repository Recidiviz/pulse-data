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
"""Helpers for LookML generation scripts."""
import argparse
import os

import recidiviz
from recidiviz.tools.looker.constants import (
    GENERATED_DIR_ROOT,
    GENERATED_SUBDIR,
    LOOKER_REPO_NAME,
    VIEWS_DIR,
)


def remove_lookml_files_from(directory: str) -> None:
    """
    Removes all LookML files from the given directory
    """
    for path, _, filenames in os.walk(directory):
        for file in filenames:
            if file.endswith(".lkml") or file.endswith(".lookml"):
                os.remove(os.path.join(path, file))


# TODO(#36190) Remove option to write directly to looker repo
# in favor of always writing to recidiviz/tools/looker/generated
# then copying generated files to the looker repo.
def parse_and_validate_output_dir_arg() -> str:
    """Parses command line arguments to get the output directory for LookML files."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--looker-repo-root",
        help="Specifies local path to the Looker repo, where all files will be saved to",
        type=str,
        required=False,
        default=None,
    )

    args = parser.parse_args()

    if args.looker_repo_root:
        if os.path.basename(args.looker_repo_root).lower() != LOOKER_REPO_NAME:
            raise ValueError(
                f"Expected looker_repo_root to be at the root of [{LOOKER_REPO_NAME}] repo, but instead got [{args.looker_repo_root}]"
            )

    return args.looker_repo_root or os.path.join(
        os.path.dirname(recidiviz.__file__), GENERATED_DIR_ROOT
    )


def get_generated_views_path(output_dir: str, module_name: str) -> str:
    """
    Returns the path to the generated views directory for a given module name.
    "module" is a loose term and can be any string that represents a logical grouping.
    TODO(#36190) Refactor looker repo so that all generated files have a common generated/ root
    directory. ex `views/{module_name}/generated/` -> `generated/views/{module_name}/`
    """
    return os.path.join(output_dir, VIEWS_DIR, module_name, GENERATED_SUBDIR)
