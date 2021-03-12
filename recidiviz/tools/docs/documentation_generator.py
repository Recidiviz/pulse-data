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

"""A script which will be called using a pre-commit githook to generate portions of a State Ingest Specification."""

import logging
import os
import subprocess
import sys

import argparse
from typing import List, Optional, Sequence, Set

import recidiviz
from recidiviz.ingest.direct import regions as regions_module
from recidiviz.ingest.direct.direct_ingest_documentation_generator import (
    DirectIngestDocumentationGenerator,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def generate_raw_data_documentation_for_region(region_code: str) -> bool:
    """
    Parses the files available under `recidiviz/ingest/direct/regions/{region_code}/raw_data/` to produce documentation
    which is suitable to be added to the region ingest specification. Overwrites or creates the markdown file
    for a given region.

    Returns True if files were modified, False otherwise.
    """
    documentation_generator = DirectIngestDocumentationGenerator()
    documentation = documentation_generator.generate_raw_file_docs_for_region(
        region_code.lower()
    )
    ingest_docs_path = "docs/ingest"
    markdown_dir_path = os.path.join(ingest_docs_path, region_code.lower())
    os.makedirs(markdown_dir_path, exist_ok=True)
    markdown_file_path = os.path.join(markdown_dir_path, "raw_data.md")

    prior_documentation = None
    if os.path.exists(markdown_file_path):
        with open(markdown_file_path, "r") as raw_data_md_file:
            prior_documentation = raw_data_md_file.read()

    if prior_documentation != documentation:
        with open(markdown_file_path, "w") as raw_data_md_file:
            raw_data_md_file.write(documentation)
            return True
    return False


def get_touched_raw_data_regions(touched_files: Optional[List[str]]) -> Set[str]:
    """Returns the touched regions' codes.

    If modified files are not provided, greps for touched files in the direct ingest
    regions directories.
    """

    if not touched_files:
        regions_dir_path = os.path.relpath(
            os.path.dirname(regions_module.__file__),
            os.path.dirname(os.path.dirname(recidiviz.__file__)),
        )
        res = subprocess.Popen(
            f'git diff --cached --name-only | grep "{regions_dir_path}"',
            shell=True,
            stdout=subprocess.PIPE,
        )
        stdout, _stderr = res.communicate()
        touched_files = stdout.decode().splitlines()

    region_names = {file.split("/")[4] for file in touched_files}
    return {
        region_name
        for region_name in region_names
        # Skip any that aren't directories (e.g. __init__.py)
        if os.path.isdir(
            os.path.join(os.path.dirname(regions_module.__file__), region_name)
        )
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "filenames",
        nargs="*",
        help="Modified files to indicate which regions need their docs to be regenerated. "
        "Paths must be relative to the root of the repository. "
        "If none are provided, will use `git diff` to determine modified files.",
    )
    args = parser.parse_args(argv)

    # Arbitrary project ID - we just need to build views in order to obtain raw table dependencies
    with local_project_id_override(GCP_PROJECT_STAGING):
        modified = False
        touched_raw_data_regions = get_touched_raw_data_regions(args.filenames)
        for region_code in touched_raw_data_regions:
            logging.info(
                "Generating raw data documentation for region [%s]", region_code
            )
            modified |= generate_raw_data_documentation_for_region(region_code)
        return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
