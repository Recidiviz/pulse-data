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

"""A script which will be called using a pre-commit githook to generate portions of a State Ingest Specification.

Can be run on-demand via:
    $ pipenv run python -m recidiviz.tools.docs.region_documentation_generator [--force-all]
"""

import argparse
import logging
import os
import subprocess
import sys
from os import listdir
from os.path import isdir, isfile, join
from typing import List, Optional, Sequence, Set

import recidiviz
from recidiviz.common.constants.states import StateCode
from recidiviz.common.file_system import delete_files, get_all_files_recursive
from recidiviz.ingest.direct import regions as regions_module
from recidiviz.ingest.direct.direct_ingest_documentation_generator import (
    STATE_RAW_DATA_FILE_HEADER_PATH,
    DirectIngestDocumentationGenerator,
)
from recidiviz.ingest.direct.direct_ingest_regions import (
    get_supported_direct_ingest_region_codes,
)
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import DOCS_ROOT_PATH, persist_file_contents
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INGEST_CATALOG_ROOT = os.path.join(DOCS_ROOT_PATH, "ingest")


def generate_raw_data_documentation_for_region(region_code: str) -> bool:
    """
    Parses the files available under `recidiviz/ingest/direct/regions/{region_code}/raw_data/` to produce documentation
    which is suitable to be added to the region ingest specification. Overwrites or creates one Markdown file
    per raw data file, plus one additional header file, for a given region.

    Returns True if files were modified, False otherwise.
    """
    documentation_generator = DirectIngestDocumentationGenerator()
    markdown_dir_path = os.path.join(INGEST_CATALOG_ROOT, region_code.lower())

    docs_per_file = documentation_generator.generate_raw_file_docs_for_region(
        region_code.lower()
    )

    raw_data_dir = os.path.join(markdown_dir_path, "raw_data")
    existing_raw_data_docs_for_region: Set[str] = get_all_files_recursive(raw_data_dir)
    os.makedirs(raw_data_dir, exist_ok=True)

    new_docs_for_region: Set[str] = set()

    anything_modified = False
    for file_name, file_contents in docs_per_file.items():
        if file_name == STATE_RAW_DATA_FILE_HEADER_PATH:
            markdown_file_path = os.path.join(markdown_dir_path, file_name)
        else:
            markdown_file_path = os.path.join(raw_data_dir, file_name)

        anything_modified |= persist_file_contents(file_contents, markdown_file_path)

        # Keep track of new docs
        new_docs_for_region.add(markdown_file_path)

    # Delete any deprecated files
    deprecated_files = existing_raw_data_docs_for_region.difference(new_docs_for_region)
    if deprecated_files:
        delete_files(deprecated_files, delete_empty_dirs=True)
        anything_modified |= True

    return anything_modified


def _create_ingest_catalog_summary() -> List[str]:
    """Creates the State Ingest Catalog portion of SUMMARY.md, as a list of lines."""
    ingest_catalog_states = sorted(
        [
            f.lower()
            for f in listdir(INGEST_CATALOG_ROOT)
            if isdir(join(INGEST_CATALOG_ROOT, f))
        ]
    )

    ingest_catalog_summary = ["## State Ingest Catalog\n\n"]

    for state in ingest_catalog_states:
        if StateCode.is_state_code(state):
            state_code = StateCode(state.upper())
            state_name = state_code.get_state()
        else:
            raise ValueError(
                f"Folder under {INGEST_CATALOG_ROOT} named {state} is not a valid state code"
            )

        ingest_catalog_summary.extend(
            [
                f"- [{state_name}](ingest/{state}/{state}.md)\n",
                f"  - [Schema Mappings](ingest/{state}/schema_mappings.md)\n",
                f"  - [Raw Data Description](ingest/{state}/raw_data.md)\n",
            ]
        )

        raw_data_dir = join(INGEST_CATALOG_ROOT, state, "raw_data")
        if not isdir(raw_data_dir):
            continue
        raw_data_files = sorted(
            [f for f in listdir(raw_data_dir) if isfile(join(raw_data_dir, f))]
        )

        for file_name in raw_data_files:
            ingest_catalog_summary.append(
                f"    - [{file_name[:-3]}](ingest/{state}/raw_data/{file_name})\n"
            )
    return ingest_catalog_summary


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
        res = subprocess.run(
            f'git diff --cached --name-only | grep "{regions_dir_path}"',
            shell=True,
            stdout=subprocess.PIPE,
            check=True,
        )
        touched_files = res.stdout.decode().splitlines()

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
    """Generates direct ingest region documentation."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "filenames",
        nargs="*",
        help="Modified files to indicate which regions need their docs to be regenerated. "
        "Paths must be relative to the root of the repository. "
        "If none are provided, will use `git diff` to determine modified files.",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        default=False,
        help="Generate docs for all regions, even if they are not modified.",
    )
    args = parser.parse_args(argv)

    # Arbitrary project ID - we just need to build views in order to obtain raw table dependencies
    with local_project_id_override(GCP_PROJECT_STAGING):
        modified = False
        if not args.force_all:
            touched_raw_data_regions = get_touched_raw_data_regions(args.filenames)
        else:
            touched_raw_data_regions = get_supported_direct_ingest_region_codes()
        for region_code in touched_raw_data_regions:
            if not StateCode.is_state_code(region_code):
                logging.info(
                    "Skipping raw data documentation for non-state region [%s]",
                    region_code,
                )
                continue
            logging.info(
                "Generating raw data documentation for region [%s]", region_code
            )
            modified |= generate_raw_data_documentation_for_region(region_code)
        if modified:
            update_summary_file(
                _create_ingest_catalog_summary(), "## State Ingest Catalog"
            )
        return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
