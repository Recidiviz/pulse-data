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

from typing import Set

import recidiviz
from recidiviz.ingest.direct import regions as regions_module
from recidiviz.ingest.direct.direct_ingest_documentation_generator import (
    DirectIngestDocumentationGenerator,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def generate_raw_data_documentation_for_region(region_code: str) -> None:
    """
    Parses the files available under `recidiviz/ingest/direct/regions/{region_code}/raw_data/` to produce documentation
    which is suitable to be added to the region ingest specification. Overwrites or creates the markdown file
    for a given region.
    """
    documentation_generator = DirectIngestDocumentationGenerator()
    documentation = documentation_generator.generate_raw_file_docs_for_region(
        region_code.lower()
    )
    ingest_docs_path = "docs/ingest"
    markdown_file_path = os.path.join(
        ingest_docs_path, f"{region_code.lower()}/raw_data.md"
    )
    with open(markdown_file_path, "w") as raw_data_md_file:
        raw_data_md_file.write(documentation)
    res = subprocess.Popen(
        f"git add {markdown_file_path}", shell=True, stdout=subprocess.PIPE
    )
    _stdout, _stderr = res.communicate()


def get_touched_raw_data_regions() -> Set[str]:
    """Greps for touched files in the direct ingest regions directories and returns the touched regions' codes"""

    regions_dir_path = os.path.relpath(
        os.path.dirname(regions_module.__file__),
        os.path.dirname(os.path.dirname(recidiviz.__file__)),
    )
    res = subprocess.Popen(
        f'git diff --cached --name-only | grep "{regions_dir_path}" | cut -d / -f 5',
        shell=True,
        stdout=subprocess.PIPE,
    )
    stdout, _stderr = res.communicate()
    return set(stdout.decode().upper().splitlines())


def main() -> None:
    # Arbitrary project ID - we just need to build views in order to obtain raw table dependencies
    with local_project_id_override(GCP_PROJECT_STAGING):
        touched_raw_data_regions = get_touched_raw_data_regions()
        for region_code in touched_raw_data_regions:
            logging.info(
                "Generating raw data documentation for region [%s]", region_code
            )
            generate_raw_data_documentation_for_region(region_code)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
