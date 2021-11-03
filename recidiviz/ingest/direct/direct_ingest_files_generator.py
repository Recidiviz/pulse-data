# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Functionality for generating direct ingest directories and files."""
import os
import re
from datetime import datetime
from shutil import copytree, ignore_patterns, rmtree
from typing import Dict

import recidiviz
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import regions as regions_module
from recidiviz.ingest.direct import templates as ingest_templates_module
from recidiviz.persistence.entity_matching import state as state_module
from recidiviz.persistence.entity_matching import (
    templates as persistence_templates_module,
)
from recidiviz.tests.ingest.direct import (
    direct_ingest_fixtures as regions_test_fixtures_module,
)
from recidiviz.tests.ingest.direct import (
    fixtures_templates as test_fixtures_templates_module,
)
from recidiviz.tests.ingest.direct import regions as regions_test_module
from recidiviz.tests.ingest.direct import templates as test_templates_module
from recidiviz.tests.ingest.direct.direct_ingest_util import PLACEHOLDER_TO_DO_STRING

DEFAULT_WORKING_DIR: str = os.path.dirname(recidiviz.__file__)
REGIONS_DIR_PATH = os.path.dirname(
    os.path.relpath(regions_module.__file__, start=DEFAULT_WORKING_DIR)
)
PERSISTENCE_DIR_PATH = os.path.dirname(
    os.path.relpath(state_module.__file__, start=DEFAULT_WORKING_DIR)
)
TESTS_DIR_PATH = os.path.dirname(
    os.path.relpath(regions_test_module.__file__, start=DEFAULT_WORKING_DIR)
)
TEST_FIXTURES_DIR_PATH = os.path.dirname(
    os.path.relpath(regions_test_fixtures_module.__file__, start=DEFAULT_WORKING_DIR)
)
DOCS_DIR_NAME = "docs"
DOCS_DIR_PATH = os.path.join(DEFAULT_WORKING_DIR, "..", DOCS_DIR_NAME)


class DirectIngestFilesGenerator:
    """A class for generating necessary directories and files for direct ingest."""

    def __init__(
        self,
        region_code: str,
        curr_directory: str = DEFAULT_WORKING_DIR,
        docs_directory: str = DOCS_DIR_PATH,
    ):
        self.region_code: str = region_code
        self.current_year: int = datetime.today().year
        self.curr_directory: str = curr_directory
        self.docs_directory: str = docs_directory

    def generate_all_new_dirs_and_files(self) -> None:
        """
        Generates all new directories and files necessary for the direct ingest of a region.
            - Copies a template directory into the new region's directory, and updates the contents.
            - To add a new directory required for ingest, add the desired destination path to the list dirs_to_create
              and create a new template_to_dest mapping.
            - If updating the direct ingest structure, the template directories should also be modified to
              fit the new requirements.
        """

        new_region_dir_path = os.path.join(
            self.curr_directory, REGIONS_DIR_PATH, self.region_code
        )
        new_region_persistence_dir_path = os.path.join(
            self.curr_directory, PERSISTENCE_DIR_PATH, self.region_code
        )
        new_region_tests_dir_path = os.path.join(
            self.curr_directory, TESTS_DIR_PATH, self.region_code
        )
        new_region_test_fixtures_dir_path = os.path.join(
            self.curr_directory, TEST_FIXTURES_DIR_PATH, self.region_code
        )
        new_region_docs_dir_path = os.path.join(
            self.docs_directory, "ingest", self.region_code
        )

        dirs_to_create = [
            new_region_dir_path,
            new_region_persistence_dir_path,
            new_region_tests_dir_path,
            new_region_test_fixtures_dir_path,
            new_region_docs_dir_path,
        ]
        existing_dirs = [d for d in dirs_to_create if os.path.exists(d)]
        if existing_dirs:
            existing_dirs_string = "\n".join(existing_dirs)
            raise FileExistsError(
                f"The following already exists:\n{existing_dirs_string}"
            )

        template_to_dest: Dict[str, str] = {
            os.path.dirname(ingest_templates_module.__file__): new_region_dir_path,
            os.path.dirname(
                persistence_templates_module.__file__
            ): new_region_persistence_dir_path,
            os.path.dirname(test_templates_module.__file__): new_region_tests_dir_path,
            os.path.dirname(
                test_fixtures_templates_module.__file__
            ): new_region_test_fixtures_dir_path,
            os.path.join(DOCS_DIR_PATH, "templates"): new_region_docs_dir_path,
        }

        try:
            # Copy the template directory into the new region's directory
            for template_dir, dest_dir in template_to_dest.items():
                copytree(
                    os.path.join(template_dir, "us_xx"),
                    dest_dir,
                    ignore=ignore_patterns("__pycache__"),
                )

            for d in dirs_to_create:
                for dir_path, _, files in os.walk(d):
                    for file in files:
                        self._update_file_contents(os.path.join(dir_path, file))
                        self._update_file_name(dir_path, file)
        except Exception:
            # Clean up
            for new_dir in dirs_to_create:
                if os.path.isdir(new_dir):
                    rmtree(new_dir)
            raise

    def _update_file_name(self, dir_path: str, file_name: str) -> None:
        """Update file names that use generic codes from templates to use the specified region code."""
        if re.search("us_xx", file_name):
            new_file_name = re.sub("us_xx", self.region_code, file_name)
            os.rename(
                os.path.join(dir_path, file_name), os.path.join(dir_path, new_file_name)
            )

    def _update_file_contents(self, file_path: str) -> None:
        """
        Update contents of files to use the region code, current year, and add placeholders.
            - Finds instances of generics (US_XX, us_xx, UsXx) and replaces them with the correctly formatted code
                - i.e. if self.region_code is us_tn, then US_XX --> US_TN, UsXx --> UsTn, us_xx --> us_tn.
            - Updates license year in all files to be the current year
            - Adds placeholders
        """
        with open(file_path, encoding="utf-8") as f:
            file_contents = f.readlines()

        with open(file_path, "w", encoding="utf-8") as updated_f:
            for line in file_contents:
                # Fix references to the templates directory
                if re.search(r"from recidiviz\.ingest\.direct import templates", line):
                    continue
                line = re.sub(
                    r"from recidiviz\.ingest\.direct\.templates",
                    "from recidiviz.ingest.direct.regions",
                    line,
                )
                line = re.sub(r"(, )?region_module_override=templates", "", line)

                # Replace all instances of template state code with real state code
                line = re.sub("US_XX", self.region_code.upper(), line)
                line = re.sub("us_xx", self.region_code, line)

                capital_case_state_code = "".join(
                    s.capitalize() for s in self.region_code.split("_")
                )
                line = re.sub("UsXx", capital_case_state_code, line)
                line = re.sub(
                    r"\[STATE\]",
                    StateCode(self.region_code.upper()).get_state().name,
                    line,
                )

                # Other clean-up
                if re.search("unknown", line):
                    line = line.rstrip() + f"  # {PLACEHOLDER_TO_DO_STRING}\n"
                if re.search(r"Copyright \(C\) 2021 Recidiviz", line):
                    line = re.sub("2021", f"{self.current_year}", line)

                updated_f.write(line)
