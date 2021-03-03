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
from shutil import rmtree, copytree, ignore_patterns
from typing import Dict

from recidiviz.ingest.direct import regions as regions_module
from recidiviz.ingest.direct import templates as ingest_templates_module
from recidiviz.persistence.entity_matching import (
    templates as persistence_templates_module,
)
from recidiviz.persistence.entity_matching import state as state_module
from recidiviz.tests.ingest.direct import regions as regions_tests_module
from recidiviz.tests.ingest.direct import templates as test_templates_module

_PLACEHOLDER = "TO" + "DO"


class DirectIngestFilesGenerator:
    """A class for generating necessary directories and files for direct ingest."""

    def __init__(self, region_code: str):
        self.region_code: str = region_code
        self.current_year = datetime.today().year

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
            os.path.dirname(regions_module.__file__), self.region_code
        )
        new_region_persistence_dir_path = os.path.join(
            os.path.dirname(state_module.__file__), self.region_code
        )
        new_region_tests_dir_path = os.path.join(
            os.path.dirname(regions_tests_module.__file__), self.region_code
        )

        dirs_to_create = [
            new_region_dir_path,
            new_region_persistence_dir_path,
            new_region_tests_dir_path,
        ]
        existing_dirs = [d for d in dirs_to_create if os.path.exists(d)]
        if existing_dirs:
            raise FileExistsError(
                "The following already exists:\n{}".format("\n".join(existing_dirs))
            )

        template_to_dest: Dict[str, str] = {
            os.path.dirname(ingest_templates_module.__file__): new_region_dir_path,
            os.path.dirname(
                persistence_templates_module.__file__
            ): new_region_persistence_dir_path,
            os.path.dirname(test_templates_module.__file__): new_region_tests_dir_path,
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
        with open(file_path) as f:
            file_contents = f.readlines()

        with open(file_path, "w") as updated_f:
            for line in file_contents:
                if re.search("US_XX", line):
                    line = re.sub("US_XX", self.region_code.upper(), line)
                if re.search("us_xx", line):
                    line = re.sub("us_xx", self.region_code, line)
                if re.search("UsXx", line):
                    line = re.sub(
                        "UsXx",
                        # Capitalize the first letter of the region and any clause following an underscore
                        re.sub(
                            r"^[a-z]|[_][a-z]",
                            lambda m: m.group().upper(),
                            self.region_code.lower(),
                        ),
                        line,
                    ).replace("_", "")
                if re.search("unknown", line):
                    line = line.rstrip() + f"  # {_PLACEHOLDER}\n"
                if re.search(r"Copyright \(C\) 2021 Recidiviz", line):
                    line = re.sub("2021", f"{self.current_year}", line)
                updated_f.write(line)
