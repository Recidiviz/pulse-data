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
"""Utility functions shared by classes modeling LookML files"""

import os

import recidiviz
from recidiviz.utils.string import StrictStringFormatter

FILE_TEMPLATE = """# This file was automatically generated using a pulse-data script.
# To regenerate, see `{script_repo_relative_path}`.

{file_body}
"""


def write_lookml_file(
    *, output_directory: str, file_name: str, source_script_path: str, file_body: str
) -> None:
    """
    Writes a LookML file into the specified output directory with a
    header indicating the date and script source of the auto-generated file.
    The input file name must include the .lkml extension, such as .view.lkml
    """
    script_repo_relative_path = os.path.join(
        "recidiviz",
        os.path.relpath(source_script_path, os.path.dirname(recidiviz.__file__)),
    )
    # if directory doesn't already exist, create
    os.makedirs(output_directory, exist_ok=True)

    with open(
        os.path.join(output_directory, file_name),
        "w",
        encoding="UTF-8",
    ) as file:
        file_text = StrictStringFormatter().format(
            FILE_TEMPLATE,
            script_repo_relative_path=script_repo_relative_path,
            file_body=file_body,
        )

        file.write(file_text)
