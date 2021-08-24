# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for documentation generation."""
import os


def persist_file_contents(documentation: str, markdown_path: str) -> bool:
    """Persists contents to the provided path. Returns whether contents at that path
    changed."""
    prior_documentation = None
    if os.path.exists(markdown_path):
        with open(markdown_path, "r", encoding="utf-8") as md_file:
            prior_documentation = md_file.read()

    if prior_documentation != documentation:
        with open(markdown_path, "w", encoding="utf-8") as md_file:
            md_file.write(documentation)
            return True
    return False
