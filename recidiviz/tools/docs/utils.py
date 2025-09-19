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
import difflib
import logging
import os
import re

import recidiviz
from recidiviz.repo.issue_references import TO_DO_REGEX

DOCS_ROOT_PATH = os.path.normpath(
    os.path.join(os.path.dirname(recidiviz.__file__), "..", "docs")
)
PLACEHOLDER_TO_DO_STRING = "TO" + "DO"
ISSUES_URL = "https://github.com/Recidiviz/pulse-data/issues/"


def hyperlink_todos(text: str) -> str:
    """Given any text, returns that text with todos linked."""

    def _link_todo(match_obj: re.Match) -> str:
        """Adds a markdown link to the found todo."""
        og_text = match_obj.group(0)
        digits = og_text[len(f"{PLACEHOLDER_TO_DO_STRING}(#") : -1]
        return f"[{og_text}]({ISSUES_URL+digits})"

    return re.sub(TO_DO_REGEX, _link_todo, text)


def persist_file_contents(
    documentation: str, markdown_path: str, log_diff: bool = False
) -> bool:
    """Persists contents to the provided path. Returns whether contents at that path
    changed."""
    prior_documentation = None
    if os.path.exists(markdown_path):
        with open(markdown_path, "r", encoding="utf-8") as md_file:
            prior_documentation = md_file.read()

    if prior_documentation != documentation:
        if log_diff and prior_documentation:
            logging.info(
                "\n".join(
                    difflib.ndiff(
                        prior_documentation.split("\n"),
                        documentation.split("\n"),
                    )
                )
            )
        path_dir, _ = os.path.split(markdown_path)
        os.makedirs(path_dir, exist_ok=True)
        with open(markdown_path, "w", encoding="utf-8") as md_file:
            md_file.write(documentation)
            return True
    return False
