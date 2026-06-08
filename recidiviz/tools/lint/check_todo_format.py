# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Checks that all TODOs in the given files use a valid reference format.

Usage:
    python -m recidiviz.tools.lint.check_todo_format file1.py file2.py ...

Exits 0 if all TODOs are valid, 1 if any invalid TODOs are found.
"""

import re
import sys

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.issue import UrlIssue
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue

TODO_RE = re.compile(r"[^A-Za-z]TODO")
VALID_TODO_RE = re.compile(
    rf"{GithubIssue.todo_regex()}|{LinearIssue.todo_regex()}|{UrlIssue.todo_regex()}"
)

EXCLUDED_PATH_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p)
    for p in [
        r"find-(closed|linked)-todos\.yml",
        r"reopen-closed-todo-issues\.yml",
        r"find_closing_issue_todos\.py",
        r"reopen_closed_todo_issues\.py",
        r"reopen_closed_todo_issues_test\.py",
        r"issue\.py",
        r"issue_test\.py",
        r"codebase_todos\.py",
        r"codebase_todos_test\.py",
        r"bandit-baseline\.json",
        r"recidiviz/tools/deploy/atmos/components/terraform/vendor",
        r"run_pylint\.sh",
        r"\.pylintrc",
        r"CLAUDE\.md",
        r"\.claude/skills/create_recidiviz_data_github_tasks/SKILL\.md",
        r"\.claude/skills/maintain_skill_files/SKILL\.md",
        r"/templates/",
        r"/nbautoexports/",
        r"/migrated_notebooks/",
        r"check_todo_format\.py",
        r"check_todo_format_test\.py",
    ]
]


def is_excluded(filepath: str) -> bool:
    return any(pattern.search(filepath) for pattern in EXCLUDED_PATH_PATTERNS)


def check_file(filepath: str) -> list[str]:
    """Returns a list of invalid TODO lines in the given file, formatted as
    'filepath:lineno:line_text'."""
    invalid_lines = []
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            for lineno, line in enumerate(f, start=1):
                if TODO_RE.search(line) and not VALID_TODO_RE.search(line):
                    invalid_lines.append(f"{filepath}:{lineno}:{line.rstrip()}")
    except (OSError, UnicodeDecodeError):
        pass
    return invalid_lines


def main(filepaths: list[str]) -> int:
    all_invalid: list[str] = []
    for filepath in filepaths:
        if is_excluded(filepath):
            continue
        all_invalid.extend(check_file(filepath))

    if all_invalid:
        print(
            "TODOs must be of format TODO(#000), TODO(organization/repo#000), "
            "TODO(OBT-000), or TODO(https://issues.com/000)"
        )
        for line in all_invalid:
            print(f"    {line}")
        return 1

    print("All TODOs match format")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
