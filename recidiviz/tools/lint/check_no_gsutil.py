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
"""Fails when a changed file contains a gsutil invocation.

Google removes gsutil from the Google Cloud CLI package after March 2027, after
which it only ships as a separate PyPI install. Cloud Build steps, GitHub Actions
runners, and laptops that install the SDK then lack the binary. Use gcloud storage,
or the wrappers in recidiviz.tools.gsutil_shell_helpers.

Usage:
    python -m recidiviz.tools.lint.check_no_gsutil file1.py file2.sh ...

Exits 0 if no invocation is found, 1 otherwise.

An invocation is the word gsutil in a command position followed by a subcommand,
in shell, YAML, a markdown code block, or a Python string: "gsutil cp a b",
"$(gsutil cat ...)", "| gsutil ls", "f'gsutil cp {x}'". A Python argv list such
as ["gsutil", "cp", ...] also counts.

These do not count, so the migration can still describe what the old tool did:

  * identifiers such as gsutil_ls and gsutil_shell_helpers, where an underscore
    follows the word rather than a subcommand
  * prose such as "gsutil is removed in 2027", where no subcommand follows
  * markdown inline code wrapped in backticks, and any line whose first non-blank
    character is "#"
  * a command quoted in prose with single quotes, such as a docstring that says
    the function wraps 'gsutil ls'. Black normalises this repo's code strings to
    double quotes, so a single quote marks prose here, not code.
  * the Claude allowlist entry "Bash(gsutil ls:*)", where "(" is not a command
    position
"""

import re
import sys

_SUBCOMMANDS = (
    "acl|autoclass|cat|compose|cors|cp|defacl|du|hash|help|hmac|iam|kms|label|"
    "lifecycle|ls|mb|mv|notification|rb|retention|rewrite|rm|rsync|setmeta|"
    "signurl|stat|ubla|version|versioning|web"
)

# A command position is the start of the line, or after whitespace, one of the
# shell separators ";", "|", "&", "$(", or a double quote that opens a string.
# A bare "(", a backtick, and a single quote are deliberately absent: they mark
# the Claude allowlist form, markdown inline code, and prose respectively.
_COMMAND_RE = re.compile(
    rf"""(?:^|[\s;|&"]|\$\(|f["'])\s*gsutil\s+(?:-[A-Za-z]+\s+)*(?:{_SUBCOMMANDS})\b"""
)
# An argv list may carry flags between the tool and the subcommand, as in
# ["gsutil", "-m", "cp", src, dst].
_ARGV_RE = re.compile(
    rf"""["']gsutil["'](?:\s*,\s*["']-[^"']*["'])*\s*,\s*["'](?:{_SUBCOMMANDS})["']"""
)
# Cut a trailing "  # ..." comment so a command named in a comment after code is
# not read as an invocation. "$#" and "${#...}" have no leading space, so they
# are untouched.
_TRAILING_COMMENT_RE = re.compile(r"\s#.*$")

EXCLUDED_PATH_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p)
    for p in [
        # Archived research notebooks that are never re-run.
        r"/migrated_notebooks/",
        r"/nbautoexports/",
        r"bandit-baseline\.json",
        r"check_no_gsutil\.py",
        r"check_no_gsutil_test\.py",
    ]
]


def is_excluded(filepath: str) -> bool:
    return any(pattern.search(filepath) for pattern in EXCLUDED_PATH_PATTERNS)


def line_has_invocation(line: str) -> bool:
    stripped = line.lstrip()
    if stripped.startswith("#"):
        return False
    code = _TRAILING_COMMENT_RE.sub("", line)
    return bool(_COMMAND_RE.search(code) or _ARGV_RE.search(code))


def check_file(filepath: str) -> list[str]:
    """Returns the gsutil invocations in the given file, formatted as
    'filepath:lineno:line_text'."""
    hits = []
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            for lineno, line in enumerate(f, start=1):
                if line_has_invocation(line):
                    hits.append(f"{filepath}:{lineno}:{line.rstrip()}")
    except (OSError, UnicodeDecodeError):
        pass
    return hits


def main(filepaths: list[str]) -> int:
    all_hits: list[str] = []
    for filepath in filepaths:
        if is_excluded(filepath):
            continue
        all_hits.extend(check_file(filepath))

    if all_hits:
        print(
            "gsutil is removed from the gcloud CLI package after March 2027. "
            "Use gcloud storage instead, or the wrappers in "
            "recidiviz/tools/gsutil_shell_helpers.py."
        )
        for line in all_hits:
            print(f"    {line}")
        return 1

    print("No gsutil invocations found")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
