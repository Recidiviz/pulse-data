# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Shared utilities for git-based validation scripts."""
import logging
import subprocess
from typing import FrozenSet, List


def get_modified_files(commit_range: str) -> FrozenSet[str]:
    """Returns a set of all files that have been modified in the given commit range.

    Args:
        commit_range: Git commit range (e.g., "main...HEAD", "abc123..def456")

    Returns:
        Set of file paths that were modified
    """
    git = subprocess.run(
        ["git", "diff", "--name-only", commit_range],
        stdout=subprocess.PIPE,
        check=True,
    )  # nosec B607 B603
    return frozenset(git.stdout.decode().splitlines())


def get_file_content_at_commit(file_path: str, commit: str) -> str:
    """Returns the content of a file at a specific commit.

    Args:
        file_path: Path to the file relative to repo root
        commit: Git commit reference (e.g., "HEAD", "main", "abc123")

    Returns:
        File content as string, empty string if file doesn't exist at that commit
    """
    try:
        git = subprocess.run(
            ["git", "show", f"{commit}:{file_path}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )  # nosec B607 B603
        return git.stdout.decode()
    except subprocess.CalledProcessError:
        # File might not exist at this commit
        return ""


def get_commits_with_pattern(commit_range: str, grep_pattern: str) -> List[str]:
    """Returns commit messages that match a grep pattern.

    Args:
        commit_range: Git commit range to search
        grep_pattern: Regex pattern to search for

    Returns:
        List of matching commit messages
    """
    git = subprocess.run(
        [
            "git",
            "log",
            "--format=%h %B",
            f"--grep={grep_pattern}",
            commit_range,
        ],
        stdout=subprocess.PIPE,
        check=True,
    )  # nosec B607 B603

    if git.returncode != 0:
        logging.error("git log failed")
        raise RuntimeError(f"git log command failed: {git.stderr.decode()}")

    return git.stdout.decode().splitlines()


def parse_commit_range(commit_range: str) -> tuple[str, str]:
    """Parses a commit range into base and head commits.

    Args:
        commit_range: Git commit range (e.g., "main...HEAD", "abc123")

    Returns:
        Tuple of (base_commit, head_commit)
    """
    range_parts = commit_range.split("...")
    if len(range_parts) == 2:
        return range_parts[0], range_parts[1]

    # Handle single commit
    return f"{commit_range}^", commit_range
