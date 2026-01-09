# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Script to check for open PRs against the branch we're cutting an RC from. The check is a noop if base_branch is main.

Example usage:

uv run python -m recidiviz.tools.deploy.check_for_prs --base_branch [BASE_BRANCH] --github_token [GITHUB_TOKEN]
"""
import argparse
import logging
import sys
from typing import List, Tuple

import requests

REPO_URL = "https://api.github.com/repos/Recidiviz/pulse-data"


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--base_branch",
        dest="base_branch",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--github_token",
        dest="github_token",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


def fetch_prs(base_branch: str, github_token: str) -> List[dict]:
    """
    Checks for open prs against base_branch
    """
    url = f"{REPO_URL}/pulls"
    payload = {
        "base": base_branch,
        "state": "open",
    }
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "authorization": f"Bearer {github_token}",
        "Content-Type": "application/json",
    }

    return requests.request(
        "GET", url, params=payload, headers=headers, timeout=180
    ).json()


def check_prs(base_branch: str, github_token: str) -> None:
    prs = fetch_prs(base_branch, github_token)
    if len(prs) > 0:
        print(
            "There are open PRs against {base_branch} that may need to be in this RC:"
        )
        for pr in prs:
            print(pr.get("title"), pr.get("html_url"))
        decision = input("Deploy anyway? (y/n)\n")
        if decision.lower() != "y":
            sys.exit(1)


if __name__ == "__main__":
    args, _ = parse_arguments(sys.argv)

    try:
        if args.base_branch != "main":
            check_prs(args.base_branch, args.github_token)
    except Exception as e:
        logging.error(
            "Failed to check for open PRs [%s].",
            e,
            exc_info=True,
        )
