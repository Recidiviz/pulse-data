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
"""Prompts if all Github Actions checks that ran for a release branch did not pass"""
import argparse
import datetime
import logging
import sys
from collections import defaultdict
from typing import Iterable, List

from github.CheckRun import CheckRun

from recidiviz.tools.utils.script_helpers import (
    ANSI,
    color_text,
    prompt_for_confirmation,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.github import RECIDIVIZ_DATA_REPO, github_helperbot_client
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--commit_ref",
        type=str,
        help="The commit reference to verify",
        required=True,
    )

    parser.add_argument(
        "--prompt",
        action="store_true",
        help="If set, allows deployer to override check status and continue with deploy",
    )

    return parser.parse_args(argv)


def print_check_statuses(check_runs: list[CheckRun]) -> None:
    indent_str = f"{ANSI.BLUE}>{ANSI.ENDC}"

    conclusion_colors = {
        "success": ANSI.GREEN,
        "failure": ANSI.FAIL,
    }
    print(f"Found {len(check_runs)} status checks:")

    for check_run in sorted(check_runs, key=lambda c: c.name):
        conclusion = check_run.conclusion
        conclusion_str = color_text(
            conclusion_colors.get(conclusion, ANSI.WARNING),
            conclusion.upper(),
        )
        print(f"{indent_str} {conclusion_str} {check_run.name}")
        if conclusion != "success" and check_run.html_url:
            print(f"- {check_run.html_url}")


def verify_github_check_statuses(
    commit_ref: str,
    success_states: Iterable[str],
) -> bool:
    github_client = github_helperbot_client()
    check_runs = list(
        github_client.get_repo(RECIDIVIZ_DATA_REPO)
        .get_commit(commit_ref)
        .get_check_runs()
    )
    check_runs_by_name = defaultdict(list)
    for check_run in check_runs:
        check_runs_by_name[check_run.name].append(check_run)

    check_runs = [
        sorted(
            check_runs,
            key=lambda c: c.started_at
            if c.started_at
            else datetime.datetime(year=datetime.MAXYEAR, month=1, day=1),
        )[0]
        for check_runs in check_runs_by_name.values()
    ]

    print_check_statuses(check_runs)

    return all(check_run.conclusion in success_states for check_run in check_runs)


def main(args: argparse.Namespace) -> None:
    with local_project_id_override(GCP_PROJECT_STAGING):
        all_checks_passed = verify_github_check_statuses(
            commit_ref=args.commit_ref,
            success_states={"success", "skipped"},
        )

        if not all_checks_passed:
            if args.prompt:
                prompt_for_confirmation(
                    input_text="Found checks that were not in success or skipped status. "
                    "Are you sure you want to proceed with the deploy?",
                    exit_on_cancel=True,
                )
            else:
                sys.exit(1)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main(args=parse_arguments(sys.argv[1:]))
