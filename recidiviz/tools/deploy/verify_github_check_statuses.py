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
"""Checks if all Github Actions checks that ran for a given commit (on main or a
release/ branch) passed. Exits with a non-zero status if there are failing checks or the
user confirms that it's ok to proceed.

Usage:
python -m recidiviz.tools.deploy.verify_github_check_statuses \
  --project_id [project-id] \
  --commit_ref COMMIT_REF \
  --prompt

  
python -m recidiviz.tools.deploy.verify_github_check_statuses \
  --project_id recidiviz-staging \
  --commit_ref 455d3130d2f9627786bfdc64ee0273d5b2c11518 \
  --prompt

"""
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
    prompt_for_step_or_skip,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.github import RECIDIVIZ_DATA_REPO, github_helperbot_client
from recidiviz.utils.metadata import local_project_id_override

_REQUIRED_CHECKS_BY_PROJECT = {
    GCP_PROJECT_STAGING: {
        "BigQuery Source Table Validation (staging)",
    },
    GCP_PROJECT_PRODUCTION: {
        "BigQuery Source Table Validation (production)",
    },
}


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        type=str,
        help="The project we will be deploying this commit to",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

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


def _get_conclusion(check_run: CheckRun) -> str:
    # In progress checks do not have a conclusion set, but do have a status
    return check_run.conclusion or check_run.status


def print_check_statuses(check_runs: list[CheckRun]) -> None:
    indent_str = f"{ANSI.BLUE}>{ANSI.ENDC}"

    check_colors = {
        "success": ANSI.GREEN,
        "failure": ANSI.FAIL,
    }
    for check_run in sorted(check_runs, key=lambda c: c.name):
        conclusion = _get_conclusion(check_run)
        conclusion_str = color_text(
            check_colors.get(conclusion, ANSI.WARNING),
            conclusion.upper(),
        )
        print(f"{indent_str} {conclusion_str} {check_run.name}")
        if conclusion != "success" and check_run.html_url:
            print(f"- {check_run.html_url}")


def verify_github_check_statuses(
    project_id: str,
    commit_ref: str,
    success_states: Iterable[str],
) -> tuple[bool, bool]:
    """Verifies that the most recent CI check runs for the given commit all pass.
    Prints out the status of all GitHub checks for the given commit. Returns a tuple of
    booleans, the first one is true if all *required* checks have passed, the second one
    is true if *all* checks have passed.
    """
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
        # Pick the most recent run
        sorted(
            check_runs,
            key=lambda c: (
                c.started_at
                if c.started_at
                else datetime.datetime(year=datetime.MINYEAR, month=1, day=1)
            ),
        )[-1]
        for check_runs in check_runs_by_name.values()
    ]

    print(f"Found {len(check_runs)} status checks:")
    print_check_statuses(check_runs)

    most_recent_check_runs_by_name = {
        check_run.name: check_run for check_run in check_runs
    }

    required_check_names = _REQUIRED_CHECKS_BY_PROJECT[project_id]
    if missing_checks := required_check_names - set(most_recent_check_runs_by_name):
        raise ValueError(
            f"Found required Github checks which were not run for commit {commit_ref}: "
            f"{missing_checks}. Did the names of these checks change or did they get "
            f"removed?"
        )

    required_check_runs = [
        most_recent_check_runs_by_name[check_name]
        for check_name in sorted(required_check_names)
    ]

    failing_required_check_runs = [
        check_run
        for check_run in required_check_runs
        if _get_conclusion(check_run) not in success_states
    ]

    if failing_required_check_runs:
        print(
            f"Found {len(failing_required_check_runs)} incomplete/failing REQUIRED "
            f"check(s):"
        )
        print_check_statuses(failing_required_check_runs)
        print(
            "You must wait for these checks to complete and/or resolve the issues "
            "before proceeding with the deploy."
        )

    return not bool(failing_required_check_runs), all(
        _get_conclusion(check_run) in success_states for check_run in check_runs
    )


def main(args: argparse.Namespace) -> None:
    with local_project_id_override(args.project_id):
        while True:
            # TODO(#53670): Wait for required checks to complete (pass or fail) before
            # allowing user to proceed.
            _, all_checks_passed = verify_github_check_statuses(
                project_id=args.project_id,
                commit_ref=args.commit_ref,
                success_states={"success", "skipped"},
            )
            if all_checks_passed:
                return

            if not args.prompt:
                sys.exit(1)

            if prompt_for_step_or_skip(
                "Found checks that were not in success or skipped status. "
                "Are you sure you want to proceed with the deploy?"
            ):
                break


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main(args=parse_arguments(sys.argv[1:]))
