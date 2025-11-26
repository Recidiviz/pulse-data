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
import time
from collections import defaultdict
from typing import List

import attr
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

_SUCCESS_STATES = {"success", "skipped"}

# Maximum time (in seconds) to wait for required checks to complete
_REQUIRED_CHECKS_TIMEOUT_SECONDS = 15 * 60  # 15 minutes


@attr.s(frozen=True, kw_only=True)
class GitHubCheckStatusResult:
    """Result of fetching GitHub check statuses for a commit."""

    commit_ref: str = attr.ib()
    all_check_runs: list[CheckRun] = attr.ib()
    required_check_names: set[str] = attr.ib()

    def __attrs_post_init__(self) -> None:
        # Validate that all required checks exist
        check_names = {check_run.name for check_run in self.all_check_runs}
        if missing_checks := self.required_check_names - check_names:
            raise ValueError(
                f"Found required Github checks which were not run for commit {self.commit_ref}: "
                f"{missing_checks}. Did the names of these checks change or did they get "
                f"removed?"
            )

    @property
    def required_check_runs(self) -> list[CheckRun]:
        """Returns the subset of check runs that are required."""
        return [
            check_run
            for check_run in self.all_check_runs
            if check_run.name in self.required_check_names
        ]

    @property
    def incomplete_required_check_runs(self) -> list[CheckRun]:
        """Required checks that are still running (in_progress)."""
        return [
            check_run
            for check_run in self.required_check_runs
            if not _is_check_completed(check_run)
        ]

    @property
    def failing_required_check_runs(self) -> list[CheckRun]:
        """Required checks that completed but failed."""
        return [
            check_run
            for check_run in self.required_check_runs
            if _is_check_completed(check_run) and not _check_passed(check_run)
        ]

    @property
    def all_checks_passed(self) -> bool:
        """True if all checks passed."""
        return all(
            _is_check_completed(check_run) and _check_passed(check_run)
            for check_run in self.all_check_runs
        )


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


def _is_check_completed(check_run: CheckRun) -> bool:
    """Returns True if the check has completed (regardless of pass/fail).

    Per GitHub API (https://docs.github.com/en/rest/checks/runs):
    status can be 'queued', 'in_progress', or 'completed'.
    Only when status='completed' will the conclusion field be set.
    """
    return check_run.status == "completed"


def _check_passed(check_run: CheckRun) -> bool:
    """Returns True if a COMPLETED check passed.

    Per GitHub API (https://docs.github.com/en/rest/checks/runs):
    conclusion is only set when status='completed' and can be:
    'success', 'failure', 'neutral', 'cancelled', 'skipped', 'timed_out', 'action_required'.
    """
    return check_run.conclusion in _SUCCESS_STATES


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


def get_github_check_statuses(
    project_id: str,
    commit_ref: str,
) -> GitHubCheckStatusResult:
    """Fetches GitHub check statuses for a commit and returns a structured result that
    provides info about the statuses.
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

    required_check_names = _REQUIRED_CHECKS_BY_PROJECT[project_id]

    return GitHubCheckStatusResult(
        commit_ref=commit_ref,
        all_check_runs=check_runs,
        required_check_names=required_check_names,
    )


def main(args: argparse.Namespace) -> None:
    """Main function to verify GitHub check statuses before deployment.

    Blocks deployment when required checks are still running (in_progress).
    Allows override with --prompt when required checks have completed but failed.
    Exits with error if required checks don't complete within timeout.
    """
    with local_project_id_override(args.project_id):
        start_time = time.time()
        while True:
            result = get_github_check_statuses(
                project_id=args.project_id,
                commit_ref=args.commit_ref,
            )

            print(f"Found {len(result.all_check_runs)} status checks:")
            print_check_statuses(result.all_check_runs)

            if result.all_checks_passed:
                return

            if result.incomplete_required_check_runs:
                if (time.time() - start_time) >= _REQUIRED_CHECKS_TIMEOUT_SECONDS:
                    print(
                        f"\n❌ Required checks have not completed after "
                        f"[{_REQUIRED_CHECKS_TIMEOUT_SECONDS // 60}] minutes. Exiting."
                    )
                    sys.exit(1)

                print(
                    f"\nFound {len(result.incomplete_required_check_runs)} "
                    f"INCOMPLETE REQUIRED check(s) (still running):"
                )
                print_check_statuses(result.incomplete_required_check_runs)
                print(
                    "\nRequired checks are still running. Waiting 30 seconds before "
                    "checking again..."
                )
                time.sleep(30)
                continue

            if result.failing_required_check_runs:
                print(
                    f"\nFound [{len(result.failing_required_check_runs)}] FAILED "
                    f"REQUIRED check(s):"
                )
                print_check_statuses(result.failing_required_check_runs)
                if not args.prompt:
                    sys.exit(1)
                if prompt_for_step_or_skip(
                    f"🚨 Found [{len(result.failing_required_check_runs)}] FAILING "
                    f"required checks. Are you ABSOLUTELY SURE you want to proceed "
                    "with the deploy?"
                ):
                    break
                continue

            # All required checks passed, some non-required checks failed / incomplete
            if not args.prompt:
                sys.exit(1)
            if prompt_for_step_or_skip(
                "⚠️ Some non-required checks have not passed. Are you sure you want to "
                "proceed with the deploy?"
            ):
                break


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main(args=parse_arguments(sys.argv[1:]))
