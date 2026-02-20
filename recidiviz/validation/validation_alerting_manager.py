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
# =========================c====================================================
"""Manager classes for opening & closing validation-related github tickets and alerting in Slack."""
import logging
import random
from functools import lru_cache
from typing import Iterator

import attrs
import requests
from github.Issue import Issue as GithubIssue
from github.Repository import Repository as GitHubRepository

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCPEnvironment, get_admin_panel_base_url
from recidiviz.utils.github import format_region_specific_ticket_title
from recidiviz.utils.secrets import get_secret
from recidiviz.validation.validation_alerting_config import (
    VALIDATION_ALERTING_CONFIG_PATH,
    ValidationAlertingConfig,
)
from recidiviz.validation.validation_models import (
    DataValidationJobResult,
    ValidationResultStatus,
)

DEFAULT_GITHUB_LABELS = ["Validation", "Team: State Pod"]
PASSING_VALIDATION_STATUSES = {
    ValidationResultStatus.FAIL_SOFT,
    ValidationResultStatus.SUCCESS,
}

# Fun emojis for Slack validation failure messages
ALERT_EMOJIS = [
    # Animals
    ":unicorn_face:",
    ":octopus:",
    ":squid:",
    ":t-rex:",
    ":sauropod:",
    ":chipmunk:",
    ":hedgehog:",
    ":flamingo:",
    ":parrot:",
    ":frog:",
    ":crab:",
    ":bee:",
    ":butterfly:",
    ":penguin:",
    ":seal:",
    # Food
    ":taco:",
    ":pizza:",
    ":doughnut:",
    ":cupcake:",
    ":avocado:",
    ":popcorn:",
    ":pretzel:",
    ":cheese:",
    ":cookie:",
    # Nature/Space
    ":rainbow:",
    ":sparkles:",
    ":cherry_blossom:",
    ":sunflower:",
    ":cactus:",
    ":mushroom:",
    ":crescent_moon:",
    ":star:",
    ":dizzy:",
    ":comet:",
]

_CONFIG_PATH = VALIDATION_ALERTING_CONFIG_PATH


@attrs.define
class ValidationFailureGithubIssue:
    """Represents a newly created GitHub issue for a validation failure."""

    issue_url: str
    validation_name: str
    env: GCPEnvironment


@lru_cache(maxsize=1)
def _load_alerting_config() -> ValidationAlertingConfig:
    """Loads and caches the typed validation alerting configuration."""
    return ValidationAlertingConfig.from_yaml(_CONFIG_PATH)


def post_issues_to_slack(
    created_issues: list[ValidationFailureGithubIssue],
    state_code: StateCode,
) -> None:
    """Posts a message to Slack with all newly created validation failure GitHub issues."""
    if not created_issues:
        return

    slack_token = get_secret("slack_bot_token")
    if not slack_token:
        logging.error(
            "Couldn't find `slack_bot_token`; "
            "skipping Slack notification for new validation issues"
        )
        return

    config = _load_alerting_config()
    slack_channel_id = config.get_slack_channel_for_state(state_code)

    owner_user_ids = config.get_slack_assignees_for_state(state_code)
    if owner_user_ids:
        owner_mentions = " ".join(f"<@{uid}>" for uid in owner_user_ids)
        owner_mention = f"cc {owner_mentions}\n"
    else:
        owner_mention = ""

    issue_count = len(created_issues)
    issue_word = "issue" if issue_count == 1 else "issues"

    # Sort issues so production comes first
    sorted_issues = sorted(
        created_issues,
        key=lambda x: (0 if x.env == GCPEnvironment.PRODUCTION else 1, x.env.value),
    )

    issue_lines = "\n".join(
        f"• [{issue.env.value}] `{issue.validation_name}`: {issue.issue_url}"
        for issue in sorted_issues
    )

    emoji = random.choice(ALERT_EMOJIS)
    text = (
        f"{emoji} *New Validation Failures for {state_code.value}* {emoji}\n"
        f"{owner_mention}"
        f"{issue_count} new GitHub {issue_word} created for hard validation failures:\n"
        f"{issue_lines}"
    )

    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            json={"text": text, "channel": slack_channel_id},
            headers={"Authorization": "Bearer " + slack_token},
            timeout=60,
        )
        logging.info("Response from Slack API call: %s", response)
    except Exception as e:
        logging.exception("Error when calling Slack API: %s", str(e))


@lru_cache
def github_labels_for_region(state_code: StateCode) -> list[str]:
    return [*DEFAULT_GITHUB_LABELS, f"Region: {state_code.value}"]


def _get_default_github_assignees_for_state(state_code: StateCode) -> list[str] | None:
    """Returns the default GitHub assignees for the given state from the YAML config."""
    return _load_alerting_config().get_github_assignees_for_state(state_code)


@attrs.define
class ValidationGithubTicketManager:
    """Manager class responsible for opening and closing validation failure tickets in
    Github.
    """

    client: GitHubRepository
    state_code: StateCode
    env: GCPEnvironment
    validation_result: DataValidationJobResult

    @classmethod
    def from_validation_result(
        cls,
        region_manager: "ValidationGithubTicketRegionManager",
        validation_result: DataValidationJobResult,
    ) -> "ValidationGithubTicketManager":
        return cls(
            client=region_manager.client,
            state_code=region_manager.state_code,
            env=region_manager.env,
            validation_result=validation_result,
        )

    @property
    def validation_name(self) -> str:
        return self.validation_result.validation_job.validation.validation_name

    @property
    def github_labels(self) -> list[str]:
        return github_labels_for_region(self.state_code)

    def _get_issue_title(self) -> str:
        """n.b. if you change this function, this will cause every currently failing
        validation to open new tickets!
        """
        return format_region_specific_ticket_title(
            region_code=self.state_code.value,
            environment=self.env.value,
            title=f"`{self.validation_name}`",
        )

    def _admin_panel_detail_for_validation(self) -> str:
        return f"{get_admin_panel_base_url()}/admin/validation_metadata/status/details/{self.validation_name}?stateCode={self.state_code.value}"

    def _build_closing_comment(self) -> str:
        return f"Found validation result for `{self.validation_name}` with the status of [{self.validation_result.validation_result_status.value}]."

    def _build_issue_body(self) -> str:
        return f"""Automated data validation found a hard failure for `{self.validation_name}` in {self.env.value} environment.
Admin Panel link: {self._admin_panel_detail_for_validation()}
Failure details: {self.validation_result.result_details.failure_description()}
Description: {self.validation_result.validation_job.validation.view_builder.description}
"""

    def handle_result(
        self, *, existing_issues: dict[str, GithubIssue]
    ) -> ValidationFailureGithubIssue | None:
        """If an open github issue match is found in |existing_issues| and this class'
        validation_result is not in a FAIL_HARD state, that issue will be closed. If no
        github issue match is found in |existing_issues|, a new issue will be opened if
        this class' validation_result is in a FAIL_HARD state; otherwise, nothing will
        be done.

        Returns a ValidationFailureGithubIssue if a new issue was created,
        otherwise None.
        """
        open_github_issue = existing_issues.get(self._get_issue_title())

        # case 1: there's no existing ticket for hard failure; let's open one!
        if (
            self.validation_result.validation_result_status
            == ValidationResultStatus.FAIL_HARD
            and not open_github_issue
        ):
            logging.info(
                "[%s] Opening new ticket for [%s]: no open ticket and hard failing",
                self.state_code.value,
                self.validation_name,
            )
            assignees = _get_default_github_assignees_for_state(self.state_code)
            issue = self.client.create_issue(
                title=self._get_issue_title(),
                body=self._build_issue_body(),
                labels=self.github_labels,
                assignees=assignees or [],
            )
            return ValidationFailureGithubIssue(
                issue_url=issue.html_url,
                validation_name=self.validation_name,
                env=self.env,
            )
        # case 2: we have an open issue for a validation that is no longer hard failing;
        #         we should close it
        if (
            self.validation_result.validation_result_status
            in PASSING_VALIDATION_STATUSES
            and open_github_issue
        ):
            logging.info(
                "[%s] Closing existing ticket for no-longer failing [%s]: open ticket and status of [%s]",
                self.state_code.value,
                self.validation_name,
                self.validation_result.validation_result_status.value,
            )
            open_github_issue.create_comment(self._build_closing_comment())
            open_github_issue.edit(state="closed", state_reason="completed")
            return None

        # case 3: either we (1) have an existing ticket AND we are hard still failing or
        #         (2) we don't have an existing ticket and are in a psuedo-success state;
        #         in both cases we don't need to do anything
        logging.info(
            "[%s] No action to take for [%s]: validation status of [%s] and [%s]",
            self.state_code.value,
            self.validation_name,
            self.validation_result.validation_result_status,
            "An open ticket" if open_github_issue else "No open ticket",
        )
        return None


@attrs.define
class ValidationGithubTicketRegionManager:
    """Manager class responsible for opening and closing validation tickets for a whole
    region.
    """

    client: GitHubRepository
    state_code: StateCode
    env: GCPEnvironment

    @property
    def github_labels(self) -> list[str]:
        return github_labels_for_region(self.state_code)

    def handle_results(self, *, results: Iterator[DataValidationJobResult]) -> None:
        logging.info("[%s] Fetching open validation issues...", self.state_code.value)
        open_issue_titles_for_region = {
            issue.title: issue
            for issue in self.client.get_issues(state="open", labels=self.github_labels)
        }

        created_issues: list[ValidationFailureGithubIssue] = []
        for validation_result in results:
            result = ValidationGithubTicketManager.from_validation_result(
                self, validation_result
            ).handle_result(existing_issues=open_issue_titles_for_region)
            if result is not None:
                created_issues.append(result)

        post_issues_to_slack(created_issues=created_issues, state_code=self.state_code)
