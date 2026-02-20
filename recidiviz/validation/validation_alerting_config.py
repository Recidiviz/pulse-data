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
"""Typed configuration classes for validation alerting."""

import os

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.yaml_dict import YAMLDict

VALIDATION_ALERTING_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "validation_alerting_config.yaml"
)


@attr.define(frozen=True)
class ValidationAlertingStateConfig:
    """Per-state configuration for validation alerting."""

    state_code: StateCode

    # Github handles for default assigneees for all validation Github
    # issues for this state.
    default_github_assignees: list[str]

    # Slack ID for channel where we will post about new issues in this state.
    slack_channel_id: str

    # Slack IDs for all people who should be @mentioned about this state's
    # issues in Slack
    default_slack_assignees: list[str]


@attr.define(frozen=True)
class ValidationAlertingConfig:
    """Top-level configuration for validation alerting, parsed from YAML."""

    default_slack_channel_id: str
    state_config: dict[StateCode, ValidationAlertingStateConfig]

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "ValidationAlertingConfig":
        """Parses the validation alerting config YAML file into a typed config object."""
        file_contents = YAMLDict.from_path(yaml_path)

        # Parse lookup maps
        slack_channels_dict = file_contents.pop_dict("slack_channels")
        slack_channels: dict[str, str] = {
            name: slack_channels_dict.pop(name, str)
            for name in slack_channels_dict.keys()
        }

        slack_people_dict = file_contents.pop_dict("slack_people")
        slack_people: dict[str, str] = {
            name: slack_people_dict.pop(name, str) for name in slack_people_dict.keys()
        }

        # Resolve default channel name to ID
        default_channel_name = file_contents.pop("default_slack_channel", str)
        if default_channel_name not in slack_channels:
            raise ValueError(
                f"default_slack_channel '{default_channel_name}' not found in "
                f"slack_channels map"
            )
        default_slack_channel_id = slack_channels[default_channel_name]

        # Parse per-state config, resolving names via lookup maps
        state_config: dict[StateCode, ValidationAlertingStateConfig] = {}
        state_config_dict = file_contents.pop_dict("state_config")
        for state_code_str in state_config_dict.keys():
            state_code = StateCode(state_code_str)
            state_dict = state_config_dict.pop_dict(state_code_str)

            channel_name = state_dict.pop("slack_channel", str)
            if channel_name not in slack_channels:
                raise ValueError(
                    f"State {state_code_str} references unknown slack_channel "
                    f"'{channel_name}'"
                )

            assignee_usernames = state_dict.pop_list("default_assignees", str)

            slack_assignees = []
            for username in assignee_usernames:
                if username not in slack_people:
                    raise ValueError(
                        f"State {state_code_str} references unknown assignee "
                        f"'{username}' not in slack_people map"
                    )
                slack_assignees.append(slack_people[username])

            state_config[state_code] = ValidationAlertingStateConfig(
                state_code=state_code,
                slack_channel_id=slack_channels[channel_name],
                default_github_assignees=assignee_usernames,
                default_slack_assignees=slack_assignees,
            )
            if len(state_dict) != 0:
                raise ValueError(
                    f"Unexpected keys in state config for {state_code_str}: "
                    f"{state_dict.keys()}"
                )

        if len(file_contents) != 0:
            raise ValueError(
                f"Unexpected top-level keys in config: {file_contents.keys()}"
            )

        return cls(
            default_slack_channel_id=default_slack_channel_id,
            state_config=state_config,
        )

    def get_slack_channel_for_state(self, state_code: StateCode) -> str:
        """Returns the Slack channel ID for the given state, or the default if not configured."""
        state_cfg = self.state_config.get(state_code)
        if state_cfg and state_cfg.slack_channel_id:
            return state_cfg.slack_channel_id
        return self.default_slack_channel_id

    def get_github_assignees_for_state(self, state_code: StateCode) -> list[str] | None:
        """Returns the default GitHub assignees for the given state, or None if not configured."""
        state_cfg = self.state_config.get(state_code)
        if state_cfg:
            return state_cfg.default_github_assignees
        return None

    def get_slack_assignees_for_state(self, state_code: StateCode) -> list[str] | None:
        """Returns the default Slack user IDs to mention for the given state, or None if not configured."""
        state_cfg = self.state_config.get(state_code)
        if state_cfg:
            return state_cfg.default_slack_assignees
        return None
