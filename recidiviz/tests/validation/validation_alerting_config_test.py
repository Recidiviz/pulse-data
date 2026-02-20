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
"""Tests for ValidationAlertingConfig."""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.validation.validation_alerting_config import (
    VALIDATION_ALERTING_CONFIG_PATH,
    ValidationAlertingConfig,
)


class ValidationAlertingConfigTest(TestCase):
    """Tests for ValidationAlertingConfig parsing and accessor methods."""

    def test_yaml_config_parses(self) -> None:
        config = ValidationAlertingConfig.from_yaml(VALIDATION_ALERTING_CONFIG_PATH)
        self.assertTrue(config.default_slack_channel_id)
        self.assertTrue(len(config.state_config) > 0)

    def test_get_slack_channel_for_configured_state(self) -> None:
        config = ValidationAlertingConfig.from_yaml(VALIDATION_ALERTING_CONFIG_PATH)
        # Pick a state that's in the config
        configured_state = next(iter(config.state_config))
        channel = config.get_slack_channel_for_state(configured_state)
        self.assertTrue(channel)
        self.assertEqual(
            channel,
            config.state_config[configured_state].slack_channel_id,
        )

    def test_get_slack_channel_falls_back_to_default(self) -> None:
        config = ValidationAlertingConfig.from_yaml(VALIDATION_ALERTING_CONFIG_PATH)
        # US_WW is a fake state not in any config
        channel = config.get_slack_channel_for_state(StateCode.US_WW)
        self.assertEqual(channel, config.default_slack_channel_id)

    def test_get_github_assignees_for_configured_state(self) -> None:
        config = ValidationAlertingConfig.from_yaml(VALIDATION_ALERTING_CONFIG_PATH)
        configured_state = next(iter(config.state_config))
        assignees = config.get_github_assignees_for_state(configured_state)
        self.assertEqual(
            assignees,
            config.state_config[configured_state].default_github_assignees,
        )

    def test_get_github_assignees_for_unconfigured_state(self) -> None:
        config = ValidationAlertingConfig.from_yaml(VALIDATION_ALERTING_CONFIG_PATH)
        assignees = config.get_github_assignees_for_state(StateCode.US_WW)
        self.assertIsNone(assignees)

    def test_get_slack_assignees_for_configured_state(self) -> None:
        config = ValidationAlertingConfig.from_yaml(VALIDATION_ALERTING_CONFIG_PATH)
        configured_state = next(iter(config.state_config))
        assignees = config.get_slack_assignees_for_state(configured_state)
        self.assertEqual(
            assignees,
            config.state_config[configured_state].default_slack_assignees,
        )

    def test_get_slack_assignees_for_unconfigured_state(self) -> None:
        config = ValidationAlertingConfig.from_yaml(VALIDATION_ALERTING_CONFIG_PATH)
        assignees = config.get_slack_assignees_for_state(StateCode.US_WW)
        self.assertIsNone(assignees)
