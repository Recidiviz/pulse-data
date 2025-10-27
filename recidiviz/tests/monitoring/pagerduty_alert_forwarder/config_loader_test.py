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
"""Tests for config_loader module."""
import tempfile
import unittest
from pathlib import Path

from recidiviz.monitoring import pagerduty_alert_forwarder
from recidiviz.monitoring.pagerduty_alert_forwarder.config_loader import (
    AlertForwarderConfig,
    ConfigurationError,
)


class TestConfigLoader(unittest.TestCase):
    """Tests for configuration loader."""

    def test_valid_config(self) -> None:
        """Test loading a valid configuration."""
        config_dict = {
            "default": {
                "pagerduty_service": "default-service",
                "severity": "info",
            },
            "rules": [
                {
                    "name": "Test Rule",
                    "match": {"incident.policy_name": {"contains": "test"}},
                    "actions": {
                        "severity": "error",
                        "pagerduty_service": "test-service",
                    },
                }
            ],
        }

        config = AlertForwarderConfig(config_dict)
        self.assertEqual(config.default.pagerduty_service, "default-service")
        self.assertEqual(config.default.severity, "info")
        self.assertEqual(len(config.rules), 1)
        self.assertEqual(config.rules[0].name, "Test Rule")

    def test_missing_default(self) -> None:
        """Test configuration without default section."""
        config_dict = {
            "rules": [
                {
                    "name": "Test Rule",
                    "match": {"incident.policy_name": {"contains": "test"}},
                    "actions": {"severity": "error"},
                }
            ]
        }

        # Should not raise error - default is optional
        config = AlertForwarderConfig(config_dict)
        self.assertEqual(config.default.severity, "info")
        self.assertEqual(config.default.pagerduty_service, "")

    def test_invalid_severity(self) -> None:
        """Test configuration with invalid severity."""
        config_dict = {
            "default": {
                "severity": "invalid-severity",
            }
        }

        with self.assertRaises(ConfigurationError) as ctx:
            AlertForwarderConfig(config_dict)
        self.assertIn("severity", str(ctx.exception).lower())

    def test_rule_without_match(self) -> None:
        """Test rule without match criteria."""
        config_dict = {
            "rules": [
                {
                    "name": "Invalid Rule",
                    "actions": {"severity": "error"},
                }
            ]
        }

        with self.assertRaises(ConfigurationError) as ctx:
            AlertForwarderConfig(config_dict)
        self.assertIn("match", str(ctx.exception).lower())

    def test_rule_without_actions(self) -> None:
        """Test rule without actions."""
        config_dict = {
            "rules": [
                {
                    "name": "Invalid Rule",
                    "match": {"incident.policy_name": {"contains": "test"}},
                }
            ]
        }

        with self.assertRaises(ConfigurationError) as ctx:
            AlertForwarderConfig(config_dict)
        self.assertIn("actions", str(ctx.exception).lower())

    def test_empty_match_criteria(self) -> None:
        """Test rule with empty match criteria."""
        config_dict = {
            "rules": [
                {
                    "name": "Invalid Rule",
                    "match": {},
                    "actions": {"severity": "error"},
                }
            ]
        }

        with self.assertRaises(ConfigurationError) as ctx:
            AlertForwarderConfig(config_dict)
        self.assertIn("at least one criterion", str(ctx.exception).lower())

    def test_invalid_title_transform(self) -> None:
        """Test rule with invalid title transform."""
        config_dict = {
            "rules": [
                {
                    "name": "Invalid Transform",
                    "match": {"incident.policy_name": {"contains": "test"}},
                    "actions": {
                        "title_transform": {
                            "not": "a string"
                        },  # Must be string, not dict
                    },
                }
            ]
        }

        with self.assertRaises(ConfigurationError) as ctx:
            AlertForwarderConfig(config_dict)
        self.assertIn("title_transform", str(ctx.exception).lower())

    def test_load_from_file(self) -> None:
        """Test loading configuration from file."""
        config_yaml = """
default:
  pagerduty_service: default-service
  severity: info

rules:
  - name: Test Rule
    match:
      incident.policy_name:
        contains: "test"
    actions:
      severity: error
      pagerduty_service: test-service
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config_yaml)
            f.flush()
            temp_path = f.name

        try:
            config = AlertForwarderConfig.from_file(temp_path)
            self.assertEqual(config.default.pagerduty_service, "default-service")
            self.assertEqual(len(config.rules), 1)
        finally:
            Path(temp_path).unlink()

    def test_load_from_nonexistent_file(self) -> None:
        """Test loading from non-existent file."""
        with self.assertRaises(ConfigurationError) as ctx:
            AlertForwarderConfig.from_file("/nonexistent/path/config.yaml")
        self.assertIn("not found", str(ctx.exception).lower())

    def test_invalid_yaml(self) -> None:
        """Test loading invalid YAML."""
        invalid_yaml = "invalid: yaml: content: [unclosed"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_yaml)
            f.flush()
            temp_path = f.name

        try:
            with self.assertRaises(ConfigurationError) as ctx:
                AlertForwarderConfig.from_file(temp_path)
            self.assertIn("yaml", str(ctx.exception).lower())
        finally:
            Path(temp_path).unlink()

    def test_load_production_config(self) -> None:
        """Test loading the actual production config file."""
        # Get path to production config file using the package
        package_dir = Path(pagerduty_alert_forwarder.__file__).parent
        config_path = package_dir / "config.yaml"

        # Should load without errors
        config = AlertForwarderConfig.from_file(str(config_path))

        # Basic sanity checks
        self.assertIsNotNone(config.default)
        self.assertIsInstance(config.rules, list)
