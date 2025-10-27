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
"""Tests for rule_engine module."""
import unittest

from recidiviz.monitoring.pagerduty_alert_forwarder.config_loader import (
    AlertForwarderConfig,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_alert import (
    PagerDutyAlert,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.rule_engine import RuleEngine


class TestRuleEngine(unittest.TestCase):
    """Tests for rule engine."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.sample_alert = {
            "incident": {
                "incident_id": "test-incident-123",
                "policy_name": "Database Connection Error",
                "resource_type_display_name": "cloud_sql_database",
                "resource_display_name": "production-db",
                "condition_name": "Connection failures > 10",
                "summary": "Database connection errors detected",
                "severity": "ERROR",
                "url": "https://console.cloud.google.com/...",
            }
        }

    def test_default_behavior(self) -> None:
        """Test that defaults are applied when no rules match."""
        config = AlertForwarderConfig(
            {
                "default": {
                    "pagerduty_service": "default-oncall",
                    "severity": "info",
                },
                "rules": [
                    {
                        "name": "Non-matching rule",
                        "match": {
                            "incident.policy_name": {"contains": "this-will-not-match"}
                        },
                        "actions": {"severity": "critical"},
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(result["severity"], "info")
        self.assertEqual(result["pagerduty_service"], "default-oncall")
        self.assertEqual(result["title"], "Database connection errors detected")

    def test_alert_name_contains_match(self) -> None:
        """Test matching by alert name contains."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default", "severity": "info"},
                "rules": [
                    {
                        "name": "Database alerts",
                        "match": {"incident.policy_name": {"contains": "database"}},
                        "actions": {
                            "severity": "critical",
                            "pagerduty_service": "database-oncall",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(result["severity"], "critical")
        self.assertEqual(result["pagerduty_service"], "database-oncall")

    def test_severity_in_list(self) -> None:
        """Test matching by severity in list."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Critical only",
                        "match": {
                            "incident.policy_name": {"contains": "Database"},
                            "incident.severity": {"in": ["CRITICAL"]},
                        },
                        "actions": {"severity": "critical"},
                    },
                    {
                        "name": "Error and below",
                        "match": {
                            "incident.policy_name": {"contains": "Database"},
                            "incident.severity": {"in": ["ERROR", "WARNING"]},
                        },
                        "actions": {"severity": "warning"},
                    },
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Should match second rule (ERROR)
        self.assertEqual(result["severity"], "warning")

    def test_resource_type_contains_match(self) -> None:
        """Test matching by resource type contains."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Cloud SQL alerts",
                        "match": {
                            "incident.resource_type_display_name": {
                                "contains": "cloud_sql"
                            }
                        },
                        "actions": {"pagerduty_service": "database-oncall"},
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(result["pagerduty_service"], "database-oncall")

    def test_title_prefix(self) -> None:
        """Test adding title prefix."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Add prefix",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {"title_prefix": "[DB-CRITICAL]"},
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(
            result["title"], "[DB-CRITICAL] Database connection errors detected"
        )

    def test_title_suffix(self) -> None:
        """Test adding title suffix."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Add suffix",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {"title_suffix": "(Production)"},
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(
            result["title"], "Database connection errors detected (Production)"
        )

    def test_title_transform(self) -> None:
        """Test template title transformation with field interpolation."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Transform title",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {
                            "title_transform": "[{incident.resource_type_display_name}] {incident.resource_display_name}: {incident.summary}",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(
            result["title"],
            "[cloud_sql_database] production-db: Database connection errors detected",
        )

    def test_combined_title_modifications(self) -> None:
        """Test combining prefix, suffix, and transform."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "All modifications",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {
                            "title_prefix": "[PROD]",
                            "title_suffix": "- URGENT",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(
            result["title"], "[PROD] Database connection errors detected - URGENT"
        )

    def test_multiple_matching_rules_applied(self) -> None:
        """Test that all matching rules are applied in order."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "First rule",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {"severity": "critical"},
                    },
                    {
                        "name": "Second rule",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {
                            "severity": "info"
                        },  # This should override the first rule
                    },
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Both rules match, but second rule's severity should win (applied last)
        self.assertEqual(result["severity"], "info")

    def test_resource_name_contains(self) -> None:
        """Test matching by resource name contains."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Production resources",
                        "match": {
                            "incident.resource_display_name": {"contains": "production"}
                        },
                        "actions": {"severity": "critical"},
                    },
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        self.assertEqual(result["severity"], "critical")

    def test_multiple_match_criteria(self) -> None:
        """Test that all match criteria must be satisfied."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Strict match",
                        "match": {
                            "incident.policy_name": {"contains": "Database"},
                            "incident.resource_type_display_name": {
                                "contains": "cloud_sql"
                            },
                            "incident.severity": {"in": ["ERROR", "CRITICAL"]},
                        },
                        "actions": {"severity": "critical"},
                    },
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # All criteria match
        self.assertEqual(result["severity"], "critical")

        # Modify to not match severity
        alert_copy = self.sample_alert.copy()
        alert_copy["incident"] = self.sample_alert["incident"].copy()
        alert_copy["incident"]["severity"] = "WARNING"

        result = engine.process_alert(PagerDutyAlert(alert_copy))
        # Should use default since severity doesn't match
        self.assertNotEqual(result["severity"], "critical")

    def test_exact_equality_match(self) -> None:
        """Test exact equality match for field."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Exact severity match",
                        "match": {"incident.severity": "ERROR"},
                        "actions": {"severity": "warning"},
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Should match (case-insensitive)
        self.assertEqual(result["severity"], "warning")

    def test_title_transform_with_missing_field(self) -> None:
        """Test template with missing field returns original placeholder."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Transform with missing field",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {
                            "title_transform": "Alert: {incident.nonexistent_field}",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Should keep placeholder when field not found
        self.assertEqual(result["title"], "Alert: {incident.nonexistent_field}")

    def test_title_transform_overrides_prefix_suffix(self) -> None:
        """Test that title_transform overrides prefix and suffix."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Transform overrides",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {
                            "title_prefix": "[SHOULD-NOT-APPEAR]",
                            "title_suffix": "(SHOULD-NOT-APPEAR)",
                            "title_transform": "{incident.resource_display_name}: {incident.summary}",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Transform should completely replace, ignoring prefix/suffix
        self.assertEqual(
            result["title"], "production-db: Database connection errors detected"
        )
        self.assertNotIn("SHOULD-NOT-APPEAR", result["title"])

    def test_rule_composition(self) -> None:
        """Test composing multiple rules for layered configuration."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "All database alerts go to database team",
                        "match": {
                            "incident.resource_type_display_name": {
                                "contains": "database"
                            }
                        },
                        "actions": {"pagerduty_service": "database-oncall"},
                    },
                    {
                        "name": "Production gets critical severity",
                        "match": {
                            "incident.resource_display_name": {"contains": "production"}
                        },
                        "actions": {"severity": "critical", "title_prefix": "[PROD]"},
                    },
                    {
                        "name": "Errors get error severity",
                        "match": {"incident.severity": "ERROR"},
                        "actions": {
                            "severity": "error"
                        },  # This overrides previous severity
                    },
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Should match all three rules in order
        # - First rule sets pagerduty_service to database-oncall
        # - Second rule sets severity to critical and adds prefix
        # - Third rule overrides severity to error
        self.assertEqual(result["pagerduty_service"], "database-oncall")
        self.assertEqual(result["severity"], "error")  # Last rule wins for severity
        self.assertTrue(
            result["title"].startswith("[PROD]")
        )  # Prefix from second rule preserved
