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
import re
import unittest
from pathlib import Path

import yaml

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

    def test_gcp_resource_name_helper(self) -> None:
        """Test the gcp_resource_name helper to extract resource name from GCP path."""
        alert_with_gcp_path = {
            "incident": {
                "incident_id": "test-incident-456",
                "policy_name": "Environment Health Check",
                "resource_name": "projects/recidiviz-staging/locations/us-central1/environments/experiment-anna",
                "summary": "Environment health check failed",
            }
        }

        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "GCP resource name extraction",
                        "match": {"incident.policy_name": {"contains": "Environment"}},
                        "actions": {
                            "title_transform": "[{incident.resource_name|gcp_resource_name}] {incident.summary}",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(alert_with_gcp_path))

        self.assertEqual(
            result["title"], "[experiment-anna] Environment health check failed"
        )

    def test_gcp_resource_name_helper_with_multiple_fields(self) -> None:
        """Test using gcp_resource_name helper with multiple fields in template."""
        alert_with_multiple_gcp_paths = {
            "incident": {
                "incident_id": "test-incident-789",
                "policy_name": "Resource Alert",
                "resource_name": "projects/recidiviz-staging/locations/us-central1/environments/experiment-bob",
                "database_name": "projects/recidiviz-staging/instances/prod-db",
                "summary": "Multiple resource alert",
            }
        }

        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Multiple GCP names",
                        "match": {"incident.policy_name": {"contains": "Resource"}},
                        "actions": {
                            "title_transform": "[{incident.resource_name|gcp_resource_name}] DB: {incident.database_name|gcp_resource_name} - {incident.summary}",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(alert_with_multiple_gcp_paths))

        self.assertEqual(
            result["title"],
            "[experiment-bob] DB: prod-db - Multiple resource alert",
        )

    def test_gcp_resource_name_helper_with_simple_name(self) -> None:
        """Test gcp_resource_name helper with a simple name (no slashes)."""
        alert_with_simple_name = {
            "incident": {
                "incident_id": "test-incident-999",
                "policy_name": "Simple Alert",
                "resource_name": "simple-resource",
                "summary": "Simple alert",
            }
        }

        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Simple name test",
                        "match": {"incident.policy_name": {"contains": "Simple"}},
                        "actions": {
                            "title_transform": "[{incident.resource_name|gcp_resource_name}] {incident.summary}",
                        },
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(alert_with_simple_name))

        # Should still work - just returns the whole string
        self.assertEqual(result["title"], "[simple-resource] Simple alert")

    def test_suppress_action(self) -> None:
        """Test that suppress action prevents alert forwarding."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "Suppress preview failures",
                        "match": {
                            "incident.resource_display_name": {"contains": "preview"}
                        },
                        "actions": {"suppress": True},
                    }
                ],
            }
        )

        engine = RuleEngine(config)

        # Alert matching suppression rule
        alert_with_preview = self.sample_alert.copy()
        alert_with_preview["incident"] = self.sample_alert["incident"].copy()
        alert_with_preview["incident"]["resource_display_name"] = "preview-db"

        result = engine.process_alert(PagerDutyAlert(alert_with_preview))
        self.assertTrue(result.get("suppress"))

    def test_suppress_action_overrides(self) -> None:
        """Test that suppress action can be overridden by later rules."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "First rule suppresses",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {"suppress": True},
                    },
                    {
                        "name": "Second rule does not suppress",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {"severity": "critical"},
                    },
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Both rules match - suppress should remain True since second rule doesn't override it
        self.assertTrue(result.get("suppress"))
        self.assertEqual(result["severity"], "critical")

    def test_no_suppress_by_default(self) -> None:
        """Test that alerts are not suppressed by default."""
        config = AlertForwarderConfig(
            {
                "default": {"pagerduty_service": "default"},
                "rules": [
                    {
                        "name": "No suppress action",
                        "match": {"incident.policy_name": {"contains": "Database"}},
                        "actions": {"severity": "critical"},
                    }
                ],
            }
        )

        engine = RuleEngine(config)
        result = engine.process_alert(PagerDutyAlert(self.sample_alert))

        # Should not be suppressed
        self.assertFalse(result.get("suppress"))

    def test_config_yaml_only_uses_supported_helpers(self) -> None:
        """Test that config.yaml only uses supported helper functions."""
        # Supported helpers
        supported_helpers = {"gcp_resource_name"}

        # Read the config.yaml file
        config_path = (
            Path(__file__).parent.parent.parent.parent
            / "monitoring"
            / "pagerduty_alert_forwarder"
            / "config.yaml"
        )

        with open(config_path, "r", encoding="utf-8") as f:
            config_content = f.read()

        # Extract all helpers used in the config using regex
        # Pattern: {field.path|helper_name}
        helper_pattern = re.compile(r"\{[a-zA-Z0-9_.]+\|([a-zA-Z0-9_]+)\}")
        helpers_found = helper_pattern.findall(config_content)

        # Check that all found helpers are supported
        unsupported_helpers = set(helpers_found) - supported_helpers

        self.assertEqual(
            unsupported_helpers,
            set(),
            f"Found unsupported helper functions in config.yaml: {unsupported_helpers}. "
            f"Supported helpers are: {supported_helpers}",
        )


class TestRuleEngineIntegration(unittest.TestCase):
    """Integration tests for rule engine with actual config.yaml."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Load the actual config.yaml
        config_path = (
            Path(__file__).parent.parent.parent.parent
            / "monitoring"
            / "pagerduty_alert_forwarder"
            / "config.yaml"
        )

        with open(config_path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)

        self.config = AlertForwarderConfig(config_data)
        self.engine = RuleEngine(self.config)

    def test_production_data_platform_cloud_run_job_failure(self) -> None:
        """Test production data platform alert with Cloud Run job failure."""
        alert = {
            "incident": {
                "incident_id": "test-123",
                "policy_name": "Cloud Run Job Failure",
                "resource": {
                    "labels": {
                        "project_id": "recidiviz-123",
                        "job_name": "calculation-pipeline-prod",
                    }
                },
                "summary": "Job failed with exit code 1",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should match both "Production Alerts" and "Cloud Run Job Failure" rules
        self.assertEqual(result["severity"], "error")
        self.assertEqual(
            result["pagerduty_service"], "[PRODUCTION] Data Platform Infrastructure"
        )
        # Title should have production prefix and job name after description
        self.assertEqual(
            result["title"],
            "🚨 [PRODUCTION] Cloud Run Job Failure: calculation-pipeline-prod",
        )

    def test_staging_data_platform_stale_metric_exports(self) -> None:
        """Test staging data platform alert with stale metric exports."""
        alert = {
            "incident": {
                "incident_id": "test-456",
                "policy_name": "GCS: Metric exports have not been uploaded in 24 hours",
                "resource": {
                    "labels": {
                        "project_id": "recidiviz-staging",
                    }
                },
                "metric": {
                    "labels": {
                        "region": "US_CA",
                    }
                },
                "summary": "No metric exports in 24 hours",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should match both "Staging Alerts" and "Stale Metric Exports" rules
        self.assertEqual(result["severity"], "warning")
        self.assertEqual(
            result["pagerduty_service"], "[STAGING] Data Platform Infrastructure"
        )
        # Title should have staging prefix and region label
        self.assertEqual(result["title"], "[STAGING] [US_CA] Stale Metric Exports")

    def test_production_dashboard_pubsub_failure(self) -> None:
        """Test production dashboard alert with Pub/Sub failure."""
        alert = {
            "incident": {
                "incident_id": "test-789",
                "policy_name": "Cloud Pub/Sub Subscription - Message not acknowledged within an hour",
                "resource": {
                    "labels": {
                        "project_id": "recidiviz-dashboard-production",
                        "subscription_id": "dashboard-events-sub",
                    }
                },
                "summary": "Messages not acknowledged",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should match both "Dashboard Production Alerts" and "Cloud Pub/Sub Handler" rules
        self.assertEqual(result["severity"], "error")
        self.assertEqual(result["pagerduty_service"], "[PRODUCTION] Dashboards Project")
        # Title should have production prefix and subscription ID after description
        self.assertEqual(
            result["title"],
            "🚨 [PRODUCTION] Pub/Sub Message Failure to Acknowledge: dashboard-events-sub",
        )

    def test_staging_dashboard_dataflow_vcpu(self) -> None:
        """Test staging dashboard alert with Dataflow high vCPU."""
        alert = {
            "incident": {
                "incident_id": "test-101",
                "policy_name": "[Dataflow] Job using more than 224 vCPUs",
                "resource": {
                    "labels": {
                        "project_id": "recidiviz-dashboard-staging",
                    }
                },
                "metadata": {
                    "user_labels": {
                        "dataflow_pipeline_job": "dashboard-metrics-staging",
                    }
                },
                "summary": "High vCPU usage detected",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should match both "Dashboard Staging Alerts" and "Dataflow Job vCPU" rules
        # Note: Dataflow rule sets severity to warning, which should override staging default
        self.assertEqual(result["severity"], "warning")
        self.assertEqual(result["pagerduty_service"], "[STAGING] Dashboards Project")
        # Title should have pipeline job name after description
        # Note: No prefix since Dashboard Staging Alerts doesn't set one
        self.assertEqual(
            result["title"],
            "[STAGING] Dataflow Job High vCPU Use: dashboard-metrics-staging",
        )

    def test_production_data_platform_airflow_idle_environment(self) -> None:
        """Test production data platform alert with idle Airflow environment."""
        alert = {
            "incident": {
                "incident_id": "test-202",
                "policy_name": "[Airflow] Potentially idle experiment environment",
                "resource": {
                    "labels": {
                        "project_id": "recidiviz-123",
                        "airflow_environment_name": "projects/recidiviz-123/locations/us-central1/environments/experiment-alice",
                    }
                },
                "summary": "Environment idle for 3 days",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should match both "Production Alerts" and "Airflow Idle Experiment Instance" rules
        # Airflow rule sets severity to warning, overriding production error
        self.assertEqual(result["severity"], "warning")
        self.assertEqual(
            result["pagerduty_service"], "[PRODUCTION] Data Platform Infrastructure"
        )
        # Title should have production prefix and extracted environment name using gcp_resource_name helper
        self.assertEqual(
            result["title"],
            "🚨 [PRODUCTION] Potentially Idle Airflow Development Environment: experiment-alice",
        )

    def test_production_data_platform_failed_scheduled_query(self) -> None:
        """Test production data platform alert with failed scheduled query."""
        alert = {
            "incident": {
                "incident_id": "test-303",
                "policy_name": "BQ Scheduled Query Monitoring",
                "resource": {
                    "labels": {
                        "project_id": "recidiviz-123",
                        "config_id": "scheduled-query-12345",
                    }
                },
                "summary": "Scheduled query failed",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should match both "Production Alerts" and "Failed Scheduled Query" rules
        # Failed Scheduled Query sets severity to warning, overriding production error
        self.assertEqual(result["severity"], "warning")
        self.assertEqual(
            result["pagerduty_service"], "[PRODUCTION] Data Platform Infrastructure"
        )
        # Title should have production prefix and config ID after description
        self.assertEqual(
            result["title"],
            "🚨 [PRODUCTION] Failed BigQuery Scheduled Query: scheduled-query-12345",
        )

    def test_production_data_platform_airflow_dag_parse_error(self) -> None:
        """Test production data platform alert with Airflow DAG parse error."""
        alert = {
            "incident": {
                "incident_id": "test-404",
                "policy_name": "Airflow - DAG Parse Error",
                "resource": {
                    "labels": {
                        "project_id": "recidiviz-123",
                    }
                },
                "summary": "DAG parsing failed",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should match both "Production Alerts" and "Airflow DAG Parse Error" rules
        # Airflow DAG Parse Error sets severity to critical, overriding production error
        self.assertEqual(result["severity"], "critical")
        self.assertEqual(
            result["pagerduty_service"], "[PRODUCTION] Data Platform Infrastructure"
        )
        # Title should have production prefix and the DAG parse error title
        self.assertEqual(result["title"], "🚨 [PRODUCTION] Airflow - DAG Parse Error")

    def test_unmatched_alert_uses_defaults(self) -> None:
        """Test that alerts not matching any project or policy use defaults."""
        alert = {
            "incident": {
                "incident_id": "test-999",
                "policy_name": "Unknown Alert Type",
                "resource": {
                    "labels": {
                        "project_id": "unknown-project",
                    }
                },
                "summary": "Some random alert",
            }
        }

        result = self.engine.process_alert(PagerDutyAlert(alert))

        # Should use default config
        self.assertEqual(result["severity"], "info")
        self.assertEqual(result["pagerduty_service"], "GCP Infrastructure")
        # Title should be the original summary since no transformations apply
        self.assertEqual(result["title"], "Some random alert")
