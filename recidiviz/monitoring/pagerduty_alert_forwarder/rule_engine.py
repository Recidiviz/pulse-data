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
"""Rule engine for evaluating and transforming alerts."""
import logging
import re
from typing import Any

from recidiviz.monitoring.pagerduty_alert_forwarder.config_loader import (
    ActionsConfig,
    AlertForwarderConfig,
    RuleConfig,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_alert import (
    PagerDutyAlert,
)

logger = logging.getLogger(__name__)


class RuleEngine:
    """Evaluates alerts against configured rules and applies transformations."""

    def __init__(self, config: AlertForwarderConfig) -> None:
        """Initialize rule engine with configuration.

        Args:
            config: Alert forwarder configuration
        """
        self.config = config

    def process_alert(self, alert: PagerDutyAlert) -> dict[str, Any]:
        """Process an alert through the rule engine.

        Args:
            alert: PagerDuty alert data wrapper

        Returns:
            Dictionary with processed alert information:
                - severity: PagerDuty severity (info/warning/error/critical)
                - pagerduty_service: Integration key for PagerDuty service
                - title: Modified incident title
                - original_alert: Original alert data for context
        """
        # Extract alert metadata
        incident = alert.raw_data.get("incident", {})
        alert_name = incident.get("policy_name", "")
        summary = incident.get("summary", alert_name)

        # Default values from config or fallback
        result = {
            "severity": self.config.default.severity,
            "pagerduty_service": self.config.default.pagerduty_service,
            "title": summary,
            "original_alert": alert.raw_data,
        }

        # Evaluate all rules in order and apply matching rules
        matched_rules = []
        for rule in self.config.rules:
            if self._matches_rule(rule, alert):
                matched_rules.append(rule.name)
                logger.info(
                    "Alert matched rule: %s",
                    rule.name,
                    extra={"alert_name": alert_name, "rule_name": rule.name},
                )
                # Apply actions from this rule
                result = self._apply_actions(rule.actions, result, summary, alert)

                # Update summary for next rule if title was modified
                summary = result.get("title", summary)

        if not matched_rules:
            logger.info(
                "No rules matched alert, using defaults",
                extra={"alert_name": alert_name},
            )
        else:
            logger.info(
                "Alert matched %d rule(s): %s",
                len(matched_rules),
                ", ".join(matched_rules),
                extra={"alert_name": alert_name, "matched_rules": matched_rules},
            )

        return result

    def _matches_rule(
        self,
        rule: RuleConfig,
        alert: PagerDutyAlert,
    ) -> bool:
        """Check if alert matches rule criteria.

        Match criteria can be:
        - Field equality: {"incident.severity": "ERROR"}
        - Field contains (string): {"incident.policy_name": {"contains": "database"}}
        - Field in list: {"incident.severity": {"in": ["ERROR", "CRITICAL"]}}

        Args:
            rule: Rule configuration
            alert: Alert data wrapper with cached field paths

        Returns:
            True if all match criteria are satisfied
        """
        # Check each match criterion
        for match_condition in rule.match.conditions:
            # Get the actual value from alert data using cached lookup
            value = alert.get(match_condition.field_path)

            if value is None and not alert.has_field(match_condition.field_path):
                # Field doesn't exist in alert data - no match
                available_fields = alert.get_available_fields()
                logger.debug(
                    "Field not found in alert data: %s. Available fields: %s",
                    match_condition.field_path,
                    ", ".join(available_fields[:10])
                    + ("..." if len(available_fields) > 10 else ""),
                    extra={
                        "field_path": match_condition.field_path,
                        "available_fields": available_fields,
                    },
                )
                return False

            # Evaluate the condition using the Condition's evaluate method
            if not match_condition.condition.evaluate(value):
                return False

        # All criteria matched
        return True

    def _apply_actions(
        self,
        actions: ActionsConfig,
        result: dict[str, Any],
        original_title: str,
        alert: PagerDutyAlert,
    ) -> dict[str, Any]:
        """Apply rule actions to transform alert.

        Args:
            actions: Actions configuration
            result: Current result dictionary
            original_title: Original alert title
            alert: Alert data wrapper for field path lookups

        Returns:
            Modified result dictionary
        """
        # Update severity if specified
        if actions.severity is not None:
            result["severity"] = actions.severity.lower()

        # Update PagerDuty service if specified
        if actions.pagerduty_service is not None:
            result["pagerduty_service"] = actions.pagerduty_service

        # Apply title transformations
        title = original_title

        # Add prefix if specified
        if actions.title_prefix is not None:
            title = f"{actions.title_prefix} {title}"

        # Add suffix if specified
        if actions.title_suffix is not None:
            title = f"{title} {actions.title_suffix}"

        # Apply template transformation if specified
        if actions.title_transform is not None:
            template = actions.title_transform
            if template:
                try:
                    # Interpolate JSON fields from alert data
                    title = self._interpolate_json_fields(template, alert)
                except Exception as e:
                    logger.warning(
                        "Failed to apply title transformation: %s",
                        e,
                        extra={"template": template},
                    )

        result["title"] = title
        return result

    def _interpolate_json_fields(self, template: str, alert: PagerDutyAlert) -> str:
        """Interpolate JSON field references in a template string.

        Supports dot notation for nested fields:
        - {incident.policy_name} -> alert["incident"]["policy_name"]
        - {incident.resource_display_name} -> alert["incident"]["resource_display_name"]

        Args:
            template: Template string with {field.path} placeholders
            alert: Alert data wrapper with cached field paths

        Returns:
            Template with placeholders replaced by actual values
        """
        # Pattern to match {field.path} or {field.nested.path}
        field_pattern = re.compile(r"\{([a-zA-Z0-9_.]+)\}")

        def replace_field(match: re.Match[str]) -> str:
            field_path = match.group(1)
            value = alert.get(field_path)

            if value is None and not alert.has_field(field_path):
                # Field not found in alert data
                available_fields = alert.get_available_fields()
                logger.warning(
                    "Field not found in alert data: %s. Available fields: %s",
                    field_path,
                    ", ".join(available_fields[:10])
                    + ("..." if len(available_fields) > 10 else ""),
                    extra={
                        "field_path": field_path,
                        "available_fields": available_fields,
                    },
                )
                # Return original placeholder if field not found
                return match.group(0)

            return str(value)

        return field_pattern.sub(replace_field, template)
