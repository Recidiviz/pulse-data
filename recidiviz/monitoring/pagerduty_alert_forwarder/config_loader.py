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
"""Configuration loader and validator for alert forwarder rulesets."""
import abc
import os
from typing import Any

import attr
import yaml


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""


class Condition(abc.ABC):
    """Abstract base class for match conditions."""

    @abc.abstractmethod
    def evaluate(self, value: Any) -> bool:
        """Evaluate if the given value matches this condition.

        Args:
            value: The value to check against the condition

        Returns:
            True if the value matches the condition
        """


@attr.define
class EqualityCondition(Condition):
    """Condition that checks for exact equality (case-insensitive for strings)."""

    expected_value: str | int | float | bool

    def __attrs_post_init__(self) -> None:
        """Validate that the expected value is a primitive type."""
        if not isinstance(self.expected_value, (str, int, float, bool)):
            raise ConfigurationError(
                f"EqualityCondition requires a string, number, or boolean value, "
                f"got {type(self.expected_value).__name__}"
            )

    def evaluate(self, value: Any) -> bool:
        """Check if value equals the expected value (case-insensitive for strings)."""
        return str(value).lower() == str(self.expected_value).lower()


@attr.define
class ContainsCondition(Condition):
    """Condition that checks if a value contains a substring (case-insensitive)."""

    search_term: str

    def __attrs_post_init__(self) -> None:
        """Validate that the search term is a string."""
        if not isinstance(self.search_term, str):
            raise ConfigurationError(
                f"ContainsCondition requires a string search term, "
                f"got {type(self.search_term).__name__}"
            )

    def evaluate(self, value: Any) -> bool:
        """Check if value contains the search term (case-insensitive)."""
        return self.search_term.lower() in str(value).lower()


@attr.define
class InCondition(Condition):
    """Condition that checks if a value is in a list of allowed values."""

    allowed_values: list[Any]

    def __attrs_post_init__(self) -> None:
        """Validate that allowed_values is a list."""
        if not isinstance(self.allowed_values, list):
            raise ConfigurationError(
                f"InCondition requires a list of allowed values, "
                f"got {type(self.allowed_values).__name__}"
            )

    def evaluate(self, value: Any) -> bool:
        """Check if value is in the list of allowed values (case-insensitive)."""
        value_upper = str(value).upper()
        allowed_upper = [str(v).upper() for v in self.allowed_values]
        return value_upper in allowed_upper


@attr.define
class MatchCondition:
    """A single match condition for a field path."""

    field_path: str
    condition: Condition


@attr.define
class MatchConfig:
    """Match criteria configuration."""

    conditions: list[MatchCondition]

    @classmethod
    def from_dict(cls, match_dict: dict[str, Any], rule_index: int) -> "MatchConfig":
        """Create MatchConfig from dictionary, validating the input.

        Args:
            match_dict: Match criteria dictionary
            rule_index: Rule index for error messages

        Returns:
            MatchConfig instance

        Raises:
            ConfigurationError: If match configuration is invalid
        """
        if not isinstance(match_dict, dict):
            raise ConfigurationError(f"Rule {rule_index} 'match' must be a dictionary")

        # At least one match criterion required
        if not match_dict:
            raise ConfigurationError(
                f"Rule {rule_index} 'match' must have at least one criterion"
            )

        conditions = []
        # Validate each field path and condition
        for field_path, condition_value in match_dict.items():
            # Validate field path format (should be dot-separated)
            if not isinstance(field_path, str) or not field_path:
                raise ConfigurationError(
                    f"Rule {rule_index} match key '{field_path}' must be a non-empty string"
                )

            # Create the appropriate Condition subclass based on the condition value
            # Validation happens in the Condition subclass __attrs_post_init__
            condition: Condition
            try:
                if isinstance(condition_value, dict):
                    # Complex condition
                    if "contains" in condition_value:
                        condition = ContainsCondition(
                            search_term=condition_value["contains"]
                        )
                    elif "in" in condition_value:
                        condition = InCondition(allowed_values=condition_value["in"])
                    else:
                        raise ConfigurationError(
                            f"Rule {rule_index} match '{field_path}' has unknown condition type: {condition_value} "
                            f"Supported: 'contains', 'in'"
                        )
                elif isinstance(condition_value, (str, int, float, bool)):
                    # Simple equality condition
                    condition = EqualityCondition(expected_value=condition_value)
                else:
                    raise ConfigurationError(
                        f"Rule {rule_index} match '{field_path}' must be a string, number, boolean, "
                        f"or dict with 'contains'/'in'"
                    )
            except ConfigurationError:
                # Re-raise ConfigurationError as-is
                raise
            except Exception as e:
                # Wrap any other errors with context
                raise ConfigurationError(
                    f"Rule {rule_index} match '{field_path}': {e}"
                ) from e

            conditions.append(
                MatchCondition(field_path=field_path, condition=condition)
            )

        return cls(conditions=conditions)


@attr.define
class ActionsConfig:
    """Actions configuration for a rule."""

    SEVERITY_KEY = "severity"
    PAGERDUTY_SERVICE_KEY = "pagerduty_service"
    TITLE_TRANSFORM_KEY = "title_transform"
    TITLE_PREFIX_KEY = "title_prefix"
    TITLE_SUFFIX_KEY = "title_suffix"

    severity: str | None = attr.field(default=None)
    pagerduty_service: str | None = attr.field(default=None)
    title_transform: str | None = attr.field(default=None)
    title_prefix: str | None = attr.field(default=None)
    title_suffix: str | None = attr.field(default=None)

    @classmethod
    def from_dict(
        cls, actions_dict: dict[str, Any], rule_index: int
    ) -> "ActionsConfig":
        """Create ActionsConfig from dictionary, validating the input.

        Args:
            actions_dict: Actions dictionary
            rule_index: Rule index for error messages

        Returns:
            ActionsConfig instance

        Raises:
            ConfigurationError: If actions configuration is invalid
        """
        if not isinstance(actions_dict, dict):
            raise ConfigurationError(
                f"Rule {rule_index} 'actions' must be a dictionary"
            )

        severity = None
        if cls.SEVERITY_KEY in actions_dict:
            severity = actions_dict[cls.SEVERITY_KEY]
            valid_severities = {"info", "warning", "error", "critical"}
            if severity.lower() not in valid_severities:
                raise ConfigurationError(
                    f"Rule {rule_index} actions.severity must be one of: {', '.join(valid_severities)}"
                )

        pagerduty_service = None
        if cls.PAGERDUTY_SERVICE_KEY in actions_dict:
            pagerduty_service = actions_dict[cls.PAGERDUTY_SERVICE_KEY]
            if not isinstance(pagerduty_service, str):
                raise ConfigurationError(
                    f"Rule {rule_index} 'actions.pagerduty_service' must be a string"
                )

        title_transform = None
        if cls.TITLE_TRANSFORM_KEY in actions_dict:
            title_transform = actions_dict[cls.TITLE_TRANSFORM_KEY]
            if not isinstance(title_transform, str):
                raise ConfigurationError(
                    f"Rule {rule_index} 'actions.title_transform' must be a string template"
                )

        title_prefix = None
        if cls.TITLE_PREFIX_KEY in actions_dict:
            title_prefix = actions_dict[cls.TITLE_PREFIX_KEY]

        title_suffix = None
        if cls.TITLE_SUFFIX_KEY in actions_dict:
            title_suffix = actions_dict[cls.TITLE_SUFFIX_KEY]

        return cls(
            severity=severity,
            pagerduty_service=pagerduty_service,
            title_transform=title_transform,
            title_prefix=title_prefix,
            title_suffix=title_suffix,
        )


@attr.define
class RuleConfig:
    """Configuration for a single rule."""

    NAME_KEY = "name"
    MATCH_KEY = "match"
    ACTIONS_KEY = "actions"

    name: str
    match: MatchConfig
    actions: ActionsConfig

    @classmethod
    def from_dict(cls, rule_dict: dict[str, Any], index: int) -> "RuleConfig":
        """Create RuleConfig from dictionary, validating the input.

        Args:
            rule_dict: Rule configuration dictionary
            index: Rule index for error messages

        Returns:
            RuleConfig instance

        Raises:
            ConfigurationError: If rule configuration is invalid
        """
        if not isinstance(rule_dict, dict):
            raise ConfigurationError(f"Rule {index} must be a dictionary")

        name = "unnamed"
        if cls.NAME_KEY in rule_dict:
            name = rule_dict[cls.NAME_KEY]
            if not isinstance(name, str):
                raise ConfigurationError(f"Rule {index} 'name' must be a string")

        # Validate and extract match criteria (required)
        if cls.MATCH_KEY not in rule_dict:
            raise ConfigurationError(f"Rule {index} must have 'match' criteria")

        match = MatchConfig.from_dict(rule_dict[cls.MATCH_KEY], index)

        # Validate and extract actions (required)
        if cls.ACTIONS_KEY not in rule_dict:
            raise ConfigurationError(f"Rule {index} must have 'actions'")

        actions = ActionsConfig.from_dict(rule_dict[cls.ACTIONS_KEY], index)

        return cls(
            name=name,
            match=match,
            actions=actions,
        )


@attr.define
class DefaultConfig:
    """Default configuration for alerts that don't match any rules."""

    PAGERDUTY_SERVICE_KEY = "pagerduty_service"
    SEVERITY_KEY = "severity"

    severity: str = attr.field(default="info")
    pagerduty_service: str = attr.field(default="")

    @classmethod
    def from_dict(cls, default_dict: dict[str, Any]) -> "DefaultConfig":
        """Create DefaultConfig from dictionary, validating the input.

        Args:
            default_dict: Default configuration dictionary

        Returns:
            DefaultConfig instance

        Raises:
            ConfigurationError: If default configuration is invalid
        """
        if not isinstance(default_dict, dict):
            raise ConfigurationError("'default' must be a dictionary")

        pagerduty_service = ""
        if cls.PAGERDUTY_SERVICE_KEY in default_dict:
            pagerduty_service = default_dict[cls.PAGERDUTY_SERVICE_KEY]

            if not isinstance(pagerduty_service, str):
                raise ConfigurationError(
                    "'default.{cls.PAGERDUTY_SERVICE_KEY}' must be a string"
                )

        severity = "info"
        if cls.SEVERITY_KEY in default_dict:
            severity = default_dict[cls.SEVERITY_KEY]
            valid_severities = {"info", "warning", "error", "critical"}
            if severity.lower() not in valid_severities:
                raise ConfigurationError(
                    f"'default.{cls.SEVERITY_KEY}' must be one of: {', '.join(valid_severities)}"
                )

        return cls(
            severity=severity,
            pagerduty_service=pagerduty_service,
        )


class AlertForwarderConfig:
    """Configuration for alert forwarder service."""

    def __init__(self, config_dict: dict[str, Any]) -> None:
        """Initialize configuration from dictionary.

        Args:
            config_dict: Configuration dictionary loaded from YAML

        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Convert default dict to DefaultConfig class
        self.default = DefaultConfig.from_dict(config_dict.get("default", {}))

        # Convert rules list to RuleConfig instances
        rules_list = config_dict.get("rules", [])
        if not isinstance(rules_list, list):
            raise ConfigurationError("'rules' must be a list")

        self.rules = [
            RuleConfig.from_dict(rule, idx) for idx, rule in enumerate(rules_list)
        ]

    @classmethod
    def from_file(cls, config_path: str) -> "AlertForwarderConfig":
        """Load configuration from YAML file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            AlertForwarderConfig instance

        Raises:
            ConfigurationError: If file cannot be loaded or is invalid
        """
        if not os.path.exists(config_path):
            raise ConfigurationError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in configuration file: {e}") from e

        return cls(config_dict)
