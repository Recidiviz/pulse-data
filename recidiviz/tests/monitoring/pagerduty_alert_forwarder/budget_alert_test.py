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
"""Tests for budget_alert module."""
import unittest
from typing import Any

from recidiviz.monitoring.pagerduty_alert_forwarder.budget_alert import (
    MAX_TITLE_LENGTH,
    BudgetEvent,
    build_budget_event,
    is_budget_notification,
)

BUDGET_NAME = "[Cloud Costs] BigQuery - $20,000 per month"
ATTRIBUTES = {
    "billingAccountId": "012345-ABCDEF-678901",
    "budgetId": "b0b0b0b0-1111-2222-3333-444444444444",
    "schemaVersion": "1.0",
}


def _budget_message(**overrides: Any) -> dict[str, Any]:
    message: dict[str, Any] = {
        "budgetDisplayName": BUDGET_NAME,
        "costAmount": 14684.37,
        "costIntervalStart": "2026-10-01T07:00:00Z",
        "budgetAmount": 20000.0,
        "budgetAmountType": "SPECIFIED_AMOUNT",
        "currencyCode": "USD",
    }
    message.update(overrides)
    return message


def _build(**overrides: Any) -> BudgetEvent:
    event = build_budget_event(_budget_message(**overrides), ATTRIBUTES)
    assert event is not None
    return event


class TestIsBudgetNotification(unittest.TestCase):
    """Tests for is_budget_notification."""

    def test_budget_message(self) -> None:
        self.assertTrue(is_budget_notification(_budget_message()))

    def test_monitoring_incident(self) -> None:
        self.assertFalse(is_budget_notification({"incident": {"policy_name": "p"}}))

    def test_incident_key_wins_over_budget_field(self) -> None:
        self.assertFalse(is_budget_notification(_budget_message(incident={})))

    def test_other_message(self) -> None:
        self.assertFalse(is_budget_notification({"foo": "bar"}))


class TestBuildBudgetEvent(unittest.TestCase):
    """Tests for build_budget_event."""

    def test_actual_threshold_warning(self) -> None:
        event = _build(alertThresholdExceeded=0.71)

        self.assertEqual(
            "[Cloud Costs] BigQuery - $20,000 per month: 71% of budget reached "
            "(cost $14,684)",
            event.title,
        )
        self.assertEqual("warning", event.severity)
        self.assertEqual("cloud-billing", event.component)
        self.assertEqual(
            "012345-ABCDEF-678901:b0b0b0b0-1111-2222-3333-444444444444:actual:0.71:"
            "2026-10-01T07:00:00Z",
            event.dedup_key,
        )
        self.assertEqual(
            {
                "budgetDisplayName": BUDGET_NAME,
                "budgetId": ATTRIBUTES["budgetId"],
                "billingAccountId": ATTRIBUTES["billingAccountId"],
                "costAmount": 14684.37,
                "budgetAmount": 20000.0,
                "currencyCode": "USD",
                "threshold": 0.71,
                "kind": "actual",
                "costIntervalStart": "2026-10-01T07:00:00Z",
            },
            event.custom_details,
        )

    def test_actual_threshold_critical(self) -> None:
        event = _build(alertThresholdExceeded=1.0, costAmount=20100)

        self.assertEqual("critical", event.severity)
        self.assertIn("100% of budget reached", event.title)

    def test_actual_threshold_above_budget_critical(self) -> None:
        self.assertEqual("critical", _build(alertThresholdExceeded=1.5).severity)

    def test_actual_threshold_info(self) -> None:
        event = _build(alertThresholdExceeded=0.5)

        self.assertEqual("info", event.severity)
        self.assertIn("50% of budget reached", event.title)

    def test_actual_threshold_just_below_warning(self) -> None:
        self.assertEqual("info", _build(alertThresholdExceeded=0.7).severity)

    def test_actual_threshold_between_warning_and_critical(self) -> None:
        self.assertEqual("warning", _build(alertThresholdExceeded=0.9).severity)

    def test_forecast_threshold(self) -> None:
        event = _build(forecastThresholdExceeded=1.0)

        self.assertEqual(
            "[Cloud Costs] BigQuery - $20,000 per month: forecast to reach 100% of "
            "budget (cost $14,684)",
            event.title,
        )
        self.assertEqual("warning", event.severity)
        self.assertEqual("forecast", event.custom_details["kind"])
        self.assertIn(":forecast:1.0:", event.dedup_key)

    def test_actual_takes_precedence_over_forecast(self) -> None:
        event = _build(alertThresholdExceeded=0.5, forecastThresholdExceeded=1.0)

        self.assertEqual("actual", event.custom_details["kind"])

    def test_routine_update_is_suppressed(self) -> None:
        self.assertIsNone(build_budget_event(_budget_message(), ATTRIBUTES))

    def test_non_numeric_threshold_is_suppressed(self) -> None:
        self.assertIsNone(
            build_budget_event(
                _budget_message(alertThresholdExceeded="lots"), ATTRIBUTES
            )
        )

    def test_title_does_not_add_cloud_costs_prefix(self) -> None:
        event = _build(budgetDisplayName="Plain budget", alertThresholdExceeded=0.5)

        self.assertTrue(event.title.startswith("Plain budget: 50% of budget"))

    def test_non_usd_currency(self) -> None:
        event = _build(alertThresholdExceeded=0.5, currencyCode="EUR")

        self.assertTrue(event.title.endswith("(cost EUR 14,684)"))

    def test_missing_cost_amount(self) -> None:
        message = _budget_message(alertThresholdExceeded=0.5)
        del message["costAmount"]
        event = build_budget_event(message, ATTRIBUTES)

        assert event is not None
        self.assertEqual(f"{BUDGET_NAME}: 50% of budget reached", event.title)

    def test_title_is_truncated(self) -> None:
        event = _build(budgetDisplayName="x" * 2000, alertThresholdExceeded=0.5)

        self.assertEqual(MAX_TITLE_LENGTH, len(event.title))
        self.assertTrue(event.title.endswith("..."))

    def test_dedup_key_differs_by_threshold_budget_and_month(self) -> None:
        base = _build(alertThresholdExceeded=0.5)
        other_threshold = _build(alertThresholdExceeded=0.71)
        other_month = _build(
            alertThresholdExceeded=0.5, costIntervalStart="2026-11-01T07:00:00Z"
        )
        other_budget = build_budget_event(
            _budget_message(alertThresholdExceeded=0.5),
            {**ATTRIBUTES, "budgetId": "another-budget"},
        )
        assert other_budget is not None

        self.assertEqual(
            4,
            len(
                {
                    base.dedup_key,
                    other_threshold.dedup_key,
                    other_month.dedup_key,
                    other_budget.dedup_key,
                }
            ),
        )

    def test_dedup_key_is_stable_for_redelivery(self) -> None:
        self.assertEqual(
            _build(alertThresholdExceeded=0.5).dedup_key,
            _build(alertThresholdExceeded=0.5).dedup_key,
        )

    def test_dedup_key_falls_back_to_display_name_without_budget_id(self) -> None:
        event = build_budget_event(
            _budget_message(alertThresholdExceeded=0.5),
            {"billingAccountId": "acct"},
        )

        assert event is not None
        self.assertEqual(
            f"acct:{BUDGET_NAME}:actual:0.5:2026-10-01T07:00:00Z", event.dedup_key
        )

    def test_dedup_key_max_length(self) -> None:
        event = build_budget_event(
            _budget_message(budgetDisplayName="x" * 400, alertThresholdExceeded=0.5),
            {"billingAccountId": "acct"},
        )

        assert event is not None
        self.assertEqual(255, len(event.dedup_key))
