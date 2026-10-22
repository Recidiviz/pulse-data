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
"""Builds PagerDuty events from GCP Billing budget notifications.

Budget notifications are published to Pub/Sub by Cloud Billing, not by Cloud
Monitoring, so they have no `incident` key and cannot be titled or deduplicated by the
rule engine. See
https://cloud.google.com/billing/docs/how-to/budgets-programmatic-notifications
"""
from collections.abc import Mapping
from typing import Any

import attr

BUDGET_COMPONENT = "cloud-billing"
BUDGET_SOURCE = "gcp-billing-budget"

ACTUAL_KIND = "actual"
FORECAST_KIND = "forecast"

MAX_TITLE_LENGTH = 1024
MAX_DEDUP_KEY_LENGTH = 255

# Fraction of the budget at or above which an actual-spend notification is critical
CRITICAL_THRESHOLD = 1.0
# Percent of the budget at or above which an actual-spend notification is a warning
WARNING_THRESHOLD_PERCENT = 71


@attr.define(frozen=True)
class BudgetEvent:
    """The PagerDuty event derived from a budget notification."""

    title: str
    severity: str
    dedup_key: str
    component: str
    custom_details: dict[str, Any]


def is_budget_notification(raw_data: Mapping[str, Any]) -> bool:
    """Returns True if the message is a GCP Billing budget notification."""
    return "incident" not in raw_data and "budgetDisplayName" in raw_data


def _parse_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_money(amount: float, currency_code: str) -> str:
    sign = "-" if round(amount) < 0 else ""
    prefix = "$" if currency_code == "USD" else f"{currency_code} "
    return f"{sign}{prefix}{abs(amount):,.0f}"


def _truncate(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def build_budget_event(
    raw_data: Mapping[str, Any], attributes: Mapping[str, str]
) -> BudgetEvent | None:
    """Builds the PagerDuty event for a budget notification.

    Budgets also publish routine cost updates several times a day. Those carry neither
    `alertThresholdExceeded` nor `forecastThresholdExceeded` and must not page, so this
    returns None for them.

    Args:
        raw_data: Decoded JSON body of the Pub/Sub message
        attributes: Pub/Sub message attributes (billingAccountId, budgetId, ...)

    Returns:
        The event to send, or None if the notification should not alert.
    """
    actual_threshold = _parse_number(raw_data.get("alertThresholdExceeded"))
    forecast_threshold = _parse_number(raw_data.get("forecastThresholdExceeded"))

    if actual_threshold is not None:
        kind, threshold = ACTUAL_KIND, actual_threshold
    elif forecast_threshold is not None:
        kind, threshold = FORECAST_KIND, forecast_threshold
    else:
        return None

    budget_name = str(raw_data["budgetDisplayName"])
    percent = round(threshold * 100)

    if kind == FORECAST_KIND:
        title = f"{budget_name}: forecast to reach {percent}% of budget"
        severity = "warning"
    else:
        title = f"{budget_name}: {percent}% of budget reached"
        if threshold >= CRITICAL_THRESHOLD:
            severity = "critical"
        elif percent >= WARNING_THRESHOLD_PERCENT:
            severity = "warning"
        else:
            severity = "info"

    cost = _parse_number(raw_data.get("costAmount"))
    if cost is not None:
        title += f" (cost {_format_money(cost, str(raw_data.get('currencyCode', '')))})"

    billing_account_id = attributes.get("billingAccountId", "")
    budget_id = attributes.get("budgetId", "")
    cost_interval_start = str(raw_data.get("costIntervalStart", ""))

    dedup_key = ":".join(
        [
            billing_account_id,
            budget_id or budget_name,
            kind,
            str(threshold),
            cost_interval_start,
        ]
    )

    return BudgetEvent(
        title=_truncate(title, MAX_TITLE_LENGTH),
        severity=severity,
        dedup_key=_truncate(dedup_key, MAX_DEDUP_KEY_LENGTH),
        component=BUDGET_COMPONENT,
        custom_details={
            "budgetDisplayName": budget_name,
            "budgetId": budget_id,
            "billingAccountId": billing_account_id,
            "costAmount": raw_data.get("costAmount"),
            "budgetAmount": raw_data.get("budgetAmount"),
            "currencyCode": raw_data.get("currencyCode"),
            "threshold": threshold,
            "kind": kind,
            "costIntervalStart": cost_interval_start,
        },
    )
