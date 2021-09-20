# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Delegate class for specifying certain metrics for the PO Monthly Report"""
import abc
import itertools
from typing import Dict, List

from recidiviz.reporting.context.po_monthly_report.constants import (
    ABSCONSIONS,
    DISTRICT,
    EMAIL_ADDRESS,
    OFFICER_EXTERNAL_ID,
    OFFICER_GIVEN_NAME,
    REVIEW_MONTH,
    REVOCATIONS_CLIENTS,
    STATE_CODE,
    TOTAL_REVOCATIONS,
)

REQUIRED_RECIPIENT_DATA_FIELDS = [
    OFFICER_EXTERNAL_ID,
    STATE_CODE,
    DISTRICT,
    EMAIL_ADDRESS,
    OFFICER_GIVEN_NAME,
    REVIEW_MONTH,
]


class PoMonthlyReportMetricsDelegate(abc.ABC):
    """Contains the base class for specifying certain metrics for the PO Monthly Report."""

    @property
    @abc.abstractmethod
    def decarceral_actions_metrics(self) -> List[str]:
        """Denotes metrics of decarceral actions initiated by supervising officer."""

    @property
    @abc.abstractmethod
    def client_outcome_metrics(self) -> List[str]:
        """Denotes metrics of client outcomes while clients are on supervision."""

    @property
    @abc.abstractmethod
    def compliance_action_metrics(self) -> List[str]:
        """Denotes metrics in which we measure supervision actions (like assessments)."""

    @property
    def compliance_action_metric_goals(self) -> List[str]:
        """Denotes goals associated with the configured compliance metrics."""
        return [f"overdue_{metric}_goal" for metric in self.compliance_action_metrics]

    @property
    @abc.abstractmethod
    def compliance_action_metric_goal_thresholds(self) -> Dict[str, float]:
        """Denotes percentage thresholds below which goal is active for the given metric."""

    @property
    @abc.abstractmethod
    def completion_date_label(self) -> str:
        """Denotes preferred terminology for completion in this state."""

    @property
    def revocation_metrics(self) -> List[str]:
        """Denotes metrics that measure revocation."""
        return [
            metric
            for metric in self.client_outcome_metrics
            if metric.endswith("revocations")
        ]

    @property
    def base_metrics_for_display(self) -> List[str]:
        return self.decarceral_actions_metrics + self.client_outcome_metrics

    @property
    def raw_and_percentage_compliance_metrics(self) -> List[str]:
        return list(
            itertools.chain(
                *[
                    [compliance_action_metric, f"{compliance_action_metric}_percent"]
                    for compliance_action_metric in self.compliance_action_metrics
                ],
                *[
                    [goal, f"{goal}_percent"]
                    for goal in self.compliance_action_metric_goals
                ],
            )
        )

    @property
    def average_metrics_for_display(self) -> List[str]:
        """Denotes both state and district averages metrics to display."""
        return list(
            itertools.chain(
                *[
                    [f"{base_metric}_district_average", f"{base_metric}_state_average"]
                    for base_metric in self.client_outcome_metrics
                ]
            )
        )

    @property
    def total_metrics_for_display(self) -> List[str]:
        """Denotes both state and district totals to display."""
        return list(
            itertools.chain(
                *[
                    [f"{base_metric}_district_total", f"{base_metric}_state_total"]
                    for base_metric in self.decarceral_actions_metrics
                ]
            )
        )

    @property
    def metrics_improve_on_increase(self) -> List[str]:
        """Denotes which metrics should indicate improvement on increase."""
        return list(
            itertools.chain(
                *[
                    [
                        decarceral_action_metric,
                        f"{decarceral_action_metric}_district_average",
                        f"{decarceral_action_metric}_state_average",
                    ]
                    for decarceral_action_metric in self.decarceral_actions_metrics
                ]
            )
        )

    @property
    def last_month_metrics(self) -> List[str]:
        """Denotes last month metrics to display."""
        return [
            f"{base_metric}_last_month" for base_metric in self.base_metrics_for_display
        ]

    @property
    def client_fields(self) -> List[str]:
        """Denotes which lists of clients to display in the PO Monthly Report attachment."""
        return (
            [
                f"{base_metric}_clients"
                for base_metric in self.base_metrics_for_display
                if not base_metric.endswith("revocations")
            ]
            + [
                f"{compliance_action_metric}_out_of_date_clients"
                for compliance_action_metric in self.compliance_action_metrics
            ]
            + [REVOCATIONS_CLIENTS]
        )

    @property
    def float_metrics_to_round_to_int(self) -> List[str]:
        """Denotes which metrics need rounding."""
        return [
            *[
                f"{compliance_action_metric}_percent"
                for compliance_action_metric in self.compliance_action_metrics
            ],
            *[f"{goal}_percent" for goal in self.compliance_action_metric_goals],
        ]

    @property
    def singular_or_plural_metrics(self) -> List[str]:
        """Denotes which metrics are to be displayed with titles that are adjusted based on value."""
        return (
            self.decarceral_actions_metrics
            + self.compliance_action_metrics
            + [TOTAL_REVOCATIONS, ABSCONSIONS]
        )

    @property
    def required_recipient_data_fields(self) -> List[str]:
        """Returns all of the required recipient fields for PO Monthly Reports."""
        return (
            self.base_metrics_for_display
            + self.average_metrics_for_display
            + self.total_metrics_for_display
            + self.last_month_metrics
            + self.client_fields
            + self.raw_and_percentage_compliance_metrics
            + REQUIRED_RECIPIENT_DATA_FIELDS
        )
