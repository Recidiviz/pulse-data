# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Outliers-related types"""
from enum import Enum
from typing import Any, Dict, List, Optional

import attr
import cattrs

from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)


class MetricOutcome(Enum):
    FAVORABLE = "FAVORABLE"
    ADVERSE = "ADVERSE"


class TargetStatusStrategy(Enum):
    # This is the default TargetStatusStrategy: the threshold for the target status is calculated by target +/- IQR.
    IQR_THRESHOLD = "IQR_THRESHOLD"
    # In some cases, i.e. certain favorable metrics, the target minus the IQR may be <= zero and officers
    # who have zero rates should be highlighted as outliers, instead of using the IQR threshold logic.
    ZERO_RATE = "ZERO_RATE"


class TargetStatus(Enum):
    MET = "MET"
    NEAR = "NEAR"
    FAR = "FAR"


@attr.s(eq=False)
class OutliersMetric:
    # The metric name/id, which should reference the name from an object in aggregated_metric_configurations.py
    # This also corresponds to a column name in an aggregated_metric view
    name: str = attr.ib()

    # Whether the metric outcome is favorable or adverse to the best path of a JII
    outcome_type: MetricOutcome = attr.ib()


@attr.s(eq=False)
class OutliersMetricConfig:
    name: str = attr.ib()

    outcome_type: MetricOutcome = attr.ib()

    # The string used as a display name for a metric, used in email templating
    title_display_name: str = attr.ib()

    # String used for metric in highlights and other running text
    body_display_name: str = attr.ib()

    # Event name corresponding to the metric
    event_name: str = attr.ib()

    @classmethod
    def build_from_metric(
        cls,
        metric: OutliersMetric,
        title_display_name: str,
        body_display_name: str,
        event_name: str,
    ) -> "OutliersMetricConfig":
        return cls(
            metric.name,
            metric.outcome_type,
            title_display_name,
            body_display_name,
            event_name,
        )


@attr.s
class OutliersConfig:
    # List of metrics that are relevant for this state,
    # where each element corresponds to a column name in an aggregated_metrics views
    metrics: List[OutliersMetricConfig] = attr.ib()

    # The string that represents what a state calls its supervision staff member, e.g. "officer" or "agent"
    supervision_officer_label: str = attr.ib()

    # Location exclusions; a unit of analysis mapped to a list of ids to exclude
    unit_of_analysis_to_exclusion: Dict[MetricUnitOfAnalysisType, List[str]] = attr.ib(
        default=None
    )

    # A string representing the filters to apply for the state's supervision officer aggregated metrics
    supervision_officer_aggregated_metric_filters: str = attr.ib(default=None)


@attr.s
class OfficerMetricEntity:
    # The name of the unit of analysis, i.e. full name of a SupervisionOfficer object
    name: str = attr.ib()
    # The current rate for this unit of analysis
    rate: float = attr.ib()
    # Categorizes how the rate for this OfficerMetricEntity compares to the target value
    target_status: TargetStatus = attr.ib()
    # The rate for the prior YEAR period for this unit of analysis;
    # None if there is no metric rate for the previous period
    prev_rate: Optional[float] = attr.ib()
    # The external_id of this OfficerMetricEntity's supervisor
    supervisor_external_id: str = attr.ib()
    # The target status for the previous period
    prev_target_status: Optional[TargetStatus] = attr.ib(default=None)


@attr.s
class MetricContext:
    # Unless otherwise specified, the target is the state average for the current period
    target: float = attr.ib()
    # All units of analysis for a given state and metric for the current period
    entities: List[OfficerMetricEntity] = attr.ib()
    # All units of analysis for a given state and metric for the previous period
    prev_period_entities: List[OfficerMetricEntity] = attr.ib()
    # Describes how the TargetStatus is calculated (see the Enum definition)
    target_status_strategy: TargetStatusStrategy = attr.ib(
        default=TargetStatusStrategy.IQR_THRESHOLD
    )


@attr.s
class OutlierMetricInfo:
    # The Outliers metric the information corresponds to
    metric: OutliersMetricConfig = attr.ib()
    # Unless otherwise specified, the target is the state average for the current period
    target: float = attr.ib()
    # Maps target status to a list of metric rates for all officers not included in highlighted_officers
    other_officers: Dict[TargetStatus, List[float]] = attr.ib()
    # Officers for a specific supervisor who have the "FAR" status for a given metric
    highlighted_officers: List[OfficerMetricEntity] = attr.ib()
    # Describes how the TargetStatus is calculated (see the Enum definition)
    target_status_strategy: TargetStatusStrategy = attr.ib(
        default=TargetStatusStrategy.IQR_THRESHOLD
    )


@attr.s
class OfficerSupervisorReportData:
    # List of OutlierMetricInfo objects, representing metrics with outliers for this supervisor
    metrics: List[OutlierMetricInfo] = attr.ib()
    # List of OutliersMetric objects for metrics where there are no outliers
    metrics_without_outliers: List[OutliersMetricConfig] = attr.ib()
    recipient_email_address: str = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


######################################
# Data classes for the Supervision District Manager email
######################################
@attr.s
class SupervisionOfficerSupervisorMetricInfo:
    # The Outliers metric the information corresponds to
    metric: OutliersMetricConfig = attr.ib()
    # The percentage of this supervisor's officers with "FAR" target status for a specific metric in the current period
    officers_far_pct: float = attr.ib()
    # The percentage of this supervisor's officers with "FAR" target status for a specific metric in the previous period
    prev_officers_far_pct: float = attr.ib()
    # Maps target status to list of officer rates with the respective status for the current period for all officers
    # that have the same supervisor
    officer_rates: Dict[TargetStatus, List[float]] = attr.ib()


@attr.s
class SupervisionOfficerSupervisorMetricEntity:
    # The full name of the supervision officer supervisor
    supervisor_name: str = attr.ib()
    # Maps the metric id to the corresponding SupervisionOfficerSupervisorMetricInfo object
    metrics: List[SupervisionOfficerSupervisorMetricInfo] = attr.ib()


@attr.s
class SupervisionDistrictReportData:
    # The name of the recipient, e.g. a SupervisionDistrictManager
    recipient_name: str = attr.ib()
    # The recipient's email address
    recipient_email: str = attr.ib()
    # The entities for this supervision district
    entities: List[SupervisionOfficerSupervisorMetricEntity] = attr.ib()
    # The copy for how to refer to a supervision officer
    officer_label: str = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)
