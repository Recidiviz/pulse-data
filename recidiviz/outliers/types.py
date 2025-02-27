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
from cattrs.gen import make_dict_unstructure_fn, override

from recidiviz.aggregated_metrics.models.aggregated_metric import EventCountMetric
from recidiviz.common.str_field_utils import person_name_case


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


class OutliersMetricValueType(Enum):
    COUNT = "COUNT"
    AVERAGE = "AVERAGE"
    # Implies that the value is divided by the avg_daily_population
    RATE = "RATE"


class ConfigurationStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


def _optional_name_converter(name: Optional[str]) -> Optional[str]:
    return None if name is None else person_name_case(name)


@attr.s
class PersonName:
    """Components of a person's name represented as structured data."""

    given_names: str = attr.ib(converter=person_name_case)
    surname: str = attr.ib(converter=person_name_case)
    middle_names: Optional[str] = attr.ib(
        default=None, converter=_optional_name_converter
    )
    name_suffix: Optional[str] = attr.ib(
        default=None, converter=_optional_name_converter
    )

    @property
    def formatted_first_last(self) -> str:
        return f"{self.given_names} {self.surname}"


@attr.s(eq=False)
class OutliersMetric:
    # The aggregated metric, which is an object from aggregated_metric_configurations.py
    aggregated_metric: EventCountMetric = attr.ib()

    # Whether the metric outcome is favorable or adverse to the best path of a JII
    outcome_type: MetricOutcome = attr.ib()

    @property
    def name(self) -> str:
        """
        The metric name/id, which should reference the name from an object in aggregated_metric_configurations.py
        This also corresponds to a column name in an aggregated_metric view
        """
        return self.aggregated_metric.name

    @property
    def metric_event_conditions_string(self) -> str:
        """
        The query fragment to use to filter analyst_data.person_events for this metric's events
        """
        return self.aggregated_metric.get_metric_conditions_string_no_newline()


@attr.s
class OutliersClientEvent:
    # The aggregated metric, which is an object from aggregated_metric_configurations.py
    aggregated_metric: EventCountMetric = attr.ib()

    @property
    def name(self) -> str:
        """
        The metric name/id, which should reference the name from an object in aggregated_metric_configurations.py
        This also corresponds to a column name in an aggregated_metric view
        """
        return self.aggregated_metric.name


@attr.s(eq=False)
class OutliersMetricConfig:
    """
    Represents all information needed for a single metric in the Outliers products
    """

    name: str = attr.ib()

    outcome_type: MetricOutcome = attr.ib()

    # The string used as a display name for a metric, used in email templating
    title_display_name: str = attr.ib()

    # String used for metric in highlights and other running text
    body_display_name: str = attr.ib()

    # Event name in the plural form corresponding to the metric
    event_name: str = attr.ib()

    # Event name in the singular form corresponding to the metric
    event_name_singular: str = attr.ib()

    # Event name in the past participle form corresponding to the metric, to be put into sentences
    # of the form "the number of JII who {event_name_past_tense}"
    event_name_past_tense: str = attr.ib()

    # A description of how the metric for this event type is calculated, formatted in markdown and
    # displayed when a user clicks on the info icon next to the metric.
    description_markdown: str | None = attr.ib(default=None)

    # The query fragment to use to filter analyst_data.person_events for this metric's events
    metric_event_conditions_string: str = attr.ib(default=None)

    @classmethod
    def build_from_metric(
        cls,
        metric: OutliersMetric,
        title_display_name: str,
        body_display_name: str,
        event_name: str,
        event_name_singular: str,
        event_name_past_tense: str,
        description_markdown: str | None = None,
    ) -> "OutliersMetricConfig":
        return cls(
            metric.name,
            metric.outcome_type,
            title_display_name,
            body_display_name,
            event_name,
            event_name_singular,
            event_name_past_tense,
            description_markdown,
            metric.metric_event_conditions_string,
        )


@attr.s
class OutliersClientEventConfig:
    name: str = attr.ib()

    display_name: str = attr.ib()

    @classmethod
    def build(
        cls, event: OutliersClientEvent, display_name: str
    ) -> "OutliersClientEventConfig":
        return cls(event.name, display_name)


@attr.s
class OutliersBackendConfig:
    """
    Information for a state's Outliers configuration represented as structured data.
    TODO(#27075): Remove copy-related fields once they are being read from the OutliersConfig database entity
    """

    # List of metrics that are relevant for this state,
    # where each element corresponds to a column name in an aggregated_metrics views
    # NOTE: if you are removing an existing metric, i.e. replacing it or deleting it,
    # add the metric to the deprecated_metrics field and create a ticket to remove it
    # once the metric changes are propogated through to production.
    metrics: List[OutliersMetricConfig] = attr.ib()

    # URL that methodology/FAQ links can be pointed to
    learn_more_url: str = attr.ib()

    # The string that represents what a state calls its supervision staff member, e.g. "officer" or "agent"
    supervision_officer_label: str = attr.ib(default="officer")

    # The string that represents what a state calls a location-based group of offices, e.g. "district" or "region"
    supervision_district_label: str = attr.ib(default="district")

    # The string that represents what a state calls a group of supervision officers, e.g. "unit"
    supervision_unit_label: str = attr.ib(default="unit")

    # The string that represents what a state calls a supervisor, e.g. "supervisor"
    supervision_supervisor_label: str = attr.ib(default="supervisor")

    # The string that represents what a state calls someone who manages supervision supervisors, e.g. "district director"
    supervision_district_manager_label: str = attr.ib(default="district director")

    # The string that represents what a state calls a justice-impacted individual on supervision, e.g. "client"
    supervision_jii_label: str = attr.ib(default="client")

    # The string that goes in "None of the X on Y's unit ______. Keep checking back" when there are no outliers
    # TODO(#27414): Allow for template strings in copy and replace this with a template that has more of the string.
    none_are_outliers_label: str = attr.ib(default="are outliers")

    # The string that describes a metric that is far worse than the statewide rate
    worse_than_rate_label: str = attr.ib(default="Far worse than statewide rate")

    # The string that describes a metric that is slightly worse than the statewide rate
    slightly_worse_than_rate_label: str = attr.ib(
        default="Slightly worse than statewide rate"
    )

    # The string that describes a metric that is at or below the statewide rate
    at_or_below_rate_label: str = attr.ib(default="At or below statewide rate")

    # A description of why some officers may be excluded from the list.
    exclusion_reason_description: str | None = attr.ib(default=None)

    # Mapping of client event types that are relevant for this state to a config with relevant info
    client_events: List[OutliersClientEventConfig] = attr.ib(default=[])

    # String containing exclusions that should be applied to the supervision staff product views.
    supervision_staff_exclusions: str = attr.ib(default=None)

    # A string representing the filters to apply for the state's supervision officer aggregated metrics
    supervision_officer_metric_exclusions: str = attr.ib(default=None)

    # List of metrics that were previously configured. This list is maintained in order
    # to prevent completely omitting a metric from the webtool when metrics are being
    # changed or replaced. Context here: https://paper.dropbox.com/doc/Design-Doc-Outliers-Metric-Race-Condition--CIFSxbs7iv0jEUQqZZHsNwqCAg-Mpf1Bq5VbYyxqJyW0jHhb
    deprecated_metrics: List[OutliersMetricConfig] = attr.ib(default=[])

    def to_json(self) -> Dict[str, Any]:
        c = cattrs.Converter()

        # Omit the conditions string since this is only used in BQ views.
        metrics_unst_hook = make_dict_unstructure_fn(
            OutliersMetricConfig, c, metric_event_conditions_string=override(omit=True)
        )
        c.register_unstructure_hook(OutliersMetricConfig, metrics_unst_hook)

        # Omit the exclusions since they are only for internal (backend) use.
        unst_hook = make_dict_unstructure_fn(
            OutliersBackendConfig,
            c,
            supervision_staff_exclusions=override(omit=True),
            supervision_officer_metric_exclusions=override(omit=True),
        )
        c.register_unstructure_hook(OutliersBackendConfig, unst_hook)
        return c.unstructure(self)

    def get_all_metrics(self) -> List[OutliersMetricConfig]:
        return self.metrics + self.deprecated_metrics


@attr.s
class OfficerMetricEntity:
    # The name of the unit of analysis, i.e. full name of a SupervisionOfficer object
    name: PersonName = attr.ib()
    # The current rate for this unit of analysis
    rate: float = attr.ib()
    # Categorizes how the rate for this OfficerMetricEntity compares to the target value
    target_status: TargetStatus = attr.ib()
    # The rate for the prior YEAR period for this unit of analysis;
    # None if there is no metric rate for the previous period
    prev_rate: Optional[float] = attr.ib()
    # The external_id of this OfficerMetricEntity's supervisor
    supervisor_external_id: str = attr.ib()
    # The supervision district the officer is assigned to
    supervision_district: str = attr.ib()
    # The target status for the previous period
    prev_target_status: Optional[TargetStatus] = attr.ib(default=None)


@attr.s
class MetricContext:
    # Unless otherwise specified, the target is the state average for the current period
    target: float = attr.ib()
    # All units of analysis for a given state and metric for the current period
    entities: List[OfficerMetricEntity] = attr.ib()
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

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s
class OfficerSupervisorReportData:
    # List of OutlierMetricInfo objects, representing metrics with outliers for this supervisor
    metrics: List[OutlierMetricInfo] = attr.ib()
    # List of OutliersMetric objects for metrics where there are no outliers
    metrics_without_outliers: List[OutliersMetricConfig] = attr.ib()
    recipient_email_address: str = attr.ib()
    # Additional emails the report data should be CC'd to
    additional_recipients: List[str] = attr.ib()

    def to_dict(self) -> Dict[str, Any]:
        c = cattrs.Converter()

        # Omit the conditions string since this is only used in BQ views.
        metrics_unst_hook = make_dict_unstructure_fn(
            OutliersMetricConfig, c, metric_event_conditions_string=override(omit=True)
        )
        c.register_unstructure_hook(OutliersMetricConfig, metrics_unst_hook)

        return c.unstructure(self)


@attr.s
class SupervisionOfficerEntity:
    # The full name of the officer
    full_name: PersonName = attr.ib()
    # The officer's external id
    external_id: str = attr.ib()
    # The officer's pseudonymized id
    pseudonymized_id: str = attr.ib()
    # The officer's supervisor's external id
    supervisor_external_id: str = attr.ib()
    # The district the officer
    district: str = attr.ib()
    # The officer's caseload type in the latest period
    caseload_type: Optional[str] = attr.ib()
    # List of objects that represent what metrics the officer is an Outlier for
    # If the list is empty, then the officer is not an Outlier on any metric.
    outlier_metrics: list = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s
class SupervisionOfficerSupervisorEntity:
    # The full name of the supervisor
    full_name: PersonName = attr.ib()
    # The supervisor's external id
    external_id: str = attr.ib()
    # The officer's pseudonymized id
    pseudonymized_id: str = attr.ib()
    # The district the supervisor's in
    supervision_district: Optional[str] = attr.ib()
    email: str = attr.ib()
    # Whether the supervisor has outliers in the latest period
    has_outliers: bool = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s
class UserInfo:
    entity: Optional[SupervisionOfficerSupervisorEntity] = attr.ib()
    role: Optional[str] = attr.ib()
    has_seen_onboarding: bool = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)
