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

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import attr
import cattrs
from cattrs.gen import make_dict_unstructure_fn, override

from recidiviz.aggregated_metrics.models.aggregated_metric import EventCountMetric
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import person_name_case
from recidiviz.observations.event_type import EventType
from recidiviz.persistence.database.schema.insights.schema import (
    ACTION_STRATEGIES_DEFAULT_COPY,
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


class OutliersMetricValueType(Enum):
    COUNT = "COUNT"
    AVERAGE = "AVERAGE"
    # Implies that the value is divided by the avg population defined in the metric config
    RATE = "RATE"
    PROPORTION = "PROPORTION"
    # Implies the value is 1.0 or 0.0
    BOOLEAN = "BOOLEAN"


class ConfigurationStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


def _optional_name_converter(name: Optional[str]) -> Optional[str]:
    return None if name is None else person_name_case(name)


@attr.s(frozen=True)
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


@attr.s(frozen=True)
class CaseloadCategory:
    # The caseload category as it appears in the data
    id: str = attr.ib()

    # The caseload category as it should appear in the UI
    display_name: str = attr.ib()


@attr.s(eq=False, frozen=True)
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
        return self.aggregated_metric.get_observation_conditions_string_no_newline(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )


@attr.s(frozen=True)
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


@attr.s(frozen=True)
class OutliersVitalsMetricConfig:
    """
    Represents all information needed for a single vitals metric in the Outliers products
    """

    # The id of the metric (e.g. "timely_risk_assessment" | "timely_contact")
    metric_id: str = attr.ib()

    # The display name of the metric
    title_display_name: str = attr.ib()

    # A shorter display name
    body_display_name: str = attr.ib()


@attr.s(frozen=True, kw_only=True)
class OutliersMetricConfig:
    """
    Represents all information needed for a single metric in the Outliers products
    """

    # The state associated with this metric configuration. This is nulled out in API
    # responses because it is unused in the frontend.
    #
    # TODO(#16661): This state_code value represents the state_code value in
    #  the *data*. This this distinction is only meaningful for US_ID/US_IX and we only
    #  null out this field in API responses because the frontend has no concept of US_IX
    #  and shouldn't ever see US_IX. We could start sending this value to the frontend
    #  once
    state_code: StateCode | None = attr.ib(
        validator=attr_validators.is_opt(StateCode), default=None
    )

    # The name of the underlying EventCountMetric for this metric
    name: str = attr.ib()

    # The EventType for observations referenced by the underlying underlying
    # EventCountMetric for this metric. This is nulled out in API responses because it
    # is unused in the frontend.
    event_observation_type: EventType | None = attr.ib(
        validator=attr_validators.is_opt(EventType), default=None
    )

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
    description_markdown: str = attr.ib()

    # The query fragment to use to filter analyst_data.person_events for this metric's
    # events. This is nulled out in API responses because it is unused in the frontend.
    # because it is unused in the frontend.
    metric_event_conditions_string: str | None = attr.ib(default=None)

    # The top percent (as an integer) of officers to highlight for this metric, if applicable;
    # i.e. top_x_pct = 10 translates to highlighting officers that are in the top 10%
    top_x_pct: int | None = attr.ib(default=None)

    # Identifies if this is an absconsion related metric
    is_absconsion_metric: bool = attr.ib(default=False)

    # Helper text to display with list of metric events
    list_table_text: str | None = attr.ib(default=None)

    # Outcomes metric rate denominator
    rate_denominator: str = attr.ib(default="avg_daily_population")

    # Only show this metric if the feature variant is enabled for this user
    feature_variant: str | None = attr.ib(
        default=None,
        validator=attr_validators.is_not_set_along_with("inverse_feature_variant"),
    )

    # Only show this metric if the feature variant is disabled for this user
    inverse_feature_variant: str | None = attr.ib(
        default=None, validator=attr_validators.is_not_set_along_with("feature_variant")
    )

    @classmethod
    def build_from_metric(
        cls,
        *,
        state_code: StateCode,
        metric: OutliersMetric,
        title_display_name: str,
        body_display_name: str,
        event_name: str,
        event_name_singular: str,
        event_name_past_tense: str,
        # TODO(#27455): Remove the default value when the copy is ready for PA and IX
        description_markdown: str = "",
        top_x_pct: int | None = None,
        is_absconsion_metric: bool = False,
        list_table_text: str | None = None,
        rate_denominator: str = "avg_daily_population",
        feature_variant: str | None = None,
        inverse_feature_variant: str | None = None,
    ) -> "OutliersMetricConfig":
        return cls(
            state_code=state_code,
            name=metric.name,
            outcome_type=metric.outcome_type,
            event_observation_type=metric.aggregated_metric.event_type,
            title_display_name=title_display_name,
            body_display_name=body_display_name,
            event_name=event_name,
            event_name_singular=event_name_singular,
            event_name_past_tense=event_name_past_tense,
            description_markdown=description_markdown,
            metric_event_conditions_string=metric.metric_event_conditions_string,
            top_x_pct=top_x_pct,
            is_absconsion_metric=is_absconsion_metric,
            list_table_text=list_table_text,
            rate_denominator=rate_denominator,
            feature_variant=feature_variant,
            inverse_feature_variant=inverse_feature_variant,
        )


@attr.s(frozen=True)
class OutliersClientEventConfig:
    name: str = attr.ib()

    display_name: str = attr.ib()

    @classmethod
    def build(
        cls, event: OutliersClientEvent, display_name: str
    ) -> "OutliersClientEventConfig":
        return cls(event.name, display_name)


@attr.s(frozen=True)
class OutliersBackendConfig:
    """
    Information for a state's Outliers configuration represented as structured data.
    """

    # List of metrics that are relevant for this state,
    # where each element corresponds to a column name in an aggregated_metrics views
    # NOTE: if you are removing an existing metric, i.e. replacing it or deleting it,
    # add the metric to the deprecated_metrics field and create a ticket to remove it
    # once the metric changes are propogated through to production.
    metrics: List[OutliersMetricConfig] = attr.ib()

    # The vitals metrics relevant to this state
    vitals_metrics: List[OutliersVitalsMetricConfig] = attr.ib(default=[])

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

    # The caseload category type to use for disaggregating caseload types in the state
    # TODO(#32104): Move this to the admin panel
    primary_category_type: InsightsCaseloadCategoryType = attr.ib(
        default=InsightsCaseloadCategoryType.ALL
    )

    # The available specialized caseload categories for this state, plus the display name for each
    # category, broken down by category type.
    # TODO(#32104): Move this to the admin panel
    available_specialized_caseload_categories: dict[
        InsightsCaseloadCategoryType, list[CaseloadCategory]
    ] = attr.ib(default={})

    def to_json(self) -> Dict[str, Any]:
        c = cattrs.Converter()

        # Omit the rate_denominator, state_code, event_observation_type, and
        # metric_event_conditions_string fields since they are only used in BQ views.
        metrics_unst_hook = make_dict_unstructure_fn(
            OutliersMetricConfig,
            c,
            metric_event_conditions_string=override(omit=True),
            event_observation_type=override(omit=True),
            state_code=override(omit=True),
            rate_denominator=override(omit=True),
            feature_variant=override(omit=True),
            inverse_feature_variant=override(omit=True),
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


@attr.s(frozen=True)
class OfficerOutlierStatus:
    # The officer's external id
    external_id: str = attr.ib()
    # The current rate for this unit of analysis
    rate: float = attr.ib()
    # Categorizes how the rate for this OfficerMetricEntity compares to the target value
    target_status: TargetStatus = attr.ib()
    # The rate for the prior YEAR period for this unit of analysis;
    # None if there is no metric rate for the previous period
    prev_rate: Optional[float] = attr.ib()
    # The target status for the previous period
    prev_target_status: Optional[TargetStatus] = attr.ib(default=None)


@attr.s(frozen=True)
class OfficerMetricEntity:
    # The name of the unit of analysis, i.e. full name of a SupervisionOfficer object
    name: PersonName = attr.ib()
    # The officer's external id
    external_id: str = attr.ib()
    # The current rate for this unit of analysis
    rate: float = attr.ib()
    # Categorizes how the rate for this OfficerMetricEntity compares to the target value
    target_status: TargetStatus = attr.ib()
    # The rate for the prior YEAR period for this unit of analysis;
    # None if there is no metric rate for the previous period
    prev_rate: Optional[float] = attr.ib()
    # The external_id of this OfficerMetricEntity's supervisor
    supervisor_external_id: str = attr.ib()
    # The external_id of this OfficerMetricEntity's supervisor
    supervisor_external_ids: List[str] = attr.ib()
    # The supervision district the officer is assigned to
    supervision_district: str = attr.ib()
    # The target status for the previous period
    prev_target_status: Optional[TargetStatus] = attr.ib(default=None)


@attr.s(frozen=True)
class MetricContext:
    # Unless otherwise specified, the target is the state average for the current period
    target: float = attr.ib()
    # All units of analysis for a given state and metric for the current period
    entities: List[OfficerOutlierStatus] = attr.ib()
    # Describes how the TargetStatus is calculated (see the Enum definition)
    target_status_strategy: TargetStatusStrategy = attr.ib(
        default=TargetStatusStrategy.IQR_THRESHOLD
    )


@attr.s(frozen=True)
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


@attr.s(frozen=True)
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
            OutliersMetricConfig,
            c,
            metric_event_conditions_string=override(omit=True),
            event_observation_type=override(omit=True),
            state_code=override(omit=True),
        )
        c.register_unstructure_hook(OutliersMetricConfig, metrics_unst_hook)

        return c.unstructure(self)


@attr.s(frozen=True)
class SupervisionOfficerEntity:
    """Represents a supervision officer for the insights product. The officer may or
    may not have been included in outcomes calculations and may or may not be an
    outlier themselves."""

    # The full name of the officer
    full_name: PersonName = attr.ib()
    # The officer's external id
    external_id: str = attr.ib()
    # The officer's pseudonymized id
    pseudonymized_id: str = attr.ib()
    # The officer's supervisor's external id
    supervisor_external_id: str = attr.ib()
    # The officer's supervisor's external ids
    supervisor_external_ids: List[str] = attr.ib()
    # The district of the officer
    district: str = attr.ib()
    # The officer's email
    email: str = attr.ib()
    # Whether this officer is currently included in outcomes calculations
    include_in_outcomes: bool = attr.ib()
    # The officer's avg caseload size in the latest period
    avg_daily_population: Optional[float] = attr.ib(default=None)
    # earliest date that this officer was assigned a caseload
    earliest_person_assignment_date: Optional[date] = attr.ib(default=None)
    # A list of zero grant opportunity types for this officer.
    zero_grant_opportunities: Optional[List[str]] = attr.ib(default=None)
    # The date of the officer's last login
    latest_login_date: Optional[date] = attr.ib(default=None)

    def to_json(self) -> Dict[str, Any]:
        result = cattrs.unstructure(self)
        if result.get("latest_login_date"):
            result["latest_login_date"] = result["latest_login_date"].isoformat()
        return result


@attr.s(frozen=True)
class SupervisionOfficerOutcomes:
    """Represents outcomes information for an officer who was included in benchmark
    and outlier status calculations."""

    # The officer's external id
    external_id: str = attr.ib()
    # The officer's pseudonymized id
    pseudonymized_id: str = attr.ib()
    # List of objects that represent what metrics the officer is an Outlier for
    # If the list is empty, then the officer is not an Outlier on any metric.
    outlier_metrics: list = attr.ib()
    # List of objects that represent what metrics the officer is in the top x% for the latest period for,
    # where x can be specified on the OutliersMetricConfig in a state's OutliersBackendConfig
    top_x_pct_metrics: list = attr.ib()
    # The caseload category this officer is part of
    caseload_category: str = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s(auto_attribs=True, frozen=True, order=True)
class SupervisionOfficerVitalsEntity:
    """
    Represents the vitals metrics for an officer for a single metric_id
    """

    officer_pseudonymized_id: str
    metric_value: float
    # Despite the name, this field represents the difference of the metric value between
    # previous_metric_date and metric_date; those dates might not be 30 days apart.
    # When there is no previous metric date, the delta is 0.
    metric_30d_delta: float
    metric_date: str
    previous_metric_date: Optional[str]

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s(auto_attribs=True, frozen=True, order=True)
class VitalsMetric:
    """Contains the vitals metrics for a metric_id for one or more officers."""

    metric_id: str
    vitals_metrics: List[SupervisionOfficerVitalsEntity] = attr.ib(factory=list)

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)

    def add_officer_vitals_entity(
        self,
        officer_pseudonymized_id: str,
        metric_value: float,
        metric_30d_delta: int,
        end_date: date,
        previous_end_date: date,
    ) -> None:
        """Adds a SupervisionOfficerVitalsEntity to the vitals_metrics list using provided parameters."""
        # Create a new SupervisionOfficerVitalsEntity instance
        officer_vitals = SupervisionOfficerVitalsEntity(
            officer_pseudonymized_id=officer_pseudonymized_id,
            metric_value=metric_value,
            metric_30d_delta=metric_30d_delta,
            metric_date=str(end_date),
            previous_metric_date=str(previous_end_date) if previous_end_date else None,
        )

        # Directly append to the existing list
        self.vitals_metrics.append(officer_vitals)


@attr.s(frozen=True)
class SupervisionOfficerSupervisorEntity:
    # The full name of the supervisor
    full_name: PersonName = attr.ib()
    # The supervisor's external id
    external_id: str = attr.ib()
    # The officer's pseudonymized id
    pseudonymized_id: str = attr.ib()
    email: str = attr.ib()
    # Whether the supervisor has outliers in the latest period
    has_outliers: bool = attr.ib()
    # Supervision location to show on the supervisor list page
    supervision_location_for_list_page: Optional[str] = attr.ib()
    # Supervision location to show on the individual supervisor page
    supervision_location_for_supervisor_page: Optional[str] = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


class UserRole(str, Enum):
    """The role of the user based on their entity."""

    SUPERVISION_OFFICER = "supervision_officer"
    SUPERVISION_OFFICER_SUPERVISOR = "supervision_officer_supervisor"


@attr.s(frozen=True)
class UserInfo:
    entity: Optional[
        Union[SupervisionOfficerSupervisorEntity, SupervisionOfficerEntity]
    ] = attr.ib()
    role: Optional[UserRole] = attr.ib()
    has_seen_onboarding: bool = attr.ib()
    has_dismissed_data_unavailable_note: bool = attr.ib()
    has_dismissed_rate_over_100_percent_note: bool = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        result = cattrs.unstructure(self)
        if result.get("entity") and result.get("entity").get("latest_login_date"):
            result["entity"]["latest_login_date"] = result["entity"][
                "latest_login_date"
            ].isoformat()
        return result


@attr.s(frozen=True)
class OutliersProductConfiguration:
    """
    Class that contains all of the information that is configured for a state
    and needed by products externally. This is a combination of what's configured for
    a state via the OutliersBackendConfig and the Configuration entity defined in
    outliers/schema.py.
    """

    # Who authored the copy configuration
    updated_by: str = attr.ib()

    # When the copy configuration was implemented
    updated_at: datetime = attr.ib()

    # The feature variant that gates this configuration (if necessary)
    feature_variant: Optional[str] = attr.ib()

    # where each element corresponds to a column name in an aggregated_metrics views
    metrics: List[OutliersMetricConfig] = attr.ib()

    # URL that methodology/FAQ links can be pointed to
    learn_more_url: str = attr.ib()

    # The string that represents what a state calls its supervision staff member, e.g. "officer" or "agent"
    supervision_officer_label: str = attr.ib()

    # The string that represents what a state calls a location-based group of offices, e.g. "district" or "region"
    supervision_district_label: str = attr.ib()

    # The string that represents what a state calls a group of supervision officers, e.g. "unit"
    supervision_unit_label: str = attr.ib()

    # The string that represents what a state calls a supervisor, e.g. "supervisor"
    supervision_supervisor_label: str = attr.ib()

    # The string that represents what a state calls someone who manages supervision supervisors, e.g. "district director"
    supervision_district_manager_label: str = attr.ib()

    # The string that represents what a state calls a justice-impacted individual on supervision, e.g. "client"
    supervision_jii_label: str = attr.ib()

    # The string shown when supervisor is missing outlier officers
    supervisor_has_no_outlier_officers_label: str = attr.ib()

    # The string shown when officer has no outlier metrics
    officer_has_no_outlier_metrics_label: str = attr.ib()

    # The string shown when supervisor has no officers with clients eligible for opportunities
    supervisor_has_no_officers_with_eligible_clients_label: str = attr.ib()

    # The string shown when officer has no clients eligible
    officer_has_no_eligible_clients_label: str = attr.ib()

    # The string that goes in "None of the X on Y's unit ______. Keep checking back" when there are no outliers
    # TODO(#27414): Allow for template strings in copy and replace this with a template that has more of the string.
    none_are_outliers_label: str = attr.ib(default="")

    # The string that describes a metric that is worse than the statewide rate
    worse_than_rate_label: str = attr.ib(default="")

    # A description of why some officers may be excluded from the list.
    exclusion_reason_description: str = attr.ib(default="")

    # The string that describes a metric that is slightly worse than the statewide rate
    slightly_worse_than_rate_label: str = attr.ib(
        default="Slightly worse than statewide rate"
    )

    # The string that describes a metric that is at or below the statewide rate
    at_or_below_rate_label: str = attr.ib(default="At or below statewide rate")

    # The string that describes a metric that is at or ABOVE the statewide rate
    at_or_above_rate_label: str = attr.ib(default="At or above statewide rate")

    # Mapping of client event types that are relevant for this state to a config with relevant info
    client_events: List[OutliersClientEventConfig] = attr.ib(default=[])

    # The category type to use for disaggregating caseload types in the state
    primary_category_type: InsightsCaseloadCategoryType = attr.ib(
        default=InsightsCaseloadCategoryType.ALL
    )

    # The available specialized caseload categories for this state, plus the display name for each
    # category, broken down by category type.
    caseload_categories: list[CaseloadCategory] = attr.ib(default=[])

    # The string that is in the "Outliers" hover tooltip explaining what an outlier is
    outliers_hover: str = attr.ib(
        default="Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate."
    )

    # The string that represents what a state calls an absconder
    absconder_label: str = attr.ib(default="absconder")

    action_strategy_copy: Dict = attr.ib(default=ACTION_STRATEGIES_DEFAULT_COPY)

    # The string that represents the URL explaining vitals metrics
    vitals_metrics_methodology_url: str = attr.ib(default="")

    vitals_metrics: List[OutliersVitalsMetricConfig] = attr.ib(default=[])

    def to_json(self) -> Dict[str, Any]:
        c = cattrs.Converter()

        # Omit the rate_denominator, state_code, event_observation_type, and
        # metric_event_conditions_string fields since they are only used in BQ views.
        metrics_unst_hook = make_dict_unstructure_fn(
            OutliersMetricConfig,
            c,
            metric_event_conditions_string=override(omit=True),
            event_observation_type=override(omit=True),
            state_code=override(omit=True),
            rate_denominator=override(omit=True),
            feature_variant=override(omit=True),
            inverse_feature_variant=override(omit=True),
        )
        c.register_unstructure_hook(OutliersMetricConfig, metrics_unst_hook)

        return c.unstructure(self)


class ActionStrategyType(Enum):
    ACTION_STRATEGY_OUTLIER = "ACTION_STRATEGY_OUTLIER"
    ACTION_STRATEGY_OUTLIER_3_MONTHS = "ACTION_STRATEGY_OUTLIER_3_MONTHS"
    ACTION_STRATEGY_OUTLIER_ABSCONSION = "ACTION_STRATEGY_OUTLIER_ABSCONSION"
    ACTION_STRATEGY_OUTLIER_NEW_OFFICER = "ACTION_STRATEGY_OUTLIER_NEW_OFFICER"
    ACTION_STRATEGY_60_PERC_OUTLIERS = "ACTION_STRATEGY_60_PERC_OUTLIERS"


@attr.s(frozen=True)
class ActionStrategySurfacedEvent:
    # The state code of the user
    state_code: str = attr.ib()
    # The pseudonymized id of the user (supervisor)
    user_pseudonymized_id: str = attr.ib()
    # The officer's pseudonymized id
    officer_pseudonymized_id: Optional[str] = attr.ib()
    # The action strategy enum value
    action_strategy: str = attr.ib()
    # Timestamp
    timestamp: date = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.frozen
class IntercomTicket:
    """Represents an Intercom ticket request schema."""

    # The ID of the type of ticket you want to create
    ticket_type_id: int
    # The email you have defined for the contact who is being added as a participant. If a contact with this email does not exist, one will be created.
    email: str
    # Title of the ticket
    default_title: str
    # Description for the ticket
    default_description: str

    def to_dict(self) -> Dict[str, Any]:
        """Converts an object into the intercom format"""
        return {
            "ticket_type_id": self.ticket_type_id,
            "contacts": [{"email": self.email}],
            "ticket_attributes": {
                "_default_title_": self.default_title,
                "_default_description_": self.default_description,
            },
        }

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.frozen
class IntercomTicketResponse:
    """Represents an Intercom ticket response schema."""

    # The id for the created ticket
    id: str

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


class RosterChangeType(str, Enum):
    ADD = "ADD"
    REMOVE = "REMOVE"


@attr.s(auto_attribs=True, frozen=True)
class RosterChangeRequestSchema:
    """Schema that represents a request to make a roster change."""

    affected_officers_external_ids: List[str]
    request_change_type: RosterChangeType
    request_note: str
    requester_name: str

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s(auto_attribs=True, frozen=True)
class RosterChangeRequestResponseSchema:
    """Schema that represents the response from the RosterTicketService"""

    # The id for the created ticket
    id: str
    # Email for the client
    email: str

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)
