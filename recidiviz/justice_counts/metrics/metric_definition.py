# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Base class for official Justice Counts metrics."""

import enum
from typing import Dict, List, Optional, Set, Type

import attr

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.utils.constants import REPORTING_FREQUENCY_CONTEXT_KEY
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Datapoint,
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)


class IncludesExcludesSetting(enum.Enum):
    YES = "Yes"
    NO = "No"


class IncludesExcludesSet:
    # Enum that describes all includes / excludes
    # options (i.e PrisonsStaffIncludesExcludes)
    members: Type[enum.Enum]
    # Dictionary that maps includes / excludes enum member to
    # the default IncludesExcludesSetting
    # (i.e {PrisonsStaffIncludesExcludes.STAFF_ON_LEAVE: IncludesExcludesSetting.YES, ...})
    member_to_default_inclusion_setting: Dict[enum.Enum, IncludesExcludesSetting]

    def __init__(
        self,
        members: Type[enum.Enum],
        excluded_set: Optional[Set[enum.Enum]] = None,
    ):
        self.members = members
        self.member_to_default_inclusion_setting = {}
        for member in self.members:
            setting = IncludesExcludesSetting.YES
            if excluded_set is not None and member in excluded_set:
                setting = IncludesExcludesSetting.NO
            self.member_to_default_inclusion_setting[member] = setting


class MetricCategory(enum.Enum):
    CAPACITY_AND_COST = "CAPACITY_AND_COST"
    OPERATIONS_AND_DYNAMICS = "OPERATIONS_AND_DYNAMICS"
    POPULATIONS = "POPULATIONS"
    PUBLIC_SAFETY = "PUBLIC_SAFETY"
    EQUITY = "EQUITY"
    FAIRNESS = "FAIRNESS"

    @property
    def human_readable_string(self) -> str:
        """The name of the metric category

        E.g. 'OPERATIONS_AND_DYNAMICS' --> 'Operations and Dynamics'
        """
        metric_category = self.value
        # Replace underscores with spaces
        # Transform all capitals to titlecase
        # Replace "And" with "and"
        metric_category = (
            metric_category.replace("_", " ").title().replace("And", "and")
        )
        return metric_category.strip()


@attr.define()
class Context:
    """Additional context that the agency is required to report on this metric.
    The `key` should be a unique identifier; `value_type` is the input type,
    `label` should be a human-readable explanation, and `required` indicates if
    this context is required or requested.
    """

    key: ContextKey
    value_type: ValueType
    required: bool
    label: str
    reporting_note: Optional[str] = None
    multiple_choice_options: Optional[Type[enum.Enum]] = None


@attr.define()
class AggregatedDimension:
    """Dimension that this metric should be disaggregated by. For instance, if OffenseType
    is an AggegregatedDimension, then agencies should report a separate datapoint for
    each possible OffenseType.
    """

    dimension: Type[DimensionBase]
    # Whether this disaggregation is requested but not required
    required: bool
    # Maps dimension member to it's 'breakdown description'.
    dimension_to_description: Optional[Dict[DimensionBase, str]] = None
    # Whether the disaggregated values should sum to the total metric value
    should_sum_to_total: bool = False
    # Text displayed as label in frontend
    # If not specified, falls back to DimensionBase.display_name
    display_name: Optional[str] = None
    # Text displayed above aggregated dimension breakdowns.
    helper_text: Optional[str] = None
    # Maps dimension member to IncludesExcludesSet to
    # describes what data is included/excluded in
    # the aggregated dimension values. This information
    # is displayed as toggles in the metric settings page.
    dimension_to_includes_excludes: Optional[
        Dict[DimensionBase, IncludesExcludesSet]
    ] = None

    @property
    def dimension_to_contexts(
        self,
    ) -> Optional[Dict[DimensionBase, List[Context]]]:
        """
        Returns a dictionary of dimension members as keys. The values of the dictionary
        are lists of Contexts. A given dimension member can potentially have 2 contexts:
            - if the dimension member has an includes/excludes, that dimension member
            will have the 'includes excludes description' context. This is used in the UI
            to provide text boxes to describe additional data alements included in the
            agency's metric definition.
            - OTHER dimension members can have the 'additional context' context. This is
             used in the UI to provide additional context text boxes for OTHER dimensions"""
        dim_to_contexts: Dict[DimensionBase, List[Context]] = {}
        for member in self.dimension:  # type: ignore[attr-defined]
            context_lst = []
            if (
                self.dimension_to_includes_excludes is not None
                and self.dimension_to_includes_excludes.get(member) is not None
            ):
                context_lst.append(
                    Context(
                        key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                        value_type=ValueType.TEXT,
                        label="If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                        required=False,
                    )
                )
            if member.name.strip() == "OTHER":
                context_lst.append(
                    Context(
                        key=ContextKey.ADDITIONAL_CONTEXT,
                        value_type=ValueType.TEXT,
                        label="Please describe what data is being included in this breakdown.",
                        required=False,
                    )
                )
            if len(context_lst) > 0:
                dim_to_contexts[member] = context_lst
        if bool(dim_to_contexts) is True:
            return dim_to_contexts
        return None

    def dimension_identifier(self) -> str:
        return self.dimension.dimension_identifier()


@attr.define(frozen=True)
class MetricDefinition:
    """Represents an official Justice Counts metric. An instance
    of this class should be 1:1 with a cell in the Tier 1 chart.
    """

    # Agencies in this system are responsible for reporting this metric
    system: System
    # Metrics are unique by <system, metric_type, aggregated_dimensions>
    metric_type: MetricType
    # Each metric belongs to a particular category
    category: MetricCategory

    # Human-readable name for the metric
    display_name: str
    # Human-readable description of the metric
    description: str
    # How the metric over a time window is reduced to a single point
    measurement_type: MeasurementType
    # How often the metric should be reported
    reporting_frequencies: List[ReportingFrequency]
    # Note to agencies about how to report this metric (i.e. the ideal methodology)
    reporting_note: Optional[str] = None
    # Additional context that the agency is required to report on this metric
    specified_contexts: Optional[List[Context]] = None
    # Dimensions that this metric should be disaggregated by in the reporting
    aggregated_dimensions: Optional[List[AggregatedDimension]] = None
    # If disabled, don't send to the frontend to render
    disabled: bool = False
    # Describes what data is included/excluded in the metrics aggregate value.
    # The IncludesExcludesSet is rendered as toggles in the metric settings page.
    includes_excludes: Optional[IncludesExcludesSet] = None

    @property
    def key(self) -> str:
        """Returns a unique identifier across all Justice Counts metrics.
        Metrics are unique by <system, metric_type>
        """
        return "_".join(
            [
                self.system.value,
                self.metric_type.value,
            ],
        )

    @property
    def contexts(self) -> List[Context]:
        """Returns the list of contexts associated with the metric.
        Appends an additional context to the list of required/requested contexts. Returns
        only a list containing the additional contexts if no contexts are associated with the metric.
        """
        additional_context: List[Context] = []

        if self.includes_excludes is not None:
            additional_context.append(
                Context(
                    key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                    value_type=ValueType.TEXT,
                    label="If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                    required=False,
                )
            )

        return (
            self.specified_contexts + additional_context
            if self.specified_contexts is not None
            else additional_context
        )

    @property
    def reporting_frequency(self) -> ReportingFrequency:
        if len(self.reporting_frequencies) > 1:
            raise ValueError("Multiple reporting frequencies are not yet supported.")
        return self.reporting_frequencies[0]

    def get_reporting_frequency_to_use(
        self, agency_datapoints: List[Datapoint]
    ) -> ReportingFrequency:
        # Returns the reporting frequency based upon what the
        # agency has set in metric settings.

        default_frequency = self.reporting_frequency
        if agency_datapoints is None:
            # If there is no agency datapoint for the agency, return
            # the default frequency
            return default_frequency

        frequency_datapoint_list = list(
            filter(
                lambda d: d.context_key == REPORTING_FREQUENCY_CONTEXT_KEY,
                agency_datapoints,
            )
        )
        if len(list(frequency_datapoint_list)) == 0:
            # If there is no custom reporting frequency datapoint for
            # the agency, return the default frequency
            return default_frequency

        frequency_datapoint = frequency_datapoint_list.pop()
        custom_reporting_frequency = CustomReportingFrequency.from_datapoint(
            frequency_datapoint
        )
        return (
            custom_reporting_frequency.frequency
            if custom_reporting_frequency.frequency is not None
            else default_frequency
        )
