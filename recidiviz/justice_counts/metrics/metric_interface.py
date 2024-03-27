# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.p
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Base class for the reported value(s) for a Justice Counts metric."""

import enum
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple, Type, TypeVar

import attr

from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_context_data import MetricContextData
from recidiviz.justice_counts.metrics.metric_definition import (
    IncludesExcludesSetting,
    MetricDefinition,
)
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS_BY_SYSTEM,
    get_supervision_subsystem_metric_definition,
)
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import DatapointGetRequestEntryPoint
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
)

MetricInterfaceT = TypeVar("MetricInterfaceT", bound="MetricInterface")


@attr.define()
class MetricInterface:
    """Represents an agency's reported values for a Justice Counts metric.
    If the agency has not filled out a field yet, the value will be None.
    """

    # The key of the metric (i.e. `MetricDefinition.key`) that is being reported
    key: str

    # The value entered for the metric. If the metric has breakdowns, this is the
    # total, aggregate value summed across all dimensions.
    value: Optional[float] = attr.field(default=None)

    # Weather or not the metric is enabled for an agency.
    is_metric_enabled: Optional[bool] = attr.field(default=None)

    # Additional context that the agency reported on this metric
    contexts: List[MetricContextData] = attr.field(factory=list)

    # Values for aggregated dimensions
    aggregated_dimensions: List[MetricAggregatedDimensionData] = attr.field(
        factory=list
    )

    # This field should is none for non-supervision metrics, and should
    # is a boolean for supervision metrics. If True, it means this
    # metric is disaggregated by supervision subsystems.
    disaggregated_by_supervision_subsystems: Optional[bool] = attr.field(default=None)

    # Values for includes_excludes settings at the metric level.
    includes_excludes_member_to_setting: Dict[
        enum.Enum, Optional[IncludesExcludesSetting]
    ] = attr.Factory(dict)

    # Values for the metric's custom reporting frequency.
    custom_reporting_frequency: CustomReportingFrequency = CustomReportingFrequency()

    @property
    def metric_definition(self) -> MetricDefinition:
        # MetricDefinition that this MetricInterface corresponds to
        return METRIC_KEY_TO_METRIC[self.key]

    @property
    def has_report_datapoints(self) -> bool:
        if self.value is not None:
            return True

        for aggregated_dimension in self.aggregated_dimensions:
            if aggregated_dimension.dimension_to_value is None:
                continue
            # Check that all values of aggregated dimensions are None.
            for _, value in aggregated_dimension.dimension_to_value.items():
                if value is not None:
                    return True
        return False

    @property
    def metric_files(self) -> List[MetricFile]:
        # MetricFiles that this MetricInterface corresponds to
        system_metric_files = SYSTEM_TO_METRICFILES[self.metric_definition.system]
        metric_files_for_metric = list(
            filter(
                lambda metric: metric.definition.key == self.key,
                system_metric_files,
            )
        )
        return metric_files_for_metric

    def get_reporting_frequency_to_use(
        self,
    ) -> Tuple[ReportingFrequency, Optional[int]]:
        # Returns a tuple containing:
        # - the reporting frequency based upon what the agency has set in metric settings.
        # - the custom starting month of the reporting frequency (if specified) (None if not specified)
        # Note that the custom starting month applies for custom reporting frequencies for annual metrics

        custom_reporting_frequency = self.custom_reporting_frequency
        default_frequency = self.metric_definition.reporting_frequency
        return (
            (
                custom_reporting_frequency.frequency,
                custom_reporting_frequency.starting_month,
            )
            if custom_reporting_frequency.frequency is not None
            else (default_frequency, None)
        )

    ### To/From JSON ###

    def to_json(
        self,
        entry_point: DatapointGetRequestEntryPoint,
        aggregate_datapoints_json: Optional[List[DatapointJson]] = None,
        dimension_id_to_dimension_member_to_datapoints_json: Optional[
            DefaultDict[str, DefaultDict[str, List[DatapointJson]]]
        ] = None,
    ) -> Dict[str, Any]:
        """Returns the json form of the MetricInterface object."""

        dimension_id_to_dimension_definition = {
            d.dimension_identifier(): d
            for d in self.metric_definition.aggregated_dimensions or []
        }
        context_key_to_context_definition = {
            context.key: context for context in self.metric_definition.contexts or []
        }

        frequency = self.metric_definition.reporting_frequency.value
        includes_excludes_json_lst = []
        if (
            entry_point is DatapointGetRequestEntryPoint.METRICS_TAB
            and self.metric_definition.includes_excludes
        ):
            for includes_excludes in self.metric_definition.includes_excludes:
                includes_excludes_json: Dict[str, Any] = {
                    "description": includes_excludes.description,
                    "settings": [],
                }
                for (
                    member,
                    default_setting,
                ) in includes_excludes.member_to_default_inclusion_setting.items():
                    included = self.includes_excludes_member_to_setting.get(member)
                    includes_excludes_json["settings"].append(
                        {
                            "key": member.name,
                            "label": member.value,
                            "included": (
                                included.value if included is not None else None
                            ),
                            "default": default_setting.value,
                        }
                    )
                includes_excludes_json_lst.append(includes_excludes_json)

        metric_filenames = [
            metric_file.canonical_filename for metric_file in self.metric_files
        ]

        return {
            "key": self.key,
            "system": {
                "key": self.metric_definition.system.value,
                "display_name": self.metric_definition.system.value.replace("_", " ")
                .title()
                .replace("And", "and"),
            },
            "display_name": self.metric_definition.display_name,
            "description": self.metric_definition.description,
            "reporting_note": self.metric_definition.reporting_note,
            "value": self.value,
            "includes_excludes": includes_excludes_json_lst,
            "unit": self.metric_definition.metric_type.unit,
            "category": self.metric_definition.category.human_readable_string,
            "label": self.metric_definition.display_name,
            "enabled": self.is_metric_enabled,
            "frequency": frequency,
            "custom_frequency": (
                self.custom_reporting_frequency.frequency.value
                if self.custom_reporting_frequency.frequency is not None
                else None
            ),
            "starting_month": (
                1
                if self.custom_reporting_frequency.starting_month is None
                and frequency == schema.ReportingFrequency.ANNUAL.value
                else self.custom_reporting_frequency.starting_month
            ),
            "filenames": metric_filenames,
            "datapoints": aggregate_datapoints_json,
            "contexts": [
                c.to_json(context_definition=context_key_to_context_definition[c.key])
                for c in self.contexts
            ],
            "disaggregations": [
                d.to_json(
                    dimension_definition=dimension_id_to_dimension_definition[
                        d.dimension_identifier()
                    ],
                    dimension_member_to_datapoints_json=(
                        dimension_id_to_dimension_member_to_datapoints_json.get(
                            d.dimension_identifier()
                        )
                        if dimension_id_to_dimension_member_to_datapoints_json
                        is not None
                        else None
                    ),
                    entry_point=entry_point,
                )
                for d in self.aggregated_dimensions
            ],
            "disaggregated_by_supervision_subsystems": self.disaggregated_by_supervision_subsystems,
        }

    @classmethod
    def from_json(
        cls: Type[MetricInterfaceT],
        json: Dict[str, Any],
        entry_point: DatapointGetRequestEntryPoint,
    ) -> MetricInterfaceT:
        """Creates a instance of a MetricInterface from a formatted json."""
        metric_context_data = [
            MetricContextData.from_json(
                json=context_json,
            )
            for context_json in json.get("contexts", [])
        ]
        metric_definition = METRIC_KEY_TO_METRIC[json["key"]]
        includes_excludes_member_to_setting = {}
        if (
            metric_definition.includes_excludes is not None
            and entry_point is DatapointGetRequestEntryPoint.METRICS_TAB
        ):
            actual_includes_excludes_list = json.get("settings", [])
            actual_includes_excludes_member_to_setting = {
                actual_includes_excludes["key"]: actual_includes_excludes["included"]
                for actual_includes_excludes in actual_includes_excludes_list
            }
            for includes_excludes in metric_definition.includes_excludes:
                for member in includes_excludes.members:
                    setting = actual_includes_excludes_member_to_setting.get(
                        member.name
                    )
                    includes_excludes_member_to_setting[member] = (
                        IncludesExcludesSetting(setting)
                        if setting
                        in {
                            IncludesExcludesSetting.YES.value,
                            IncludesExcludesSetting.NO.value,
                        }
                        else None
                    )

        disaggregations = []
        dimension_id_to_definition = (
            {
                d.dimension.dimension_identifier(): d
                for d in metric_definition.aggregated_dimensions
            }
            if metric_definition.aggregated_dimensions is not None
            else {}
        )
        for dimension_json in json.get("disaggregations", []):
            dimension_definition = dimension_id_to_definition.get(dimension_json["key"])
            if dimension_definition is None:
                raise JusticeCountsServerError(
                    code="invalid_dimension_id",
                    description=f'Metric json contains an invalid dimension identifier: {dimension_json["key"]}',
                )
            disaggregations.append(
                MetricAggregatedDimensionData.from_json(
                    json=dimension_json,
                    entry_point=entry_point,
                    disaggregation_definition=dimension_definition,
                )
            )

        if (
            "value" not in json
            and entry_point is DatapointGetRequestEntryPoint.REPORT_PAGE
        ):
            raise JusticeCountsServerError(
                code="report_metric_no_value",
                description="No value field is included in request json",
            )
        return cls(
            key=json["key"],
            value=json.get("value"),
            contexts=metric_context_data,
            aggregated_dimensions=disaggregations,
            includes_excludes_member_to_setting=includes_excludes_member_to_setting,
            is_metric_enabled=json.get("enabled"),
            custom_reporting_frequency=CustomReportingFrequency.from_json(json),
            disaggregated_by_supervision_subsystems=json.get(
                "disaggregated_by_supervision_subsystems"
            ),
        )

    ### Helpers ###

    @staticmethod
    def get_metric_definitions_by_systems(
        systems: Set[schema.System],
    ) -> List[MetricDefinition]:
        """Given a list of systems and report frequency, return all
        MetricDefinitions that belong to one of the systems and have
        the given frequency."""
        # Sort systems so that Supervision, Parole, Probation, and Post-Release
        # are always grouped together, in that order
        systems_ordered = schema.System.sort(systems=list(systems))
        supervision_metrics = METRICS_BY_SYSTEM[schema.System.SUPERVISION.value]
        metrics = []
        for system in systems_ordered:
            if system == schema.System.SUPERVISION:
                for metric in supervision_metrics:
                    metrics.append(metric)
            elif (
                system in schema.System.supervision_subsystems()
                and schema.System.SUPERVISION in systems
            ):
                # Make copies of Supervision metrics for all Supervision subsystems.
                # Later, these copies will be turned on/off depending on whether the
                # user has specified "disaggregated_by_supervision_subsystems.""
                for metric in supervision_metrics:
                    subsystem_metric = get_supervision_subsystem_metric_definition(
                        subsystem=system.value,
                        supervision_metric_definition=metric,
                    )
                    metrics.append(subsystem_metric)
            else:
                metrics += METRICS_BY_SYSTEM[system.value]

        metric_definitions = [
            metric for metric in metrics if metric.disabled is not True
        ]

        return metric_definitions
