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
    ConfigurationStatus,
    IncludesExcludesSet,
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
from recidiviz.justice_counts.utils.constants import (
    METRIC_KEY_TO_V2_DASHBOARD_METRIC_KEY,
    DatapointGetRequestEntryPoint,
    ReportingAgencyCategory,
)
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

    # Whether or not the metric is enabled for an agency.
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

    # Whether or not the top-level includes/excludes settings are considered fully
    # configured by an agency.
    is_includes_excludes_configured: Optional[ConfigurationStatus] = attr.field(
        default=None
    )

    # Agency / Source ID responsible for reporting the metric.
    reporting_agency_id: Optional[int] = attr.field(default=None)

    # Indicates whether the agency is self-reporting. If true, the agency's
    # metadata is available in the agency section of the API response. If None,
    # the agency has not set up a reporting agency. If false, another agency
    # is reporting on its behalf and the other reporting_agency fields
    # will be populated.
    is_self_reported: Optional[bool] = attr.field(default=None)

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

    def post_process_storage_json(self) -> None:
        """Method to run on a MetricInterface after it has been created from a storage
        json object.

        This method stores any assertions we expect to have on a MetricInterface before
        it enters the rest of our system.
        """
        metric_definition = self.metric_definition
        # If this is a supervision subsystem metric, and the metric is not supposed to
        # be disaggregated by supervision subsystems, the metric must be disabled.
        if (
            self.is_metric_enabled is None
            and metric_definition.system in schema.System.supervision_subsystems()
            and not self.disaggregated_by_supervision_subsystems
        ):
            self.is_metric_enabled = False
        # A supervision metric interface's disaggregated_by_supervision_subsystems field
        # cannot be None.
        if (
            self.disaggregated_by_supervision_subsystems is None
            and metric_definition.system == schema.System.SUPERVISION
        ):
            self.disaggregated_by_supervision_subsystems = False

        # If the MetricInterface corresponds to a metric which should have at least one
        # aggregated dimension, but the MetricInterface has an unpopulated
        # aggregated_dimensions field (this might happen if there are no metric settings
        # stored in the MetricSetting table for this agency and metric key pair), we
        # must populate the aggregated_dimension field. We set all dimension's
        # dimension_to_enabled_status and dimension_to_value fields to None.
        if len(self.aggregated_dimensions) == 0:
            for aggregated_dimension in metric_definition.aggregated_dimensions or []:
                self.aggregated_dimensions.append(
                    MetricAggregatedDimensionData(
                        dimension_to_value={
                            d: None for d in aggregated_dimension.dimension  # type: ignore[attr-defined]
                        },
                        dimension_to_enabled_status={
                            d: None for d in aggregated_dimension.dimension  # type: ignore[attr-defined]
                        },
                    )
                )

        # Add null contexts if the MetricInterface has an empty context field.
        if len(self.contexts) == 0:
            for context in metric_definition.contexts:
                self.contexts.append(MetricContextData(key=context.key, value=None))

    def to_storage_json(self) -> Dict[str, Any]:
        """
        Returns the json form of the MetricInterface object to be stored in the database.
        This differs from to_json() in that it drops many fields that are empty so that
        the json object is smaller for storage. We also drop all report datapoints,
        which should instead be stored in the datapoint table.
        """
        return {
            "key": self.key,
            # Type: Optional[bool]
            "is_metric_enabled": self.is_metric_enabled,
            # Type: List[MetricContextData] -> Dict[key, value]
            "contexts": {
                context.key.name: context.value
                for context in self.contexts
                if context.value is not None
            },
            # Type: List[MetricAggregatedDimensionData]
            "aggregated_dimensions": {
                aggregated_dimension.dimension_identifier(): aggregated_dimension.to_storage_json()
                for aggregated_dimension in self.aggregated_dimensions
            },
            # Type: Optional[bool]
            "disaggregated_by_supervision_subsystems": self.disaggregated_by_supervision_subsystems,
            # Type: Dict[enum.Enum, Optional[IncludesExcludesSetting]]
            "includes_excludes_member_to_setting": {
                member.name: include_excludes_setting.name
                for member, include_excludes_setting in self.includes_excludes_member_to_setting.items()
                if include_excludes_setting is not None
            },
            # Type: CustomReportingFrequency
            # Fields:
            #   frequency: Optional[schema.ReportingFrequency]
            #   starting_month: Optional[int]
            "custom_reporting_frequency": {
                "custom_frequency": (
                    self.custom_reporting_frequency.frequency.name
                    if self.custom_reporting_frequency.frequency is not None
                    else None
                ),
                "starting_month": self.custom_reporting_frequency.starting_month,
            },
            "is_includes_excludes_configured": ConfigurationStatus.to_json(
                self.is_includes_excludes_configured
            ),
            "reporting_agency_id": self.reporting_agency_id,
            "is_self_reported": self.is_self_reported,
            "version": "v1",
        }

    @classmethod
    def _validate_storage_json(
        cls: Type[MetricInterfaceT],
        json: Dict[str, Any],
    ) -> None:
        """
        Ensure that the json object has all required fields. This should always be the
        case, since we should only call from_storage_json() on json objects that were
        created using to_storage_json().
        """
        required_fields = [
            "key",
            "aggregated_dimensions",
            "disaggregated_by_supervision_subsystems",
        ]
        for required_field in required_fields:
            if required_field not in json:
                raise JusticeCountsServerError(
                    code="invalid_storage_json",
                    description=f"Metric interface json is missing required field: {required_field}",
                )

    @classmethod
    def from_storage_json(
        cls: Type[MetricInterfaceT],
        json: Dict[str, Any],
    ) -> MetricInterfaceT:
        """Creates a instance of a MetricInterface from a formatted json."""
        cls._validate_storage_json(json)
        metric_definition = METRIC_KEY_TO_METRIC[json["key"]]

        contexts = MetricContextData.get_metric_context_data_from_storage_json(
            stored_metric_contexts=json.get("contexts", {}),
            # stored_metric_contexts=dict(json.get("contexts", []).items()),
            metric_definition_contexts=metric_definition.contexts,
        )

        aggregated_dimensions: List[MetricAggregatedDimensionData] = [
            MetricAggregatedDimensionData.from_storage_json(
                json=json["aggregated_dimensions"].get(
                    aggregated_dimension.dimension.dimension_identifier(), {}
                ),
                aggregated_dimension=aggregated_dimension,
            )
            for aggregated_dimension in metric_definition.aggregated_dimensions or []
        ]

        disaggregated_by_supervision_subsystems: Optional[bool] = (
            False  # Cannot be null for supervision systems.
            if metric_definition.system == schema.System.SUPERVISION
            and json.get("disaggregated_by_supervision_subsystems") is None
            else json.get("disaggregated_by_supervision_subsystems")
        )

        includes_excludes_member_to_setting: Dict[
            enum.Enum, Optional[IncludesExcludesSetting]
        ] = IncludesExcludesSet.get_includes_excludes_dict_from_storage_json(
            includes_excludes_member_to_setting=json.get(
                "includes_excludes_member_to_setting", {}
            ),
            includes_excludes_set_lst=metric_definition.includes_excludes,
        )

        return cls(
            key=json["key"],
            value=None,  # Stored metric interfaces do not contain datapoint fields.
            is_metric_enabled=json.get("is_metric_enabled", None),
            contexts=contexts,
            aggregated_dimensions=aggregated_dimensions,
            disaggregated_by_supervision_subsystems=disaggregated_by_supervision_subsystems,
            includes_excludes_member_to_setting=includes_excludes_member_to_setting,
            custom_reporting_frequency=CustomReportingFrequency.from_json(
                json.get("custom_reporting_frequency", {})
            ),
            is_includes_excludes_configured=ConfigurationStatus.from_json(
                json.get("is_includes_excludes_configured")
            ),
            reporting_agency_id=json.get("reporting_agency_id"),
            is_self_reported=json.get("is_self_reported"),
        )

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
        reporting_agency_id_to_agency: Optional[Dict[int, schema.Agency]] = None,
        aggregate_datapoints_json: Optional[List[DatapointJson]] = None,
        dimension_id_to_dimension_member_to_datapoints_json: Optional[
            DefaultDict[str, DefaultDict[str, List[DatapointJson]]]
        ] = None,
        is_v2: Optional[bool] = False,
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
            entry_point == DatapointGetRequestEntryPoint.METRICS_TAB or is_v2 is True
        ) and self.metric_definition.includes_excludes:
            for includes_excludes in self.metric_definition.includes_excludes:
                includes_excludes_json: Dict[str, Any] = {
                    "description": includes_excludes.description,
                    "multiselect": includes_excludes.multiselect,
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

        system_value = {
            "key": self.metric_definition.system.value,
            "display_name": self.metric_definition.system.value.replace("_", " ")
            .title()
            .replace("And", "and"),
        }

        key = (
            METRIC_KEY_TO_V2_DASHBOARD_METRIC_KEY.get(self.key, self.key)
            if is_v2 is True
            else self.key
        )

        response: Dict[str, Any] = {
            "key": key,
            "display_name": self.metric_definition.display_name,
            "description": self.metric_definition.description,
            "includes_excludes": includes_excludes_json_lst,
            "unit": (
                self.metric_definition.unit.value
                if self.metric_definition.unit is not None
                else None
            ),
            "category": self.metric_definition.category.human_readable_string,
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
                    is_v2=is_v2,
                )
                for d in self.aggregated_dimensions
            ],
            "disaggregated_by_supervision_subsystems": self.disaggregated_by_supervision_subsystems,
            "is_includes_excludes_configured": ConfigurationStatus.to_json(
                self.is_includes_excludes_configured
            ),
        }

        if is_v2 is True:
            reporting_agency_url = None
            reporting_agency_name = None
            reporting_agency_category = None
            if (
                self.reporting_agency_id is not None
                and reporting_agency_id_to_agency is not None
            ):
                reporting_agency = reporting_agency_id_to_agency[
                    self.reporting_agency_id
                ]

                reporting_agency_name = reporting_agency.name
                if reporting_agency.type == "agency":
                    reporting_agency_category = (
                        ReportingAgencyCategory.SUPER_AGENCY.value
                    )
                elif reporting_agency.type == ReportingAgencyCategory.CSG.value:
                    reporting_agency_category = ReportingAgencyCategory.CSG.value
                else:
                    reporting_agency_category = ReportingAgencyCategory.VENDOR.value

                urls = list(
                    filter(
                        lambda setting: setting.setting_type
                        in [
                            schema.AgencySettingType.HOMEPAGE_URL.value,
                        ],
                        reporting_agency.agency_settings,
                    )
                )
                reporting_agency_url = urls[0].value if len(urls) == 1 else None

            response["sector"] = system_value
            response["is_self_reported"] = self.is_self_reported
            response["reporting_agency_id"] = self.reporting_agency_id
            response["reporting_agency_name"] = reporting_agency_name
            response["reporting_agency_url"] = reporting_agency_url
            response["reporting_agency_category"] = reporting_agency_category
            response[
                "additional_description"
            ] = self.metric_definition.additional_description

            return response

        response["system"] = system_value
        response["reporting_note"] = self.metric_definition.reporting_note
        response["value"] = self.value
        response["filenames"] = metric_filenames
        response["label"] = self.metric_definition.display_name
        return response

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
            is_includes_excludes_configured=ConfigurationStatus.from_json(
                json.get("is_includes_excludes_configured")
            ),
            reporting_agency_id=json.get("reporting_agency_id"),
            is_self_reported=json.get("is_self_reported"),
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
