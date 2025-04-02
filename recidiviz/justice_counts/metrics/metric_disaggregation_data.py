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
"""Base class for the reported value(s) for a Justice Counts metric dimension."""

import enum
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Tuple, Type, TypeVar, cast

import attr

from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.dimensions.person import RaceAndEthnicity
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_context_data import MetricContextData
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    ConfigurationStatus,
    IncludesExcludesSet,
    IncludesExcludesSetting,
)
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import DatapointGetRequestEntryPoint

MetricAggregatedDimensionDataT = TypeVar(
    "MetricAggregatedDimensionDataT", bound="MetricAggregatedDimensionData"
)


@attr.define()
class MetricAggregatedDimensionData:
    """Values entered by the agency for a given `AggregatedDimension`. The `dimension_to_value`
    dictionary should map `Dimension` enum values to numeric values.
    """

    dimension_to_value: Optional[Dict[DimensionBase, Any]] = attr.field(default=None)
    dimension_to_enabled_status: Optional[Dict[DimensionBase, Any]] = attr.field(
        default=None
    )
    dimension_to_includes_excludes_member_to_setting: Dict[
        DimensionBase, Dict[enum.Enum, Optional[IncludesExcludesSetting]]
    ] = attr.Factory(dict)
    dimension_to_contexts: Dict[DimensionBase, List[MetricContextData]] = attr.Factory(
        dict
    )
    contexts: List[MetricContextData] = attr.Factory(list)
    # Maps dimension to configuration status (whether the agency considers
    # the includes/excludes of each dimension of this disaggregation to be fully
    # configured to their satisfaction)
    dimension_to_includes_excludes_configured_status: Dict[
        DimensionBase, Optional[ConfigurationStatus]
    ] = attr.Factory(dict)
    # Indicates whether the agency considers this disaggregation's breakdowns to be
    # turned on/off to their satisfaction
    is_breakdown_configured: Optional[ConfigurationStatus] = attr.field(default=None)
    # Maps each dimension to a list of user-defined labels that further break down
    # the "Other" categories.
    dimension_to_other_sub_dimensions: Dict[DimensionBase, List[str]] = attr.Factory(
        dict
    )

    def dimension_identifier(self) -> str:
        # Identifier of the Dimension class that this breakdown corresponds to
        # e.g. if `dimension_to_value` is `{Gender.FEMALE: 10, Gender.MALE: 5}`
        # then this returns `Gender.FEMALE.__class__.dimensions_identifier()`
        dimension_dict = (
            self.dimension_to_enabled_status
            if self.dimension_to_enabled_status
            else self.dimension_to_value
        )
        if dimension_dict is not None:
            return list(dimension_dict.keys())[0].__class__.dimension_identifier()

        raise JusticeCountsServerError(
            code="no_dimension_data",
            description="Metric has no dimension_to_enabled_status or dimension_to_value dictionary.",
        )

    ### To/From JSON ###
    def to_storage_json(self) -> Dict[str, Any]:
        """
        Convert the MetricAggregatedDimensionData object to a json object for storage in
        the database. We drop the `dimension_to_value` field for storage. Report
        datapoints are stored in the datapoints table.
        """
        return {
            # Optional[Dict[DimensionBase, Any]]
            "dimension_to_enabled_status": (
                {
                    dimension.to_enum().name: enabled
                    for dimension, enabled in self.dimension_to_enabled_status.items()
                    if enabled is not None
                }
                if self.dimension_to_enabled_status is not None
                else None
            ),
            # Dict[DimensionBase, Dict[enum.Enum, Optional[IncludesExcludesSetting]]]
            "dimension_to_includes_excludes_member_to_setting": {
                dimension.to_enum().name: {
                    member.name: include_excludes_setting.name
                    for member, include_excludes_setting in includes_excludes_member_to_setting.items()
                    if include_excludes_setting is not None
                }
                for dimension, includes_excludes_member_to_setting in self.dimension_to_includes_excludes_member_to_setting.items()
            },
            "contexts": {
                context.key.name: context.value
                for context in self.contexts
                if context.value is not None
            },
            # Dict[DimensionBase, List[MetricContextData]]
            "dimension_to_contexts": {
                dimension.to_enum().name: {
                    context.key.name: context.value for context in contexts
                }
                for dimension, contexts in self.dimension_to_contexts.items()
            },
            # Optional[ConfigurationStatus]
            "is_breakdown_configured": ConfigurationStatus.to_json(
                self.is_breakdown_configured
            ),
            # Dict[DimensionBase, ConfigurationStatus]
            "dimension_to_includes_excludes_configured_status": {
                dimension.to_enum().name: ConfigurationStatus.to_json(config_status)
                for dimension, config_status in self.dimension_to_includes_excludes_configured_status.items()
            },
            "dimension_to_other_sub_dimensions": {
                dimension.to_enum().name: other_sub_dimensions
                for dimension, other_sub_dimensions in self.dimension_to_other_sub_dimensions.items()
            },
        }

    @classmethod
    def from_storage_json(
        cls: Type[MetricAggregatedDimensionDataT],
        json: Dict[str, Any],
        aggregated_dimension: AggregatedDimension,
    ) -> MetricAggregatedDimensionDataT:
        """
        Convert the json object from the database to a MetricAggregatedDimensionData
        object. This is the inverse of to_storage_json.
        We must reference the AggregatedDimension to populate the full set of dimensions
        and members for include exclude settings and contexts, even if they are not
        present in the json.
        """
        dimension_class = aggregated_dimension.dimension  # example: RaceAndEthnicity
        dimension_str_to_dimension = {
            dimension.to_enum().name: dimension for dimension in dimension_class  # type: ignore[attr-defined]
        }
        # We set the value of all dimensions to None since from_storage_json does not
        # return report datapoints. Report datapoints are stored in the Datapoints table.
        dimension_to_value: Dict[DimensionBase, Any] = {
            dimension: None for dimension in dimension_class  # type: ignore[attr-defined]
        }

        # For dimensions not present in the json, we set their enabled status to None.
        dimension_to_enabled_status: Dict[DimensionBase, Any] = {}
        for dimension in dimension_class:  # type: ignore[attr-defined]
            dimension_to_enabled_status[dimension] = None

        for dimension, enabled_status in json.get(
            "dimension_to_enabled_status", {}
        ).items():
            dimension_to_enabled_status[
                dimension_str_to_dimension[dimension]
            ] = enabled_status

        # For dimensions not present in the json, we set their configuration status to None.
        dimension_to_includes_excludes_configured_status: Dict[
            DimensionBase, Optional[ConfigurationStatus]
        ] = {}
        for dimension in dimension_class:  # type: ignore[attr-defined]
            dimension_to_includes_excludes_configured_status[dimension] = None

        for dimension, config_status in json.get(
            "dimension_to_includes_excludes_configured_status", {}
        ).items():
            dimension_to_includes_excludes_configured_status[
                dimension_str_to_dimension[dimension]
            ] = ConfigurationStatus.from_json(config_status)

        dimension_to_includes_excludes_member_to_setting: Dict[
            DimensionBase, Dict[enum.Enum, Optional[IncludesExcludesSetting]]
        ] = defaultdict(dict)
        if aggregated_dimension.dimension_to_includes_excludes is not None:
            json_dimension_to_includes_excludes_member_to_setting = json.get(
                "dimension_to_includes_excludes_member_to_setting", {}
            )
            for dimension in dimension_class:  # type: ignore[attr-defined]
                # If the dimension in the dimension class is not represented in the
                # stored json, this is fine.
                # get_includes_excludes_dict_from_storage_json() will populate the dict
                # with a default setting for that member.
                includes_excludes_member_to_setting = (
                    json_dimension_to_includes_excludes_member_to_setting.get(
                        dimension.to_enum().name, {}
                    )
                )
                dimension_to_includes_excludes_member_to_setting[
                    dimension
                ] = IncludesExcludesSet.get_includes_excludes_dict_from_storage_json(
                    includes_excludes_member_to_setting=includes_excludes_member_to_setting,
                    includes_excludes_set_lst=aggregated_dimension.dimension_to_includes_excludes.get(
                        dimension
                    ),
                )

        dimension_to_contexts: Dict[DimensionBase, List[MetricContextData]] = {}
        if aggregated_dimension.dimension_to_contexts is not None:
            for dimension_str, stored_metric_contexts in json.get(
                "dimension_to_contexts", {}
            ).items():
                dimension = dimension_str_to_dimension[dimension_str]
                dimension_to_contexts[
                    dimension
                ] = MetricContextData.get_metric_context_data_from_storage_json(
                    stored_metric_contexts=stored_metric_contexts,
                    metric_definition_contexts=aggregated_dimension.dimension_to_contexts[
                        dimension
                    ],
                )

        dimension_to_other_sub_dimensions: Dict[DimensionBase, List[str]] = {}
        for dimension_str, other_sub_dimensions in json.get(
            "dimension_to_other_sub_dimensions", {}
        ).items():
            dimension = dimension_str_to_dimension[dimension_str]
            dimension_to_other_sub_dimensions[dimension] = other_sub_dimensions

        return cls(
            dimension_to_value=dimension_to_value,
            dimension_to_enabled_status=dimension_to_enabled_status,
            dimension_to_includes_excludes_member_to_setting=dimension_to_includes_excludes_member_to_setting,
            dimension_to_contexts=dimension_to_contexts,
            dimension_to_includes_excludes_configured_status=dimension_to_includes_excludes_configured_status,
            dimension_to_other_sub_dimensions=dimension_to_other_sub_dimensions,
            contexts=MetricContextData.get_metric_context_data_from_storage_json(
                stored_metric_contexts=json.get("contexts", {}),
                metric_definition_contexts=aggregated_dimension.contexts or [],
            ),
            is_breakdown_configured=ConfigurationStatus.from_json(
                json.get("is_breakdown_configured")
            ),
        )

    def to_json(
        self,
        dimension_definition: AggregatedDimension,
        entry_point: DatapointGetRequestEntryPoint,
        dimension_member_to_datapoints_json: Optional[
            DefaultDict[str, List[DatapointJson]]
        ] = None,
        is_v2: Optional[bool] = False,
    ) -> Dict[str, Any]:
        """
        Converts the given dimension definition and related information into a JSON-compatible dictionary.

        Args:
            dimension_definition (AggregatedDimension): The definition of the dimension to be converted to JSON.
            entry_point (DatapointGetRequestEntryPoint): The entry point from which the data request originates.
            dimension_member_to_datapoints_json (Optional[DefaultDict[str, List[DatapointJson]]], optional):
                A dictionary mapping dimension members to their corresponding datapoints in JSON format.
                Defaults to None.

        Returns:
            Dict[str, Any]: A dictionary containing the JSON representation of the dimension definition and its attributes.

        Notes:
            - The disaggregation status is determined based on the enabled status of its dimensions:
                - If at least one dimension is enabled or disabled, the disaggregation is considered enabled.
                - If all dimensions are disabled, the disaggregation is considered disabled.
                - If all dimensions are None, the disaggregation will be None.
            - The response includes a key, display name, dimensions, and enabled status.
            - Additional fields such as helper text, required status, and should_sum_to_total are included
            if the entry point is not the agency dashboard.
        """

        is_disaggregation_enabled = None
        if self.dimension_to_enabled_status is not None:
            enabled_statuses = self.dimension_to_enabled_status.values()
            if all(dim is False for dim in enabled_statuses):
                is_disaggregation_enabled = False
            elif any(dim is not None for dim in enabled_statuses):
                is_disaggregation_enabled = True

        response = {
            "key": dimension_definition.dimension.dimension_identifier(),
            "is_breakdown_configured": ConfigurationStatus.to_json(
                self.is_breakdown_configured
            ),
            "display_name": dimension_definition.display_name
            or dimension_definition.dimension.display_name(),
            "dimensions": self.dimension_to_json(
                entry_point=entry_point,
                dimension_member_to_datapoints_json=dimension_member_to_datapoints_json,
                dimension_definition=dimension_definition,
                is_v2=is_v2,
            ),
            "enabled": is_disaggregation_enabled,
        }

        if is_v2 is False:
            response["helper_text"] = dimension_definition.helper_text
            response["required"] = dimension_definition.required
            response["should_sum_to_total"] = dimension_definition.should_sum_to_total
            context_key_to_context_definition = {
                context.key: context for context in dimension_definition.contexts or []
            }
            response["contexts"] = [
                c.to_json(context_definition=context_key_to_context_definition[c.key])
                for c in self.contexts
                if c.key in context_key_to_context_definition
            ]
        elif (
            dimension_definition.dimension_identifier()
            == RaceAndEthnicity.dimension_identifier()
        ):
            response[
                "consolidated_race_ethnicity"
            ] = self._get_consolidated_race_ethnicity_dict(  # type: ignore[assignment]
                dimension_definition=dimension_definition,
                dimension_member_to_datapoints_json=dimension_member_to_datapoints_json,
            )

        return response

    def _get_consolidated_race_ethnicity_dict(
        self,
        dimension_definition: AggregatedDimension,
        dimension_member_to_datapoints_json: Optional[
            DefaultDict[str, List[DatapointJson]]
        ] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Aggregates and groups datapoints by consolidated race/ethnicity into a JSON-compatible dictionary,
        with descriptions of consolidated race and ethnicity groups.

        Args:
            dimension_member_to_datapoints_json (Optional[DefaultDict[str, List[DatapointJson]]], optional):
                A dictionary mapping dimension members to lists of datapoints in JSON format.
                Each datapoint contains time range and value data. Defaults to None.

        Returns:
            Dict[str, Union[Dict[str, Dict[str, int]], Dict[str, str]]]: A dictionary with two keys:
                - "data": A dictionary mapping each consolidated race/ethnicity to time ranges,
                with the corresponding aggregated total value for each time range.
                - "descriptions": A dictionary providing human-readable definitions of the consolidated
                race and ethnicity groups.

        Notes:
            - Consolidates race and ethnicity based on the `RaceAndEthnicity` dimension:
                - If the dimension member's ethnicity is "Hispanic or Latino", it is grouped as such.
                - Otherwise, it is grouped by race.
            - Aggregates datapoints within each race/ethnicity group over specified time ranges.
            - The time ranges are represented as strings in the format "start_date - end_date".

            - Transforms `dimension_member_to_datapoints_json` with the format:
                `HISPANIC_WHITE`: [DatapointJson, DatapointJson...],
                `HISPANIC_BLACK`: [DatapointJson, DatapointJson...],
                `NOT_HISPANIC_WHITE`: [DatapointJson, DatapointJson...],
                `NOT_HISPANIC_BLACK`: [DatapointJson, DatapointJson...]

            into `consolidated_race_ethnicity_json` in this format:
                - "data": {
                    `HISPANIC`: {June 1 datetime - July 1 datetime: <total for Hispanic ethnicity over time frame>, ...},
                    `WHITE`: {June 1 datetime - July 1 datetime: <total for White, not/unknown Hispanic ethnicity over time frame>, ...},
                    ...
                }
                - "descriptions": {
                    `HISPANIC`: "Includes all individuals who self-identify as Hispanic or Latino, regardless of race.",
                    `WHITE`: "Includes individuals who self-identify as White and are not of Hispanic or Latino ethnicity.",
                    ...
                }
        """
        consolidated_race_ethnicity_json: Dict[str, Dict[str, Any]] = {}
        consolidated_race_ethnicity_json["descriptions"] = (
            {
                key.dimension_value: description
                for key, description in dimension_definition.dimension_to_description.items()
            }
            if dimension_definition.dimension_to_description is not None
            else {}
        )

        if dimension_member_to_datapoints_json is None:
            return consolidated_race_ethnicity_json

        # 1) Re-group datapoints by consolidate race/ethnicity
        consolidated_race_ethnicity_to_time_range_to_aggregate_total: DefaultDict[
            str, DefaultDict[Tuple[str, str], int]
        ] = defaultdict(lambda: defaultdict(int))
        for (
            dimension_member,
            datapoint_json_list,
        ) in dimension_member_to_datapoints_json.items():
            dimension = RaceAndEthnicity[dimension_member]
            consolidated_race_ethnicity = (
                dimension.ethnicity
                if dimension.ethnicity == "Hispanic or Latino"
                else dimension.race
            )
            for datapoint_json in datapoint_json_list:
                if datapoint_json["value"] is not None:
                    consolidated_race_ethnicity_to_time_range_to_aggregate_total[
                        consolidated_race_ethnicity
                    ][
                        (
                            datapoint_json["start_date"],
                            datapoint_json["end_date"],
                        )
                    ] += datapoint_json[
                        "value"
                    ]
        consolidated_race_ethnicity_data_json: Dict[str, Dict[str, int]] = {}

        # 2) Format consolidated race/ethnicity into a JSON response
        for (
            consolidated_race_ethnicity,
            time_range_to_aggregate_total,
        ) in consolidated_race_ethnicity_to_time_range_to_aggregate_total.items():
            for (
                time_range,
                aggregate_total,
            ) in time_range_to_aggregate_total.items():
                if (
                    consolidated_race_ethnicity
                    not in consolidated_race_ethnicity_data_json
                ):
                    consolidated_race_ethnicity_data_json[
                        consolidated_race_ethnicity
                    ] = {}

                consolidated_race_ethnicity_data_json[consolidated_race_ethnicity][
                    f"{time_range[0]} - {time_range[1]}"
                ] = aggregate_total

        consolidated_race_ethnicity_json["data"] = consolidated_race_ethnicity_data_json

        return consolidated_race_ethnicity_json

    @classmethod
    def from_json(
        cls: Type[MetricAggregatedDimensionDataT],
        json: Dict[str, Any],
        entry_point: DatapointGetRequestEntryPoint,
        disaggregation_definition: AggregatedDimension,
    ) -> MetricAggregatedDimensionDataT:
        """
        - The input json is expected to be of the format {dimension name -> value/enabled}, e.g. {"BLACK": 50} for report
          datapoints or {"BLACK": True} for agency datapoints.
        - The input json does not need to include all dimension names, i.e. it can be partial/incomplete. Values/enabled status'
          that are not reported will have values of None (e.g {"BLACK": None} for report datapoints or {"BLACK": None}). None values
          for report datapoints represent values that have not been reported. None values for agency datapoints represent metric configuration
          values that have not been changed.
        - This function will create a dimension_to_value or a dimension_to_enabled_status dictionary that
          does include all dimension names.
        - The dimensions that were reported in json will be copied over to dimension_to_value/dimension_to_enabled_status dict.
        """
        value_key = (
            "value"
            if entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
            else "enabled"
        )
        # convert dimension name -> value/enabled mapping to dimension class -> value/enabled mapping
        # e.g "BLACK" : 50 -> RaceAndEthnicity().BLACK : 50
        dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
            json["key"]
        ]  # example: RaceAndEthnicity
        dimensions = json.get("dimensions")
        dimension_enum_value_to_value = {
            dim["key"]: dim.get(value_key) for dim in dimensions or []
        }  # example: {"BLACK": 50, "WHITE": 20, ...} if a report metric
        # or {"BLACK": True, "WHITE": False, ...} if it is an agency metric
        is_breakdown_configured = ConfigurationStatus.from_json(
            json.get("is_breakdown_configured")
        )

        if entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE:
            return cls(
                dimension_to_value={
                    dimension: dimension_enum_value_to_value.get(
                        dimension.to_enum().value, None
                    )
                    for dimension in dimension_class  # type: ignore[attr-defined]
                },
                contexts=[
                    MetricContextData(
                        key=ContextKey[context["key"]],
                        value=context["value"],
                    )
                    for context in json.get("contexts", [])
                ],
            )  # example: {RaceAndEthnicity.BLACK: 50, RaceAndEthnicity.WHITE: 20})

        # default_dimension_enabled_status will be True or False if a disaggregation is being turned off/on,
        # and None otherwise.
        default_dimension_enabled_status = json.get("enabled")
        dimension_to_enabled_status = {
            dimension: dimension_enum_value_to_value.get(
                dimension.to_enum().value,
                default_dimension_enabled_status,
            )
            for dimension in dimension_class  # type: ignore[attr-defined]
        }  # example: {RaceAndEthnicity.BLACK: True, RaceAndEthnicity.WHITE: False}

        if (
            disaggregation_definition is not None
            and disaggregation_definition.dimension_to_includes_excludes is None
            and disaggregation_definition.contexts is None
        ):
            # If the disaggregation definition has no includes_excludes options specified or any contexts,
            # return a MetricAggregatedDimensionData object with just a dimension_to_enabled_status
            # dict.
            return cls(
                dimension_to_enabled_status=dimension_to_enabled_status,
                is_breakdown_configured=is_breakdown_configured,
            )
        disaggregation_contexts = []
        for context in json.get("contexts", []):
            disaggregation_contexts.append(
                MetricContextData(
                    key=ContextKey[context["key"]],
                    value=context["value"],
                )
            )

        dimension_enum_value_to_includes_excludes_member_to_setting = {}
        dimension_to_includes_excludes_configured_status = {}
        dimension_to_contexts = {}
        for dim in json.get("dimensions", []):
            dimension_key = dim["key"]
            # First, process the settings (i.e. the includes excludes)
            # example: {"BLACK": {"SETTING_1": "Yes", "SETTING_2", "No"},
            # "WHITE": {"SETTING_1": "No", "SETTING_2", "Yes"}}
            dimension_enum_value_to_includes_excludes_member_to_setting[
                dimension_key
            ] = {
                setting["key"]: setting["included"]
                for setting in dim.get("settings", [])
            }

            # Now, process the contexts
            # Need to convert dimension_key to dimension_enum_value
            dimension_enum_value = dimension_class(dimension_key)  # type: ignore[abstract]
            for context in dim.get("contexts", []):
                dimension_to_contexts[dimension_enum_value] = [
                    MetricContextData(
                        key=ContextKey[context["key"]],
                        value=context["value"],
                    )
                ]

            # Process the configuration status of each includes/excludes
            dimension_to_includes_excludes_configured_status[
                dimension_enum_value
            ] = ConfigurationStatus.from_json(
                dim.get("is_dimension_includes_excludes_configured")
            )

        dimension_to_includes_excludes_member_to_setting: Dict[
            DimensionBase, Dict[enum.Enum, Optional[IncludesExcludesSetting]]
        ] = {
            dimension: {} for dimension in dimension_class  # type: ignore[attr-defined]
        }

        for dimension in dimension_class:  # type: ignore[attr-defined]
            # For each dimension that is part of the aggregated dimension,
            # get the IncludesExcludesSet, which contains all the
            # members of the includes/excludes enum as well as the default settings.
            includes_excludes_set = (
                (
                    disaggregation_definition.dimension_to_includes_excludes.get(
                        dimension
                    )
                )
                if disaggregation_definition.dimension_to_includes_excludes is not None
                else None
            )
            member_to_include_excludes_setting = {}
            # Example: {"SETTING_1": "Yes", "SETTING_2": "No...}
            member_to_actual_inclusion_setting = (
                dimension_enum_value_to_includes_excludes_member_to_setting.get(
                    dimension.to_enum().value, {}
                )
            )
            if includes_excludes_set is not None:
                for includes_excludes in includes_excludes_set:
                    for member in includes_excludes.members:
                        setting = member_to_actual_inclusion_setting.get(member.name)
                        member_to_include_excludes_setting[member] = (
                            IncludesExcludesSetting(setting)
                            if setting
                            in {
                                IncludesExcludesSetting.YES.value,
                                IncludesExcludesSetting.NO.value,
                            }
                            else None
                        )
                dimension_to_includes_excludes_member_to_setting[
                    dimension
                ] = member_to_include_excludes_setting

        return cls(
            dimension_to_enabled_status=dimension_to_enabled_status,
            dimension_to_includes_excludes_member_to_setting=dimension_to_includes_excludes_member_to_setting,
            dimension_to_contexts=dimension_to_contexts,
            contexts=disaggregation_contexts,
            is_breakdown_configured=is_breakdown_configured,
            dimension_to_includes_excludes_configured_status=dimension_to_includes_excludes_configured_status,
        )

    ### To/From JSON Helpers ###

    def dimension_to_json(
        self,
        entry_point: DatapointGetRequestEntryPoint,
        dimension_definition: AggregatedDimension,
        dimension_member_to_datapoints_json: Optional[
            Dict[str, List[DatapointJson]]
        ] = None,
        is_v2: Optional[bool] = False,
    ) -> List[Dict[str, List[Dict[str, Any]]]]:
        """This method would be called in two scenarios: 1) We are getting the json of
        a report metric which will have both dimension_to_enabled_status and dimension_to_value
        populated or 2) We are getting the json of an agency metric which will only have
        dimension_to_enabled_status populated."""
        dimension_to_includes_excludes = (
            dimension_definition.dimension_to_includes_excludes
        )
        dimension_to_description = dimension_definition.dimension_to_description
        dimension_to_contexts = dimension_definition.dimension_to_contexts
        dimensions = []
        if self.dimension_to_enabled_status is not None:
            for dimension, status in self.dimension_to_enabled_status.items():
                json = {
                    "key": dimension.to_enum().value,
                    "label": dimension.dimension_value,
                    "enabled": status,
                    "datapoints": (
                        dimension_member_to_datapoints_json.get(
                            dimension.to_enum().name
                        )
                        if dimension_member_to_datapoints_json is not None
                        else None
                    ),
                }
                json["contexts"] = []
                if dimension_to_contexts is not None:
                    # contexts we expect for this dimension, according to its definition.
                    # For OTHER and UNKNOWN contexts, this should always be a singleton list
                    definition_contexts = dimension_to_contexts.get(dimension, [])
                    # contexts that were actually passed in via POST
                    # construct dict of actual context key to actual context value
                    actual_contexts = {}
                    for actual_context in self.dimension_to_contexts.get(dimension, []):
                        actual_contexts[actual_context.key.value] = actual_context.value
                    for context in definition_contexts:
                        # check to see if definition context has already been saved in db
                        if actual_contexts.get(context.key.value) is not None:
                            json_context = {
                                "key": context.key.value,
                                "value": actual_contexts.get(context.key.value),
                            }
                            json_context[
                                "label" if is_v2 is False else "display_name"
                            ] = context.label
                        else:
                            json_context = {
                                "key": context.key.value,
                                "value": None,
                            }
                            json_context[
                                "label" if is_v2 is False else "display_name"
                            ] = context.label
                        json["contexts"].append(json_context)
                if dimension_to_includes_excludes is not None and (
                    entry_point == DatapointGetRequestEntryPoint.METRICS_TAB
                    or is_v2 is True
                ):
                    includes_excluded_json = self.to_included_excluded_json(
                        dimension=dimension,
                        includes_excludes_set_lst=dimension_to_includes_excludes.get(
                            dimension
                        ),
                    )
                    json["includes_excludes"] = includes_excluded_json
                if (
                    self.dimension_to_includes_excludes_configured_status is not None
                    and entry_point == DatapointGetRequestEntryPoint.METRICS_TAB
                ):
                    json[
                        "is_dimension_includes_excludes_configured"
                    ] = ConfigurationStatus.to_json(
                        self.dimension_to_includes_excludes_configured_status.get(
                            dimension
                        )
                    )
                if (
                    self.dimension_to_value is not None
                    and entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
                ):
                    # if there is a non-null dimension_to_value dictionary, add dimension
                    # values into the json
                    json["value"] = self.dimension_to_value.get(dimension)
                if (
                    dimension.dimension_identifier()
                    == RaceAndEthnicity.dimension_identifier()
                ):
                    # Add race and ethnicity key/value if the dimension is a race dimension
                    json["race"] = cast(Type[RaceAndEthnicity], dimension).race
                    json["ethnicity"] = cast(
                        Type[RaceAndEthnicity], dimension
                    ).ethnicity
                elif (
                    self.dimension_to_value is None
                    and entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
                ):
                    raise JusticeCountsServerError(
                        code="no_dimension_values",
                        description=f"Metric {dimension.to_enum().value} has no dimension values",
                    )
                if (
                    dimension_to_description is not None
                    and dimension.dimension_identifier()
                    != RaceAndEthnicity.dimension_identifier()
                ):
                    json["description"] = dimension_to_description.get(dimension)
                else:
                    json["description"] = None
                dimensions.append(json)
        return dimensions

    def to_included_excluded_json(
        self,
        dimension: DimensionBase,
        includes_excludes_set_lst: Optional[List[IncludesExcludesSet]] = None,
    ) -> List[Dict[str, Any]]:
        """Returns a json list of include_exclude settings for a dimension."""

        includes_excludes_list: List[Dict[str, Optional[str]]] = []
        if includes_excludes_set_lst is None:
            return includes_excludes_list

        # Example: {SettingEnum.SETTING_1: IncludesExcludesSetting.YES,
        # SettingEnum.SETTING_2: IncludesExcludesSetting.No, ...}
        actual_member_to_includes_excludes_setting = (
            self.dimension_to_includes_excludes_member_to_setting.get(dimension, {})
        )

        for includes_excludes_set in includes_excludes_set_lst:
            includes_excludes_dict: Dict[str, Any] = {
                "settings": [],
                "multiselect": includes_excludes_set.multiselect,
                "description": includes_excludes_set.description,
            }
            for (
                member,
                default_setting,
            ) in includes_excludes_set.member_to_default_inclusion_setting.items():
                included = actual_member_to_includes_excludes_setting.get(member)
                includes_excludes_dict["settings"].append(
                    {
                        "key": member.name,
                        "label": member.value,
                        "included": included.value if included is not None else None,
                        "default": default_setting.value,
                    }
                )
            includes_excludes_list.append(includes_excludes_dict)
        return includes_excludes_list

    ### Validations ###

    @dimension_to_value.validator
    def validate(self, _attribute: attr.Attribute, value: Any) -> None:
        # Validate that all dimensions enum instances in the dictionary belong
        # to the same dimension enum class
        if self.dimension_to_value is None:
            return
        if value is None or value == {}:
            return
        dimension_classes = [d.__class__ for d in value.keys()]
        if not all(d == dimension_classes[0] for d in dimension_classes):
            raise ValueError(
                "Cannot instantiate ReportedAggregated Dimension: "
                + "Not all dimension instances belong to the same class."
            )

        # Validate that all members of the dimension enum class are present
        # in the dictionary
        if not set(dimension_classes[0]) == set(value.keys()):
            raise ValueError(
                "Cannot instantiate MetricAggregatedDimensionData: "
                + "Not all members of the dimension enum have a reported value.",
            )
