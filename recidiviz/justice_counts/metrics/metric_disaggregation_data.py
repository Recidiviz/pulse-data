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
from typing import Any, Dict, List, Optional, Type, TypeVar

import attr

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    IncludesExcludesSet,
    IncludesExcludesSetting,
)
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
    ] = attr.field(default={})

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

    def to_json(
        self,
        dimension_definition: AggregatedDimension,
        entry_point: DatapointGetRequestEntryPoint,
    ) -> Dict[str, Any]:
        is_disaggregation_enabled = (
            self.dimension_to_enabled_status is not None
            and any(list(self.dimension_to_enabled_status.values()))
        )  # A disaggregation is enabled if at least one of it's dimensions is enabled.

        return {
            "key": dimension_definition.dimension.dimension_identifier(),
            "helper_text": dimension_definition.helper_text,
            "required": dimension_definition.required,
            "should_sum_to_total": dimension_definition.should_sum_to_total,
            "display_name": dimension_definition.display_name
            or dimension_definition.dimension.display_name(),
            "dimensions": self.dimension_to_json(
                entry_point=entry_point,
                dimension_to_includes_excludes=dimension_definition.dimension_to_includes_excludes,
            ),
            "enabled": is_disaggregation_enabled,
        }

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
        if entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE:
            return cls(
                dimension_to_value={
                    dimension: dimension_enum_value_to_value.get(
                        dimension.to_enum().value, None
                    )
                    for dimension in dimension_class  # type: ignore[attr-defined]
                }
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
        ):
            # If the disaggregation definition has no includes_excludes options specified,
            # return a MetricAggregatedDimensionData object with just a dimension_to_enabled_status
            # dict.
            return cls(
                dimension_to_enabled_status=dimension_to_enabled_status,
            )

        dimension_to_includes_excludes_member_to_setting: Dict[
            DimensionBase, Dict[enum.Enum, Optional[IncludesExcludesSetting]]
        ] = {
            dimension: {} for dimension in dimension_class  # type: ignore[attr-defined]
        }

        # example: {"BLACK": {"SETTING_1": "Yes", "SETTING_2", "N/A"},
        # "WHITE": {"SETTING_1": "No", "SETTING_2", "Yes"}}
        dimension_enum_value_to_includes_excludes_member_to_setting = {
            dim["key"]: {
                setting["key"]: setting["included"]
                for setting in dim.get("settings", [])
            }
            for dim in dimensions or []
        }

        for dimension in dimension_class:  # type: ignore[attr-defined]
            # For each dimension that is part of the aggregated dimension,
            # get the IncludesExcludesSet, which contains all the
            # members of the includes/excludes enum as well as the default settings.
            includes_excludes_set = (
                disaggregation_definition.dimension_to_includes_excludes.get(dimension)
            )
            member_to_include_excludes_setting = {}
            # Example: {"SETTING_1": "Yes", "SETTING_2": "No...}
            member_to_actual_inclusion_setting = (
                dimension_enum_value_to_includes_excludes_member_to_setting.get(
                    dimension.to_enum().value, {}
                )
            )
            if includes_excludes_set is not None:
                for member in includes_excludes_set.members:
                    setting = member_to_actual_inclusion_setting.get(member.name)
                    member_to_include_excludes_setting[member] = (
                        IncludesExcludesSetting(setting)
                        if setting is not None
                        else None
                    )
                dimension_to_includes_excludes_member_to_setting[
                    dimension
                ] = member_to_include_excludes_setting

        return cls(
            dimension_to_enabled_status=dimension_to_enabled_status,
            dimension_to_includes_excludes_member_to_setting=dimension_to_includes_excludes_member_to_setting,
        )

    ### To/From JSON Helpers ###

    def dimension_to_json(
        self,
        entry_point: DatapointGetRequestEntryPoint,
        dimension_to_includes_excludes: Optional[
            Dict[DimensionBase, IncludesExcludesSet]
        ] = None,
    ) -> List[Dict[str, Any]]:
        """This method would be called in two scenarios: 1) We are getting the json of
        a report metric which will have both dimension_to_enabled_status and dimension_to_value
        populated or 2) We are getting the json of an agency metric which will only have
        dimension_to_enabled_status populated."""
        dimensions = []
        if self.dimension_to_enabled_status is not None:
            for dimension, status in self.dimension_to_enabled_status.items():
                json = {
                    "key": dimension.to_enum().value,
                    "label": dimension.dimension_value,
                    "enabled": status,
                }
                if (
                    dimension_to_includes_excludes is not None
                    and entry_point == DatapointGetRequestEntryPoint.METRICS_TAB
                ):
                    json["settings"] = self.to_included_excluded_json(
                        dimension=dimension,
                        includes_excludes_definition=dimension_to_includes_excludes.get(
                            dimension
                        ),
                    )
                if (
                    self.dimension_to_value is not None
                    and entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
                ):
                    # if there is a non-null dimension_to_value dictionary, add dimension
                    # values into the json
                    json["value"] = self.dimension_to_value.get(dimension)

                elif (
                    self.dimension_to_value is None
                    and entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
                ):
                    raise JusticeCountsServerError(
                        code="no_dimension_values",
                        description=f"Metric {dimension.to_enum().value} has no dimension values",
                    )
                dimensions.append(json)
        return dimensions

    def to_included_excluded_json(
        self,
        dimension: DimensionBase,
        includes_excludes_definition: Optional[IncludesExcludesSet] = None,
    ) -> List[Dict[str, Any]]:
        """Returns a json list of include_exclude settings for a dimension."""

        includes_excludes_list: List[Dict[str, str]] = []
        if includes_excludes_definition is None:
            return includes_excludes_list

        # Example: {SettingEnum.SETTING_1: IncludesExcludesSetting.YES,
        # SettingEnum.SETTING_2: IncludesExcludesSetting.No, ...}
        actual_member_to_includes_excludes_setting = (
            self.dimension_to_includes_excludes_member_to_setting.get(dimension, {})
        )

        for (
            member,
            default_setting,
        ) in includes_excludes_definition.member_to_default_inclusion_setting.items():
            included = actual_member_to_includes_excludes_setting.get(member)
            includes_excludes_list.append(
                {
                    "key": member.name,
                    "label": member.value,
                    "included": included.value
                    if included is not None
                    else default_setting.value,
                    "default": default_setting.value,
                }
            )
        return includes_excludes_list

    ### Validations ###

    @dimension_to_value.validator
    def validate(self, _attribute: attr.Attribute, value: Any) -> None:
        # Validate that all dimensions enum instances in the dictionary belong
        # to the same dimension enum class
        if self.dimension_to_value is None:
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
