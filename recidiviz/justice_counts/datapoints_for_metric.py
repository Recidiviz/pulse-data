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
"""Interface for working with the Datapoint model."""
import enum
from collections import defaultdict
from typing import Any, Dict, List, Optional

import attr

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    IncludesExcludesSet,
    IncludesExcludesSetting,
    MetricDefinition,
)
from recidiviz.justice_counts.metrics.metric_interface import (
    MetricAggregatedDimensionData,
    MetricContextData,
)
from recidiviz.justice_counts.utils.datapoint_utils import get_dimension
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.types import assert_type


@attr.define
class DatapointsForMetric:
    """Class that aggregates all datapoints that correspond to the same Metric Definition,
    and slots them to their corresponding category (aggregate value, dimension, context).
    Serves as an untermediate step between fetching Datapoints from the DB and formatting
    them as MetricInterfaces (which we then convert to JSON).
    """

    is_metric_enabled: bool = attr.field(default=True)
    aggregated_value: Optional[int] = None

    context_key_to_agency_datapoint: Dict[str, schema.Datapoint] = attr.field(
        factory=dict[str, schema.Datapoint]
    )
    dimension_id_to_agency_datapoints: Dict[str, List[schema.Datapoint]] = attr.field(
        factory=(lambda: defaultdict(list))
    )
    context_key_to_report_datapoint: Dict[str, schema.Datapoint] = attr.field(
        factory=dict[str, schema.Datapoint]
    )
    dimension_id_to_report_datapoints: Dict[str, List[schema.Datapoint]] = attr.field(
        factory=(lambda: defaultdict(list))
    )

    # includes_excludes_key_to_datapoint will hold includes/excludes
    # datapoints that at the metric level.
    includes_excludes_key_to_datapoint: Dict[str, schema.Datapoint] = attr.field(
        default=None
    )
    # dimension_id_to_includes_excludes_key_to_datapoint will hold
    # includes/excludes datapoints that at the dimension level.
    dimension_id_to_includes_excludes_key_to_datapoint: Dict[
        str, Dict[str, schema.Datapoint]
    ] = attr.field(factory=(lambda: defaultdict(dict)))

    ### Top level methods used to construct MetricInterface ###

    def get_reported_contexts(
        self, metric_definition: MetricDefinition
    ) -> List[MetricContextData]:
        """
        - This method first determines which contexts we expect for this dimension definition
        - Then it looks at the contexts already reported in the database or saved as pre-filled
        - recurring context options, and fills in any of the expected contexts that have already
        - been reported with their reported value.
        """
        contexts = []
        for context in metric_definition.contexts:
            value = None
            report_datapoint = self.context_key_to_report_datapoint.get(
                context.key.value
            )
            report_value = (
                report_datapoint.get_value() if report_datapoint is not None else None
            )
            report_status = (
                report_datapoint.report.status
                if report_datapoint is not None
                else schema.ReportStatus.NOT_STARTED
            )
            agency_datapoint = self.context_key_to_agency_datapoint.get(
                context.key.value
            )
            agency_value = (
                agency_datapoint.get_value() if agency_datapoint is not None else None
            )
            # If no data has been reported, the metric is enabled, AND the report is
            # unpublished, fill context with the value of the agency datapoint.
            value = (
                agency_value
                if report_value is None
                and self.is_metric_enabled is True
                and report_status != schema.ReportStatus.PUBLISHED
                else report_value
            )
            contexts.append(MetricContextData(key=context.key, value=value))
        return contexts

    def get_aggregated_dimension_data(
        self, metric_definition: MetricDefinition
    ) -> List[MetricAggregatedDimensionData]:
        """
        - This method first looks at all dimensions that have already been reported in
        the database, and extracts them into a dictionary dimension_id_to_dimension_values_dicts.
        - As we fill out this dictionary, we make sure that each dimension_values dict is "complete",
        i.e. contains all member values for that dimension. If one of the members hasn't been reported yet, it's value will be None.
        - It's possible that not all dimensions we expect for this metric are in this dictionary,
        i.e. if the user hasn't filled out any values for a particular dimension yet.
        - Thus, at the end of the function, we look at all the dimensions that are expected for this metric,
        and if one doesn't exist in the dictionary, we add it with all values set to None.
        """
        aggregated_dimensions = []
        dimension_id_to_dimension_to_enabled_status = (
            self._get_dimension_id_to_dimension_to_enabled_status_dict(
                metric_definition=metric_definition,
            )
        )

        dimension_id_to_dimension_to_value = (
            self._get_dimension_id_to_dimension_to_value_dict(
                metric_definition=metric_definition,
            )
        )

        for aggregated_dimension in metric_definition.aggregated_dimensions or []:
            aggregated_dimensions.append(
                MetricAggregatedDimensionData(
                    dimension_to_value=dimension_id_to_dimension_to_value.get(
                        aggregated_dimension.dimension_identifier()
                    ),
                    dimension_to_enabled_status=dimension_id_to_dimension_to_enabled_status.get(
                        aggregated_dimension.dimension_identifier()
                    ),
                    dimension_to_includes_excludes_member_to_setting=self._get_dimension_to_includes_excludes_member_to_setting(
                        aggregated_dimension_definition=aggregated_dimension
                    ),
                ),
            )

        return aggregated_dimensions

    def get_includes_excludes_dict(
        self,
        includes_excludes_set: Optional[IncludesExcludesSet] = None,
        dimension_id: Optional[str] = None,
    ) -> Dict[enum.Enum, Optional[IncludesExcludesSetting]]:
        """Returns the includes/excludes dicts. This is used to populate
        the includes_excludes dict at the metric level and at the
        dimension level."""
        includes_excludes_dict: Dict[enum.Enum, Optional[IncludesExcludesSetting]] = {}
        if includes_excludes_set is None:
            return includes_excludes_dict

        if dimension_id is None and self.includes_excludes_key_to_datapoint is None:
            return includes_excludes_dict

        if (
            dimension_id is not None
            and self.dimension_id_to_includes_excludes_key_to_datapoint.get(
                dimension_id
            )
            is None
        ):
            return includes_excludes_dict

        includes_excludes_key_to_datapoint = (
            self.includes_excludes_key_to_datapoint
            if dimension_id is None
            else self.dimension_id_to_includes_excludes_key_to_datapoint.get(
                dimension_id
            )
        )
        for (
            member,
            default,
        ) in includes_excludes_set.member_to_default_inclusion_setting.items():
            datapoint = assert_type(includes_excludes_key_to_datapoint, dict).get(
                member.name
            )
            includes_excludes_dict[member] = (
                IncludesExcludesSetting(datapoint.value)
                if datapoint is not None
                else default
            )
        return includes_excludes_dict

    ### Helpers ###

    def _get_dimension_to_includes_excludes_member_to_setting(
        self, aggregated_dimension_definition: AggregatedDimension
    ) -> Dict[DimensionBase, Dict[enum.Enum, Optional[IncludesExcludesSetting]]]:
        """This method returns an includes_excludes_member_to_setting dictionary
        that is populated from datapoints that represent includes/excludes settings
        at the dimension level."""
        if aggregated_dimension_definition.dimension_to_includes_excludes is None:
            return {}

        dimension_to_includes_excludes_member_to_setting: Dict[
            DimensionBase, Dict[enum.Enum, Optional[IncludesExcludesSetting]]
        ] = defaultdict(dict)
        for dimension in DIMENSION_IDENTIFIER_TO_DIMENSION[
            aggregated_dimension_definition.dimension_identifier()
        ]:
            includes_excludes_set = (
                aggregated_dimension_definition.dimension_to_includes_excludes.get(
                    dimension
                )
            )
            dimension_to_includes_excludes_member_to_setting[
                dimension
            ] = self.get_includes_excludes_dict(
                includes_excludes_set=includes_excludes_set,
                dimension_id=dimension.dimension_identifier(),
            )

        return dimension_to_includes_excludes_member_to_setting

    def _get_dimension_id_to_dimension_to_value_dict(
        self, metric_definition: MetricDefinition
    ) -> Dict[str, Dict[DimensionBase, Any]]:
        return self._get_dimension_id_to_dimension_dicts(
            metric_definition=metric_definition, create_dimension_to_value_dict=True
        )

    def _get_dimension_id_to_dimension_to_enabled_status_dict(
        self,
        metric_definition: MetricDefinition,
    ) -> Dict[str, Dict[DimensionBase, Any]]:
        return self._get_dimension_id_to_dimension_dicts(
            metric_definition=metric_definition,
            create_dimension_to_value_dict=False,
        )

    def _get_dimension_id_to_dimension_dicts(
        self,
        metric_definition: MetricDefinition,
        create_dimension_to_value_dict: bool,
    ) -> Dict[str, Dict[DimensionBase, Optional[Any]]]:
        """Helper method that returns dimension_to_value and dimension_to_enabled_status
        dictionaries. If create_dimension_to_value_dict is true, then this method will
        return a dimension_id -> dimension_to_value dictionary. If not, it will return
        dimension_id -> dimension_to_enabled_status."""

        # dimension_id_to_dimension_values_dicts maps dimension identifier to their
        # corresponding dimension_to_values dictionary
        # e.g global/gender/restricted -> {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20...}
        dimension_id_to_dimension_dicts: Dict[
            str, Dict[DimensionBase, Optional[Any]]
        ] = {
            aggregated_dimension.dimension_identifier(): {
                d: None if create_dimension_to_value_dict else True
                for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                    aggregated_dimension.dimension_identifier()
                ]
            }
            for aggregated_dimension in metric_definition.aggregated_dimensions or []
        }

        dimension_id_to_datapoint: Dict[str, List[schema.Datapoint]] = (
            self.dimension_id_to_report_datapoints
            if create_dimension_to_value_dict is True
            else self.dimension_id_to_agency_datapoints
        )
        # When creating a dimension_to_value dictionary we're dealing with a report datapoint
        # the value we're interested in is the actual data, whereas if we're creating
        # dimension_to_enabled_status with an agency datapoint, the value we're interested
        # in is the enabled status.
        for dimension_id, dimension_datapoints in dimension_id_to_datapoint.items():
            for datapoint in dimension_datapoints:

                dimension_enum_member = get_dimension(datapoint=datapoint)

                curr_dimension_dict = dimension_id_to_dimension_dicts.get(
                    dimension_id
                )  # example: curr_dimension_dict = {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: None, GenderRestricted.NON_BINARY: None...}

                if (
                    dimension_enum_member is not None
                    and curr_dimension_dict is not None
                ):
                    curr_dimension_dict[dimension_enum_member] = (
                        datapoint.get_value()
                        if create_dimension_to_value_dict
                        else datapoint.enabled
                    )
                    # update curr_dimension_to_values to add new dimension datapoint.
                    # example: curr_dimension_to_values = {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20, GenderRestricted.NON_BINARY: None...}
                    dimension_id_to_dimension_dicts[dimension_id] = curr_dimension_dict
                    # update dimension_id_to_dimension_values_dicts dictionary -> {"global/gender/restricted": {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20, GenderRestricted.NON_BINARY: None...}
        return dimension_id_to_dimension_dicts
