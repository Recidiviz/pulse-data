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
import datetime
from typing import Any, Dict, List, Optional

import attr
from sqlalchemy.orm import Session

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.exceptions import JusticeCountsDataError
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_interface import (
    MetricAggregatedDimensionData,
    MetricContextData,
    MetricInterface,
)
from recidiviz.justice_counts.utils.persistence_utils import (
    delete_existing,
    expunge_existing,
    get_existing_entity,
    update_existing_or_create,
)
from recidiviz.persistence.database.schema.justice_counts import schema


class DatapointInterface:
    """Contains methods for working with Datapoint.
    Datapoints can either be numeric metric values, or contexts associated with a metric.
    In either case, metric_definition_key indicates which metric the datapoint applies to.
    If context_key is not None, the datapoint is a context. value_type will be numeric for non-context
    datapoints, and otherwise will indicate the type of context. If dimension_identifier_to_member is not
    None the numeric value applies to a particular dimension.
    """

    @staticmethod
    def get_agency_datapoints(
        session: Session,
        agency_id: int,
    ) -> List[schema.Datapoint]:
        return (
            session.query(schema.Datapoint)
            .filter(schema.Datapoint.source_id == agency_id)
            .all()
        )

    @staticmethod
    def get_metric_settings_by_agency(
        session: Session,
        agency: schema.Agency,
    ) -> List[MetricInterface]:
        """Returns a list of MetricInterfaces representing agency datapoints
        that represent metric settings - not metric data - for the agency provided."""
        agency_datapoints = DatapointInterface.get_agency_datapoints(
            session=session, agency_id=agency.id
        )

        metric_definitions = MetricInterface.get_metric_definitions(
            systems={schema.System[system] for system in agency.systems or []}
        )

        metric_key_to_data_points = DatapointInterface.build_metric_key_to_datapoints(
            datapoints=agency_datapoints
        )

        agency_metrics = []
        # For each metric associated with this agency, construct a MetricInterface object

        for metric_definition in metric_definitions:
            datapoints = metric_key_to_data_points.get(
                metric_definition.key,
                DatapointInterface.DatapointsForMetricDefinition(),
            )
            agency_metrics.append(
                MetricInterface(
                    key=metric_definition.key,
                    is_metric_enabled=datapoints.is_metric_enabled,
                    contexts=datapoints.get_reported_contexts(
                        # convert context datapoints to MetricContextData
                        metric_definition=metric_definition
                    ),
                    aggregated_dimensions=datapoints.get_aggregated_dimension_data(
                        # convert dimension datapoints to MetricAggregatedDimensionData
                        metric_definition=metric_definition
                    ),
                )
            )
        return agency_metrics

    @staticmethod
    def add_or_update_agency_datapoints(
        session: Session,
        agency: schema.Agency,
        agency_metric: MetricInterface,
    ) -> None:
        """
        Agency datapoints are not used to store data, like report datapoints are; rather, they are used to
        indicate which metrics are enabled for an agency, and what their default contexts should be.
        Agency datapoints have an agency_id in the source_id column and a null value in the report column.

        This method takes in the MetricInterface for a single metric, which might then get converted to several datapoints.
        This method serves two purposes:
        1) To update agency datapoints such that we can query the table and see what
        metrics are enabled and disabled.
            -  If a whole metric is disabled, an agency datapoint will have a null value
            in the context key column, a null value in the dimension_identifier_to_member
            column and a False value in the enabled column.
            -  If a metric breakdown is disabled, an agency datapoint will have a null value
            in the context key column, the corresponding dimension value in the dimension_identifier_to_member
            column and a False value in the enabled column. If all categories are disabled, that means the
            disaggregation is disabled.

        2) To update agency context datapoints with default values that can be used on every report.

        """

        if agency_metric.is_metric_enabled is False:
            # Turn off the whole metric
            update_existing_or_create(
                ingested_entity=schema.Datapoint(
                    metric_definition_key=agency_metric.key,
                    source=agency,
                    enabled=False,
                    dimension_identifier_to_member=None,
                ),
                session=session,
            )

        if agency_metric.is_metric_enabled is True:
            # When metric is re-enabled, delete corresponding agency datapoint.
            delete_existing(
                session=session,
                ingested_entity=schema.Datapoint(
                    source=agency,
                    metric_definition_key=agency_metric.key,
                    dimension_identifier_to_member=None,
                ),
                entity_cls=schema.Datapoint,
            )

            # Pre-fill contexts
            for context in agency_metric.contexts:
                update_existing_or_create(
                    schema.Datapoint(
                        metric_definition_key=agency_metric.key,
                        source=agency,
                        context_key=context.key.value,
                        value=context.value,
                        dimension_identifier_to_member=None,
                    ),
                    session,
                )

            for aggregated_dimension in agency_metric.aggregated_dimensions:
                if aggregated_dimension.dimension_to_enabled_status is not None:
                    for (
                        dimension,
                        is_dimension_enabled,
                    ) in aggregated_dimension.dimension_to_enabled_status.items():
                        # If is_dimension_enabled is None, then there are no deltas associated
                        # with a dimension datapoint and there is no need to update/create/delete
                        # the datapoint.
                        if is_dimension_enabled is False:
                            update_existing_or_create(
                                ingested_entity=schema.Datapoint(
                                    metric_definition_key=agency_metric.key,
                                    source=agency,
                                    dimension_identifier_to_member={
                                        dimension.dimension_identifier(): dimension.dimension_name
                                    },
                                    enabled=False,
                                ),
                                session=session,
                            )
                        if is_dimension_enabled is True:
                            delete_existing(
                                session=session,
                                ingested_entity=schema.Datapoint(
                                    source=agency,
                                    metric_definition_key=agency_metric.key,
                                    dimension_identifier_to_member={
                                        dimension.dimension_identifier(): dimension.dimension_name
                                    },
                                ),
                                entity_cls=schema.Datapoint,
                            )
        session.commit()

    @staticmethod
    def add_datapoint(
        session: Session,
        report: schema.Report,
        value: Any,
        user_account: schema.UserAccount,
        metric_definition_key: str,
        current_time: datetime.datetime,
        context_key: Optional[ContextKey] = None,
        value_type: Optional[ValueType] = None,
        dimension: Optional[DimensionBase] = None,
        use_existing_aggregate_value: bool = False,
    ) -> Optional[schema.Datapoint]:
        """Given a Report and a MetricInterface, add a row to the datapoint table.
        All datapoints associated with a metric are saved, even if the value is None.

        The only exception to the above is if `use_existing_aggregate_value`
        is True. in this case, if `datapoint.value` is None, we ignore it,
        and fallback to whatever value is already in the db. If `datapoint.value`
        is specified, we validate that it matches what is already in the db.
        If nothing is in the DB, we save the new aggregate value.
        """

        # Don't save invalid datapoint values when publishing
        if (
            report.status == schema.ReportStatus.PUBLISHED
            and value is not None
            and (value_type is None or value_type == ValueType.NUMBER)
        ):
            try:
                float(value)
            except ValueError as e:
                raise JusticeCountsDataError(
                    code="invalid_datapoint_value",
                    description=(
                        "Datapoint represents a float value, but is a string. "
                        f"Datapoint ID: {report.id}, value: {value}"
                    ),
                ) from e

        ingested_entity = schema.Datapoint(
            value=str(value) if value is not None else value,
            report_id=report.id,
            metric_definition_key=metric_definition_key,
            context_key=context_key.value if context_key else None,
            value_type=value_type,
            start_date=report.date_range_start,
            end_date=report.date_range_end,
            report=report,
            dimension_identifier_to_member={
                dimension.dimension_identifier(): dimension.dimension_name
            }
            if dimension
            else None,
        )

        if use_existing_aggregate_value:
            # Validate that the incoming aggregate value matches what's already
            # in the db. If not, raise an error. If so, continue without making
            # any changes. If nothing is in the DB, save the new aggregate value.
            expunge_existing(session=session, ingested_entity=ingested_entity)
            existing_entity = get_existing_entity(ingested_entity, session)
            if (
                existing_entity is not None
                and abs(float(existing_entity.value) - value) > 1
            ):
                raise ValueError(
                    "`use_existing_aggregate_value was specified, "
                    "but the aggregate value either read or inferred from "
                    "incoming data does not match the existing aggregate value."
                )

        datapoint, existing_datapoint = update_existing_or_create(
            ingested_entity,
            session,
        )

        if existing_datapoint:
            if existing_datapoint.value != datapoint.value:
                datapoint_history = schema.DatapointHistory(
                    datapoint_id=existing_datapoint.id,
                    user_account_id=user_account.id,
                    timestamp=current_time,
                    old_value=existing_datapoint.value,
                    new_value=str(value) if value is not None else value,
                )

                datapoint.datapoint_histories.append(datapoint_history)
        return datapoint

    @attr.define
    class DatapointsForMetricDefinition:
        """Class that maps datapoints to their corresponding category (aggregate value, dimension, context)"""

        is_metric_enabled: bool = attr.field(default=True)
        aggregated_value: Optional[int] = None
        context_key_to_agency_datapoint: Dict[str, schema.Datapoint] = attr.field(
            factory=dict[str, schema.Datapoint]
        )
        dimension_id_to_agency_datapoints: Dict[
            str, List[schema.Datapoint]
        ] = attr.field(factory=dict[str, schema.Datapoint])
        context_key_to_report_datapoint: Dict[str, schema.Datapoint] = attr.field(
            factory=dict[str, schema.Datapoint]
        )
        dimension_id_to_report_datapoints: Dict[
            str, List[schema.Datapoint]
        ] = attr.field(factory=dict[str, schema.Datapoint])

        def get_reported_contexts(
            self, metric_definition: MetricDefinition
        ) -> List[MetricContextData]:
            """
            - This method first determines which contexts we expect for this dimension definition
            - Then it looks at the contexts already reported in the database or saved as pre-filled
            - recurring context options, and fills in any of the expected contexts that have already
            - been reported with their reported value
            """
            contexts = []
            for context in metric_definition.contexts:
                value = None
                datapoint = (
                    self.context_key_to_report_datapoint.get(context.key.value)
                    if self.context_key_to_report_datapoint.get(context.key.value)
                    else self.context_key_to_agency_datapoint.get(context.key.value)
                )
                if datapoint is not None:
                    value = datapoint.get_value()
                contexts.append(MetricContextData(key=context.key, value=value))
            return contexts

        def get_dimension_id_to_dimension_dicts(
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
                disaggregation.dimension_identifier(): {
                    d: None if create_dimension_to_value_dict else True
                    for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                        disaggregation.dimension_identifier()
                    ]
                }
                for disaggregation in metric_definition.aggregated_dimensions or []
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
                    if (
                        len(datapoint.dimension_identifier_to_member) > 1  # type: ignore[attr-defined]
                    ):
                        raise JusticeCountsDataError(
                            code="invalid_datapoint",
                            description=(
                                "Datapoint represents multiple dimensions. "
                                f"Datapoint ID: {datapoint.id}."
                            ),
                        )

                    # example: dimension_name = "MALE"
                    dimension_name = list(
                        datapoint.dimension_identifier_to_member.values()
                    ).pop()

                    dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
                        dimension_id
                    ]  # example: dimension_class = GenderRestricted

                    curr_dimension_dict = dimension_id_to_dimension_dicts.get(
                        dimension_id
                    )  # example: curr_dimension_dict = {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: None, GenderRestricted.NON_BINARY: None...}

                    if curr_dimension_dict is not None:
                        curr_dimension_dict[dimension_class[dimension_name]] = (
                            datapoint.get_value()
                            if create_dimension_to_value_dict
                            else datapoint.enabled
                        )
                        # update curr_dimension_to_values to add new dimension datapoint.
                        # example: curr_dimension_to_values = {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20, GenderRestricted.NON_BINARY: None...}
                        dimension_id_to_dimension_dicts[
                            dimension_id
                        ] = curr_dimension_dict
                        # update dimension_id_to_dimension_values_dicts dictionary -> {"global/gender/restricted": {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20, GenderRestricted.NON_BINARY: None...}
            return dimension_id_to_dimension_dicts

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
            dimension_id_to_value_dicts = {}
            dimension_id_to_enabled_status_dicts = {}
            if len(self.dimension_id_to_report_datapoints) > 0:
                dimension_id_to_value_dicts = self.get_dimension_id_to_dimension_dicts(
                    create_dimension_to_value_dict=True,
                    metric_definition=metric_definition,
                )
            if len(self.dimension_id_to_agency_datapoints) > 0:
                dimension_id_to_enabled_status_dicts = (
                    self.get_dimension_id_to_dimension_dicts(
                        create_dimension_to_value_dict=False,
                        metric_definition=metric_definition,
                    )
                )

            for aggregated_dimension in metric_definition.aggregated_dimensions or []:
                aggregated_dimensions.append(
                    MetricAggregatedDimensionData(
                        dimension_to_value=dimension_id_to_value_dicts.get(
                            aggregated_dimension.dimension_identifier(),
                            {
                                d: None
                                for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                                    aggregated_dimension.dimension.dimension_identifier()
                                ]
                            },
                        ),
                        dimension_to_enabled_status=dimension_id_to_enabled_status_dicts.get(
                            aggregated_dimension.dimension_identifier(),
                            {
                                d: self.is_metric_enabled
                                for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                                    aggregated_dimension.dimension.dimension_identifier()
                                ]
                            },
                        ),
                    )
                )

            return aggregated_dimensions

    @staticmethod
    def build_metric_key_to_datapoints(
        datapoints: List[schema.Datapoint],
    ) -> Dict[str, DatapointsForMetricDefinition]:
        """Associate the datapoints with their metric and sort each datapoint by what
        they represent (context, dimension, or aggregated_value). metric_key_to_data_points
        is a dictionary of DatapointsForMetricDefinition. Each metric definition key points to a
        DatapointsForMetricDefinition instance that stores datapoints by context, disaggregations,
        or aggregated_value and by their classification as a report or agency datapoint.

        This method will be called from two places: 1) getting a report and 2) getting the metric tab.
        In the first case, the datapoints input will include agency and report datapoints, and in
        the second case, it will just include agency datapoints.
        """
        metric_key_to_data_points = {}
        for datapoint in datapoints:
            # if no DatapointsForMetricDefinition exists for the metric definition, create one.
            if datapoint.metric_definition_key not in metric_key_to_data_points:
                metric_key_to_data_points[
                    datapoint.metric_definition_key
                ] = DatapointInterface.DatapointsForMetricDefinition()
            metric_datapoints = metric_key_to_data_points[
                datapoint.metric_definition_key
            ]
            if datapoint.context_key is not None:
                key = datapoint.context_key
                # If a datapoint represents a context, add it into a dictionary
                # formatted as {context_key: datapoint}
                if datapoint.report is not None:
                    metric_datapoints.context_key_to_report_datapoint[key] = datapoint
                elif datapoint.source is not None:
                    metric_datapoints.context_key_to_agency_datapoint[key] = datapoint
            elif datapoint.dimension_identifier_to_member is not None:
                dimension_identifier = list(
                    datapoint.dimension_identifier_to_member.keys()
                ).pop()
                # If a datapoint represents a aggregated_dimension, add it into a dictionary
                # formatted as {dimension_identifier: [all datapoints with same dimension identifier...]}
                if datapoint.report is not None:
                    if (
                        dimension_identifier
                        not in metric_datapoints.dimension_id_to_report_datapoints
                    ):
                        metric_datapoints.dimension_id_to_report_datapoints[
                            dimension_identifier
                        ] = []
                    metric_datapoints.dimension_id_to_report_datapoints[
                        dimension_identifier
                    ].append(datapoint)
                elif datapoint.source is not None:
                    if (
                        dimension_identifier
                        not in metric_datapoints.dimension_id_to_agency_datapoints
                    ):
                        metric_datapoints.dimension_id_to_agency_datapoints[
                            dimension_identifier
                        ] = []
                    metric_datapoints.dimension_id_to_agency_datapoints[
                        dimension_identifier
                    ].append(datapoint)
            elif (
                datapoint.dimension_identifier_to_member is None
                and datapoint.context_key is None
            ):
                if datapoint.report is not None:
                    # If a datapoint has a report attached to it and has no context key or
                    # dimension_identifier_to_member value, it represents the reported aggregate value
                    # of a metric.
                    metric_datapoints.aggregated_value = datapoint.get_value()
                if datapoint.source is not None:
                    # If a datapoint has a source attached to it and has no context key or
                    # dimension_identifier_to_member value, it represents the weather or not the
                    # datapoint is enabled. is_metric_enabled defaults to True. If there is no
                    # corresponding agency datapoint, then the metric is on.
                    metric_datapoints.is_metric_enabled = datapoint.enabled
            else:
                raise JusticeCountsDataError(
                    code="invalid_datapoint",
                    description=(
                        "Datapoint does not represent a dimension, "
                        "aggregate value, or context.",
                    ),
                )
        return metric_key_to_data_points
