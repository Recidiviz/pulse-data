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
import enum
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import false
from sqlalchemy.orm import Session
from sqlalchemy.sql import case

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.datapoints_for_metric import DatapointsForMetric
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_definition import IncludesExcludesSetting
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import (
    DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
    REPORTING_FREQUENCY_CONTEXT_KEY,
    UploadMethod,
)
from recidiviz.justice_counts.utils.datapoint_utils import (
    filter_deprecated_datapoints,
    get_dimension,
    get_dimension_id,
    get_dimension_id_and_member,
    get_value,
)
from recidiviz.justice_counts.utils.persistence_utils import (
    expunge_existing,
    update_existing_or_create,
)
from recidiviz.persistence.database.schema.justice_counts import schema

# Datapoints are unique by a tuple of:
# <report start date, report end date, agency id, metric definition,
# context key, disaggregations>
DatapointUniqueKey = Tuple[
    datetime.date,
    datetime.date,
    int,
    str,
    Optional[str],
    Optional[str],
]


class DatapointInterface:
    """Contains methods for working with Datapoint.
    Datapoints can either be numeric metric values, or contexts associated with a metric.
    In either case, metric_definition_key indicates which metric the datapoint applies to.
    If context_key is not None, the datapoint is a context. value_type will be numeric for non-context
    datapoints, and otherwise will indicate the type of context. If dimension_identifier_to_member is not
    None the numeric value applies to a particular dimension.
    """

    ### Fetch from the DB ###

    @staticmethod
    def get_datapoints_by_report_ids(
        session: Session, report_ids: List[int], include_contexts: bool = True
    ) -> List[schema.Datapoint]:
        """Given a list of report ids, get all datapoints belonging to those reports.
        Filter out datapoints with a deprecated dimension identifier or value.
        """
        q = session.query(schema.Datapoint)

        # when fetching datapoints for data viz, no need to fetch context datapoints
        if include_contexts is False:
            q = q.filter(schema.Datapoint.context_key.is_(None))

        datapoints = (
            q.filter(schema.Datapoint.report_id.in_(report_ids))
            .order_by(schema.Datapoint.start_date.asc())
            .all()
        )
        return filter_deprecated_datapoints(datapoints=datapoints)

    # TODO(#28469): Deprecate/delete this after the MetricSetting migration.
    @staticmethod
    def get_agency_datapoints(
        session: Session,
        agency_id: int,
    ) -> List[schema.Datapoint]:
        """Given an agency id, get all "Agency Datapoints" -- i.e. datapoints
        that provide configuration information, rather than report data.
        Filter out datapoints with a deprecated dimension identifier or value.
        """
        datapoints = (
            session.query(schema.Datapoint).filter(
                schema.Datapoint.source_id == agency_id,
                schema.Datapoint.is_report_datapoint == false(),
            )
        ).all()
        return filter_deprecated_datapoints(datapoints=datapoints)

    @staticmethod
    def get_report_datapoints_for_agency_dashboard(
        session: Session,
        report_ids: List[int],
    ) -> List[schema.Datapoint]:
        """Returns report datapoints that we need to render the agency dashboard.
        To improve performance, rather than returning fully instantiated
        datapoint objects, we return a tuple of their properties.
        """
        datapoints = (
            session.query(*schema.Datapoint.__table__.columns)
            .filter(
                # Published report datapoints
                (schema.Datapoint.report_id.in_(report_ids))
            )
            .order_by(schema.Datapoint.start_date.asc())
        )
        return filter_deprecated_datapoints(datapoints=datapoints)

    ### Export to the FE ###

    @staticmethod
    def to_json_response(
        datapoint: schema.Datapoint,
        is_published: bool,
        frequency: schema.ReportingFrequency,
        old_value: Optional[str] = None,
        agency_name: Optional[str] = None,
        is_v2: Optional[bool] = False,
    ) -> DatapointJson:
        """Serializes Datapoint object into json format for consumption in the Justice Counts Control Panel"""
        metric_definition = METRIC_KEY_TO_METRIC[datapoint.metric_definition_key]
        metric_display_name = metric_definition.display_name

        disaggregation_display_name = None
        dimension_display_name = None
        dimension, success = get_dimension(datapoint)
        if not success:
            # These datapoints should have already been filtered out, so we should
            # never see this error.
            raise ValueError("Datapoint has deprecated dimension identifier or value.")

        if dimension is not None:
            disaggregation_display_name = dimension.human_readable_name()
            dimension_display_name = dimension.dimension_value

        response: DatapointJson = {
            "id": datapoint.id,
            "start_date": datapoint.start_date,
            "end_date": datapoint.end_date,
            "value": get_value(datapoint=datapoint),
            "frequency": frequency.value,
        }

        if is_v2 is True:
            if dimension is not None:
                response["disaggregation_display_name"] = disaggregation_display_name
                response["dimension_display_name"] = dimension_display_name

            return response

        response["agency_name"] = agency_name
        response["metric_definition_key"] = datapoint.metric_definition_key
        response["metric_display_name"] = metric_display_name
        response["disaggregation_display_name"] = disaggregation_display_name
        response["dimension_display_name"] = dimension_display_name
        response["old_value"] = (
            get_value(datapoint=datapoint, use_value=old_value)
            if old_value is not None
            else None
        )
        response["is_published"] = is_published
        response["report_id"] = datapoint.report_id
        return response

    ### Get Path: Both Agency and Report Datapoints ###

    @staticmethod
    def join_report_datapoints_to_metric_interfaces(
        report_datapoints: List[schema.Datapoint],
        metric_key_to_metric_interface: Dict[str, MetricInterface],
    ) -> Dict[str, MetricInterface]:
        """
        Populates the MetricInterfaces in `metric_key_to_metric_interface` with the values
        stored in `report_datapoints`.
        Expects that the MetricInterfaces in `metric_key_to_metric_interface` contains all
        metric settings that an agency reports for, but that their report datapoint
        fields are empty (`value` and `aggregated_dimensions.dimension_to_value`).
        """
        for datapoint in report_datapoints:
            if datapoint.is_report_datapoint is False:
                raise ValueError(
                    f"Expected is_report_datapoint to be True. Instead got {datapoint.is_report_datapoint}."
                )
            if datapoint.context_key is not None:
                # There are some deprecated report datapoints that used to store context
                # for report data. Skip these.
                continue

            # If an agency has report datapoints for metrics they have not configured
            # yet, we will create an empty metric interface for this metric.
            if datapoint.metric_definition_key not in metric_key_to_metric_interface:
                metric_key_to_metric_interface[
                    datapoint.metric_definition_key
                ] = MetricInterface(key=datapoint.metric_definition_key)

            metric_interface = metric_key_to_metric_interface[
                datapoint.metric_definition_key
            ]

            # Populate top-level metric.
            if datapoint.dimension_identifier_to_member is None:
                metric_interface.value = get_value(datapoint=datapoint)
                continue

            # Populate breakdown metric.
            dimension_enum_member, success = get_dimension(datapoint=datapoint)
            if not success:
                # This datapoint has a deprecated dimension identifier or value,
                # so just skip over it.
                continue
            if dimension_enum_member is None:
                raise JusticeCountsServerError(
                    code="invalid_datapoint",
                    description=(
                        "Report datapoint does not represent a dimension or an "
                        "aggregate value."
                    ),
                )
            dimension_in_metric_interface: list[MetricAggregatedDimensionData] = list(
                filter(
                    lambda x, dim=get_dimension_id(datapoint=datapoint): dim  # type: ignore[arg-type]
                    == x.dimension_identifier(),
                    metric_interface.aggregated_dimensions,
                )
            )
            if len(dimension_in_metric_interface) > 1:
                raise ValueError(
                    "A metric interface must only have one dimension entry"
                    "in aggregated_dimension per type."
                )

            # The dimension is in the metric interface's `aggregated_dimensions`.
            if len(dimension_in_metric_interface) == 1:
                if dimension_in_metric_interface[0].dimension_to_value is None:
                    dimension_in_metric_interface[0].dimension_to_value = {}
                dimension_in_metric_interface[0].dimension_to_value[
                    dimension_enum_member
                ] = get_value(datapoint=datapoint)
                continue

            # Dimension is not in `aggregated_dimensions`. Add a new entry for it.
            dimension_data = MetricAggregatedDimensionData()
            dimension_data.dimension_to_value = {
                dimension_enum_member: get_value(datapoint=datapoint)
            }
            metric_interface.aggregated_dimensions.append(dimension_data)

        return metric_key_to_metric_interface

    # TODO(#28469): Deprecate/delete this after the MetricSetting migration.
    @staticmethod
    def build_metric_key_to_datapoints(
        datapoints: List[schema.Datapoint],
    ) -> Dict[str, DatapointsForMetric]:
        """Associate the datapoints with their metric and sort each datapoint by what
        they represent (context, dimension, or aggregated_value). metric_key_to_data_points
        is a dictionary of DatapointsForMetric. Each metric definition key points to a
        DatapointsForMetric instance that stores datapoints by context, disaggregations,
        or aggregated_value and by their classification as a report or agency datapoint.

        This method will be called from two places: 1) getting a report and 2) getting the metric tab.
        In the first case, the datapoints input will include agency and report datapoints, and in
        the second case, it will just include agency datapoints.
        """
        metric_key_to_data_points: Dict[str, DatapointsForMetric] = defaultdict(
            DatapointsForMetric
        )
        for datapoint in datapoints:
            metric_datapoints = metric_key_to_data_points[
                datapoint.metric_definition_key
            ]

            # CONTEXTS
            if datapoint.context_key is not None:
                key = datapoint.context_key
                # Note: Report-level contexts are deprecated!
                if datapoint.is_report_datapoint is False:
                    # If a datapoint represents a context, add it into a dictionary
                    # formatted as {context_key: datapoint}
                    if datapoint.context_key == REPORTING_FREQUENCY_CONTEXT_KEY:
                        # contexts special case 1
                        metric_datapoints.custom_reporting_frequency = (
                            CustomReportingFrequency.from_datapoint(datapoint=datapoint)
                        )
                    elif (
                        datapoint.context_key == DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS
                    ):
                        # contexts special case 2
                        metric_datapoints.disaggregated_by_supervision_subsystems = (
                            datapoint.value == "True"
                        )
                    elif datapoint.dimension_identifier_to_member is not None:
                        # general contexts for particular dimensions
                        (
                            dimension_id,
                            dimension_member,
                        ) = get_dimension_id_and_member(datapoint=datapoint)
                        if dimension_id is not None:
                            dimension = DIMENSION_IDENTIFIER_TO_DIMENSION[dimension_id][
                                dimension_member  # type: ignore[index]
                            ]
                            metric_datapoints.dimension_to_context_key_to_datapoints[
                                dimension
                            ] = {datapoint.context_key: datapoint}
                    else:
                        # general contexts for top level metric
                        metric_datapoints.context_key_to_agency_datapoint[
                            key
                        ] = datapoint
            # INCLUDES / EXCLUDES
            elif datapoint.includes_excludes_key is not None:
                if datapoint.dimension_identifier_to_member is not None:
                    # If a datapoint represents an includes/excludes setting at the dimension level,
                    # add it into a dictionary formatted as {dimension_id: {includes_excludes_key: datapoint}}
                    (
                        dimension_id,
                        dimension_member,
                    ) = get_dimension_id_and_member(datapoint=datapoint)
                    if dimension_member is None or dimension_id is None:
                        raise JusticeCountsServerError(
                            code="invalid_datapoint",
                            description=(
                                "Datapoint representing a breakdown does not have a valid, "
                                "dimension member or id."
                            ),
                        )
                    dimension = DIMENSION_IDENTIFIER_TO_DIMENSION[dimension_id][
                        dimension_member
                    ]  # type: ignore[misc]
                    metric_datapoints.dimension_to_includes_excludes_key_to_datapoint[
                        dimension
                    ][datapoint.includes_excludes_key] = datapoint
                elif datapoint.dimension_identifier_to_member is None:
                    # If a datapoint represents an includes/excludes setting at the metric level,
                    # add it into a dictionary formatted as {includes_excludes_key: datapoint}
                    if metric_datapoints.includes_excludes_key_to_datapoint is None:
                        metric_datapoints.includes_excludes_key_to_datapoint = {}
                    metric_datapoints.includes_excludes_key_to_datapoint[
                        datapoint.includes_excludes_key
                    ] = datapoint

            # DIMENSIONS
            elif datapoint.dimension_identifier_to_member is not None:
                # If a datapoint represents an aggregated_dimension, add it into a dictionary
                # formatted as {dimension_identifier: [all datapoints with same dimension identifier...]}
                dimension_identifier = list(
                    datapoint.dimension_identifier_to_member.keys()
                ).pop()
                if datapoint.report_id is not None:
                    metric_datapoints.dimension_id_to_report_datapoints[
                        dimension_identifier
                    ].append(datapoint)
                elif datapoint.is_report_datapoint is False:
                    metric_datapoints.dimension_id_to_agency_datapoints[
                        dimension_identifier
                    ].append(datapoint)

            # TOP-LEVEL METRIC
            elif datapoint.dimension_identifier_to_member is None:
                if datapoint.report_id is not None:
                    # If a datapoint has a report attached to it and has no context key or
                    # dimension_identifier_to_member value, it represents the reported aggregate value
                    # of a metric.
                    metric_datapoints.aggregated_value = get_value(datapoint=datapoint)
                if datapoint.is_report_datapoint is False:
                    # If a datapoint has a source attached to it and has no context key or
                    # dimension_identifier_to_member value, it represents the weather or not the
                    # datapoint is enabled. is_metric_enabled defaults to True. If there is no
                    # corresponding agency datapoint, then the metric is on.
                    metric_datapoints.is_metric_enabled = datapoint.enabled

            else:
                raise JusticeCountsServerError(
                    code="invalid_datapoint",
                    description=(
                        "Datapoint does not represent a dimension, "
                        "aggregate value, or context."
                    ),
                )
        return metric_key_to_data_points

    ### Save Path: Report Datapoints ###

    @staticmethod
    def add_report_datapoint(
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        report: schema.Report,
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
        value: Any,
        metric_definition_key: str,
        current_time: datetime.datetime,
        upload_method: UploadMethod,
        context_key: Optional[ContextKey] = None,
        value_type: Optional[ValueType] = None,
        dimension: Optional[DimensionBase] = None,
        uploaded_via_breakdown_sheet: bool = False,
        user_account: Optional[schema.UserAccount] = None,
        agency: Optional[schema.Agency] = None,
    ) -> Optional[DatapointJson]:
        """
        Given a Report and a MetricInterface, add the new datapoint to either the
        `inserts` or `updates` lists.

        * If the datapoint is new (not found in `existing_datapoints_dict`), we
        will add it to `inserts`.

        * If the datapoint is updating an existing datapoint, we add it to `updates`.
        The datapoints in `inserts` and `updates` are not written to the database in
        this method. Instead, the user must call `flush_report_datapoints()` which will
        bulk insert/update them to the datapoint table.

        * For each `update`, we also record a DatapointHistory entry in `histories` which
        also must be passed to `flush_report_datapoints()` for writing.

        All datapoints associated with a metric will be saved, even if the value is None.
        The only exception to the above is if `uploaded_via_breakdown_sheet`
        is True. In this case, if `datapoint.value` is None, we ignore it. If
        `datapoint.value` is specified, prefer the existing value in the db, unless
        there isn't one, in which case we save the incoming value.
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
                raise JusticeCountsServerError(
                    code="invalid_datapoint_value",
                    description=(
                        "Datapoint represents a float value, but is a string. "
                        f"Datapoint ID: {report.id}, value: {value}"
                    ),
                ) from e

        # Check if there is an existing datapoint that needs to be updated,
        # or if we need to create a new one. Datapoints are unique by a tuple of:
        # <report, metric definition, context key, disaggregations>
        datapoint_key = (
            report.date_range_start,
            report.date_range_end,
            report.source_id,
            metric_definition_key,
            context_key.value if context_key else None,
            (
                json.dumps({dimension.dimension_identifier(): dimension.dimension_name})
                if dimension
                else None
            ),
        )
        existing_datapoint = existing_datapoints_dict.get(datapoint_key)
        if uploaded_via_breakdown_sheet:
            # If this flag is set and there is an existing aggregate value, keep the
            # existing aggregate value.
            if existing_datapoint is not None and existing_datapoint.value is not None:
                logging.info(
                    "An aggregate value already exists in the database. Keeping the existing value."
                )
                return None

        new_datapoint = schema.Datapoint(
            value=str(value) if value is not None else value,
            report_id=report.id,
            metric_definition_key=metric_definition_key,
            context_key=context_key.value if context_key else None,
            value_type=value_type,
            start_date=report.date_range_start,
            end_date=report.date_range_end,
            created_at=(
                current_time
                if existing_datapoint is None
                else existing_datapoint.created_at
            ),
            last_updated=current_time,
            report=report,
            dimension_identifier_to_member=(
                {dimension.dimension_identifier(): dimension.dimension_name}
                if dimension
                else None
            ),
            source_id=report.source_id,
            is_report_datapoint=True,
            upload_method=upload_method.value,
        )

        # Store the new datapoint in this dict, so that it can be
        # referenced later, e.g. an explicit aggregate total
        # will later be referenced by an inferred aggregate
        existing_datapoints_dict[datapoint_key] = new_datapoint

        # Creating the new datapoint might have added it to the session;
        # to avoid constraint violation errors, remove it before adding
        # it back later.
        expunge_existing(session, new_datapoint)

        if existing_datapoint is None:
            equal_to_existing = False
            inserts.append(new_datapoint)
        else:
            # Compare values using `get_value` so e.g. 3 == 3.0
            equal_to_existing = get_value(datapoint=new_datapoint) == get_value(
                datapoint=existing_datapoint
            )
            if not equal_to_existing:
                new_datapoint.id = existing_datapoint.id
                updates.append(new_datapoint)
                datapoint_history = schema.DatapointHistory(
                    datapoint_id=existing_datapoint.id,
                    user_account_id=(
                        user_account.id if user_account is not None else None
                    ),
                    timestamp=current_time,
                    old_value=existing_datapoint.value,
                    new_value=str(value) if value is not None else value,
                    old_upload_method=existing_datapoint.upload_method,
                    new_upload_method=upload_method.value,
                )
                # Creating the new datapoint history might have added it to the session;
                # to avoid constraint violation errors, remove it before adding
                # it back later.
                expunge_existing(session, datapoint_history)
                histories.append(datapoint_history)

        # Return datapoint json because datapoint values and metadata will be
        # used in the bulk upload data summary pages.
        return (
            DatapointInterface.to_json_response(
                datapoint=new_datapoint,
                is_published=report.status == schema.ReportStatus.PUBLISHED,
                frequency=schema.ReportingFrequency[report.type],
                old_value=(
                    existing_datapoint.value
                    if not equal_to_existing and existing_datapoint is not None
                    else None
                ),
                agency_name=agency.name if agency is not None else None,
            )
            if new_datapoint is not None
            else None
        )

    @staticmethod
    def flush_report_datapoints(
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
    ) -> None:
        """
        Bulk writes the datapoints in `inserts`, bulk updates the datapoints in
        `updates`, and bulk writes the datapoint_histories in `histories`.

        Clears the `inserts`, `updates`, and `histories` lists after writing.

        Make sure to call session.commit() at some point after flush_report_datapoints.
        """
        # Flush inserts
        if len(inserts) > 0:
            session.add_all(inserts)

        # Flush updates.
        # Only modifies `value`, `upload_method`, and `last_updated` columns since all
        # other columns stay constant for a report datapoint update.
        if len(updates) > 0:
            update_ids = [update.id for update in updates]
            success_count = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.id.in_(update_ids))
                .update(
                    {
                        schema.Datapoint.value: case(
                            {update.id: update.value for update in updates},
                            value=schema.Datapoint.id,
                        ),
                        schema.Datapoint.upload_method: case(
                            {update.id: update.upload_method for update in updates},
                            value=schema.Datapoint.id,
                        ),
                        schema.Datapoint.last_updated: case(
                            {update.id: update.last_updated for update in updates},
                            value=schema.Datapoint.id,
                        ),
                    },
                    # No attributes in this session to sync with.
                    synchronize_session=False,
                )
            )
            if success_count != len(update_ids):
                raise ValueError(
                    f"Bulk update failed. Updates not committed. Expected {len(update_ids)} updates but only committed {success_count} updates."
                )

        # Flush histories
        if len(histories) > 0:
            session.add_all(histories)

        # Clear inserts, updates, and histories.
        inserts.clear()
        updates.clear()
        histories.clear()

    ### Save Path: Agency Datapoints ###

    # TODO(#28469): Deprecate/delete this after the MetricSetting migration.
    @staticmethod
    def get_metric_settings_by_agency(
        session: Session,
        agency: schema.Agency,
        agency_datapoints: Optional[List[Any]] = None,
    ) -> List[MetricInterface]:
        """Returns a list of MetricInterfaces representing agency datapoints
        that represent metric settings - not metric data - for the agency provided."""
        if agency_datapoints is None:
            agency_datapoints = DatapointInterface.get_agency_datapoints(
                session=session, agency_id=agency.id
            )
        # To prevent circularity, we actually need this implemention to remain. We allow
        # ourselves a method which can convert agency datapoints into unpopulated MetricInterfaces.
        # This will be the same logic we use to build the script for parity in the
        # MetricSettings table.
        # TODO(#28389): Deprecate get_metric_settings_by_agency in favor of reading directly
        # from the MetricSetting table.
        metric_key_to_datapoints = DatapointInterface.build_metric_key_to_datapoints(
            datapoints=agency_datapoints
        )

        metric_definitions = MetricInterface.get_metric_definitions_by_systems(
            systems={schema.System[system] for system in agency.systems or []},
        )

        agency_metrics = []
        # For each metric associated with this agency, construct a MetricInterface object

        for metric_definition in metric_definitions:
            datapoints = metric_key_to_datapoints.get(
                metric_definition.key,
                DatapointsForMetric(),
            )

            # If this is a supervision subsystem metric, and the metric is not
            # supposed to be disaggregated by supervision subsystems, then
            # disable the metric
            enabled = datapoints.is_metric_enabled
            if datapoints.is_metric_enabled is None:
                if (
                    metric_definition.system in schema.System.supervision_subsystems()
                    and not datapoints.disaggregated_by_supervision_subsystems
                ):
                    enabled = False

            agency_metrics.append(
                MetricInterface(
                    key=metric_definition.key,
                    is_metric_enabled=enabled,
                    includes_excludes_member_to_setting=datapoints.get_includes_excludes_dict(
                        includes_excludes_set_lst=metric_definition.includes_excludes
                    ),
                    contexts=datapoints.get_agency_contexts(
                        # convert context datapoints to MetricContextData
                        metric_definition=metric_definition
                    ),
                    aggregated_dimensions=datapoints.get_aggregated_dimension_data(
                        # convert dimension datapoints to MetricAggregatedDimensionData
                        metric_definition=metric_definition
                    ),
                    custom_reporting_frequency=datapoints.custom_reporting_frequency,
                    disaggregated_by_supervision_subsystems=(
                        False
                        if metric_definition.system == schema.System.SUPERVISION
                        and datapoints.disaggregated_by_supervision_subsystems is None
                        else datapoints.disaggregated_by_supervision_subsystems
                    ),
                )
            )
        return agency_metrics

    # TODO(#28469): Deprecate/delete this after the MetricSetting migration.
    @staticmethod
    def record_agency_datapoint_history(
        session: Session,
        datapoint_before_update: Optional[schema.Datapoint],
        datapoint_after_update: schema.Datapoint,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        """
        Record metric datapoint changes in the DatapointHistory table. We won't record
        the following fields because they remain the same across modifications:
            -> source_id
            -> dimension_identifier_to_member
            -> context_key
            -> includes_excludes_key
        A user can cross reference the datapoint id with the Datapoint table to derive
        these non-included column values.
        """
        if datapoint_before_update is None:
            return
        # Don't write a history entry if the underlying datapoint has not changed.
        if (
            datapoint_before_update.value == datapoint_after_update.value
            and datapoint_before_update.enabled == datapoint_after_update.enabled
        ):
            return
        session.add(
            schema.DatapointHistory(
                # datapoint_before_update and datapoint_after_update share the same id.
                datapoint_id=datapoint_after_update.id,
                user_account_id=user_account.id if user_account is not None else None,
                timestamp=datapoint_after_update.last_updated,
                old_value=datapoint_before_update.value,
                new_value=datapoint_after_update.value,
                old_enabled=datapoint_before_update.enabled,
                new_enabled=datapoint_after_update.enabled,
                # Not populated for agency datapoints.
                old_upload_method=None,
                new_upload_method=None,
            )
        )
        return

    # TODO(#28469): Deprecate/delete this after the MetricSetting migration.
    # Also create a MetricSetting alternative for historical datapoints.
    @staticmethod
    def add_agency_datapoint(
        session: Session,
        datapoint: schema.Datapoint,
        current_time: datetime.datetime,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        """
        Write (or overwrite) the agency datapoint in the Datapoint table and record the
        modification in the Datapoint History table.
        """
        datapoint_after_update, datapoint_before_update = update_existing_or_create(
            ingested_entity=datapoint,
            session=session,
            current_time=current_time,
        )
        DatapointInterface.record_agency_datapoint_history(
            session=session,
            datapoint_before_update=datapoint_before_update,
            datapoint_after_update=datapoint_after_update,
            user_account=user_account,
        )

    # TODO(#28469): Deprecate/delete this after the MetricSetting migration.
    # Also create a MetricSetting alternative for historical datapoints.
    @staticmethod
    def add_or_update_agency_datapoints(
        session: Session,
        agency: schema.Agency,
        agency_metric: MetricInterface,
        user_account: Optional[schema.UserAccount] = None,
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
        3) To update metric includes/excludes datapoints to save include/exclude settings. Includes/excludes
           settings describe what data is used to make up aggregate or disaggregation values.
        """

        current_time = datetime.datetime.now(tz=datetime.timezone.utc)
        # 1. Enable / disable top level metric
        if agency_metric.is_metric_enabled is not None:
            # Only enable/disable metric if the frontend explicitly specifies an
            # enable/disabled status.
            DatapointInterface.add_agency_datapoint(
                session=session,
                datapoint=schema.Datapoint(
                    metric_definition_key=agency_metric.key,
                    source=agency,
                    enabled=agency_metric.is_metric_enabled,
                    dimension_identifier_to_member=None,
                    is_report_datapoint=False,
                ),
                current_time=current_time,
                user_account=user_account,
            )

        # 2. Set default contexts
        for context in agency_metric.contexts:
            DatapointInterface.add_agency_datapoint(
                session=session,
                datapoint=schema.Datapoint(
                    metric_definition_key=agency_metric.key,
                    source=agency,
                    context_key=context.key.value,
                    value=context.value,
                    dimension_identifier_to_member=None,
                    is_report_datapoint=False,
                ),
                current_time=current_time,
                user_account=user_account,
            )

        # 3. Set top-level includes/excludes
        if (
            agency_metric.metric_definition.includes_excludes is not None
            and agency_metric.includes_excludes_member_to_setting != {}
        ):
            # Create new datapoint for each includes_excludes setting
            # at the metric level.
            for (
                member,
                setting,
            ) in agency_metric.includes_excludes_member_to_setting.items():
                DatapointInterface.add_includes_excludes_datapoint(
                    session=session,
                    member=member,
                    setting=setting,
                    agency=agency,
                    user_account=user_account,
                    agency_metric=agency_metric,
                )

        # 4. Add datapoint for custom reporting frequency
        if agency_metric.custom_reporting_frequency.frequency is not None:
            # Only enable/disable if the frontend explicitly specifies an
            # enable/disabled status.
            DatapointInterface.add_agency_datapoint(
                session=session,
                datapoint=schema.Datapoint(
                    metric_definition_key=agency_metric.key,
                    source=agency,
                    context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
                    value=agency_metric.custom_reporting_frequency.to_json_str(),
                    dimension_identifier_to_member=None,
                    is_report_datapoint=False,
                ),
                current_time=current_time,
                user_account=user_account,
            )

        # 4. Add datapoints to record that metric is disaggregated_by_supervision_subsystems
        # if the metric is disaggregated and the agency also reports for supervision subsystems,
        systems_enums = {schema.System[s] for s in agency.systems}
        if (
            agency_metric.disaggregated_by_supervision_subsystems is not None
            and len(schema.System.supervision_subsystems().intersection(systems_enums))
            > 0
        ):
            current_system = agency_metric.metric_definition.system.value
            for system in agency.systems:
                if (
                    schema.System[system] == schema.System.SUPERVISION
                    or schema.System[system] in schema.System.supervision_subsystems()
                ):
                    # First, add a datapoint for disaggregated_by_supervision_subsystems for every
                    # supervision system that an agency belongs to.
                    metric_definition_key = (
                        agency_metric.key
                        if system == current_system
                        else agency_metric.key.replace(current_system, system, 1)
                    )
                    DatapointInterface.add_agency_datapoint(
                        session=session,
                        datapoint=schema.Datapoint(
                            metric_definition_key=metric_definition_key,
                            source=agency,
                            context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                            dimension_identifier_to_member=None,
                            value=str(
                                agency_metric.disaggregated_by_supervision_subsystems
                            ),
                            is_report_datapoint=False,
                        ),
                        current_time=current_time,
                        user_account=user_account,
                    )

                    # Then, update the enabled/disabled statuses accordingly. If the metric
                    # is Supervision we are *not* disaggregating, the metric should be enabled; otherwise, disabled.
                    # If the metric is a supervsion subsystem,the logic is reversed.
                    if schema.System[system] == schema.System.SUPERVISION:
                        DatapointInterface.add_agency_datapoint(
                            session=session,
                            datapoint=schema.Datapoint(
                                metric_definition_key=metric_definition_key,
                                source=agency,
                                enabled=not agency_metric.disaggregated_by_supervision_subsystems,
                                dimension_identifier_to_member=None,
                                is_report_datapoint=False,
                            ),
                            current_time=current_time,
                            user_account=user_account,
                        )
                    elif (
                        schema.System[system] in schema.System.supervision_subsystems()
                    ):
                        DatapointInterface.add_agency_datapoint(
                            session=session,
                            datapoint=schema.Datapoint(
                                metric_definition_key=metric_definition_key,
                                source=agency,
                                enabled=agency_metric.disaggregated_by_supervision_subsystems,
                                dimension_identifier_to_member=None,
                                is_report_datapoint=False,
                            ),
                            current_time=current_time,
                            user_account=user_account,
                        )

        for aggregated_dimension in agency_metric.aggregated_dimensions:
            for dimension, contexts_lst in (
                aggregated_dimension.dimension_to_contexts or {}
            ).items():
                for context in contexts_lst:
                    DatapointInterface.add_agency_datapoint(
                        session=session,
                        datapoint=schema.Datapoint(
                            metric_definition_key=agency_metric.key,
                            source=agency,
                            context_key=context.key.value,
                            value=context.value,
                            dimension_identifier_to_member={
                                dimension.dimension_identifier(): dimension.dimension_name
                            },
                            is_report_datapoint=False,
                        ),
                        current_time=current_time,
                        user_account=user_account,
                    )

            for (
                dimension,
                is_dimension_enabled,
            ) in (aggregated_dimension.dimension_to_enabled_status or {}).items():
                # If is_dimension_enabled is None, then there are no deltas associated
                # with a dimension datapoint and there is no need to update/create/delete
                # the datapoint.
                dimension_identifier_to_member = {
                    dimension.dimension_identifier(): dimension.dimension_name
                }

                # 1b. Enable / disable metric dimensions
                if is_dimension_enabled is not None:
                    DatapointInterface.add_agency_datapoint(
                        session=session,
                        datapoint=schema.Datapoint(
                            metric_definition_key=agency_metric.key,
                            source=agency,
                            dimension_identifier_to_member=dimension_identifier_to_member,
                            enabled=is_dimension_enabled,
                            is_report_datapoint=False,
                        ),
                        current_time=current_time,
                        user_account=user_account,
                    )

                # 3b. Set disaggregation-level includes/excludes
                for (member, setting,) in (
                    aggregated_dimension.dimension_to_includes_excludes_member_to_setting.get(
                        dimension
                    )
                    or {}
                ).items():
                    # For each reported includes/excludes setting, create a new datapoint
                    DatapointInterface.add_includes_excludes_datapoint(
                        session=session,
                        member=member,
                        setting=setting,
                        dimension_identifier_to_member=dimension_identifier_to_member,
                        agency=agency,
                        user_account=user_account,
                        agency_metric=agency_metric,
                    )

    # TODO(#28469): Deprecate/delete this after the MetricSetting migration.
    @staticmethod
    def add_includes_excludes_datapoint(
        session: Session,
        member: enum.Enum,
        agency: schema.Agency,
        agency_metric: MetricInterface,
        setting: Optional[IncludesExcludesSetting] = None,
        dimension_identifier_to_member: Optional[Dict[str, str]] = None,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        """Adds agency datapoints to the Datapoint table that correspond with
        includes/excludes settings."""

        if setting is None:
            return
        current_time = datetime.datetime.now(tz=datetime.timezone.utc)
        DatapointInterface.add_agency_datapoint(
            session=session,
            datapoint=schema.Datapoint(
                metric_definition_key=agency_metric.key,
                source=agency,
                includes_excludes_key=member.name,
                value=setting.value,
                dimension_identifier_to_member=dimension_identifier_to_member,
                is_report_datapoint=False,
            ),
            current_time=current_time,
            user_account=user_account,
        )

    ### Helpers ###

    @staticmethod
    def is_whole_metric_disabled(
        metric_key_to_metric_interface: Dict[str, MetricInterface],
        metric_key: str,
    ) -> bool:
        """
        This function returns true if a whole metric is turned off by an agency.
        """
        if metric_key not in metric_key_to_metric_interface:
            return False

        metric_interface = metric_key_to_metric_interface[metric_key]
        return (
            metric_interface.is_metric_enabled is not None
            and metric_interface.is_metric_enabled is False
        )
