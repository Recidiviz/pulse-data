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
import json
from typing import Any, Dict, List, Optional, Tuple, Union

from sqlalchemy.orm import Session
from sqlalchemy.sql import case

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import UploadMethod
from recidiviz.justice_counts.utils.datapoint_utils import (
    filter_deprecated_datapoints,
    get_dimension,
    get_dimension_id,
    get_value,
)
from recidiviz.justice_counts.utils.persistence_utils import expunge_existing
from recidiviz.persistence.database.schema.justice_counts import schema

# Datapoints are unique by a tuple of:
# <report start date, report end date, agency id, metric definition,
# context key, disaggregations>
DatapointUniqueKey = Tuple[
    datetime.date, datetime.date, int, str, Optional[str], Optional[str], Optional[str]
]


class DatapointInterface:
    """Contains methods for working with Datapoint.
    Datapoints are numeric metric values associated with a metric. The
    metric_definition_key indicates which metric the datapoint applies to.
    If dimension_identifier_to_member is not None the numeric value applies
    to a particular dimension.
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
        response["sub_dimension_name"] = (
            datapoint.sub_dimension_name.title()
            if datapoint.sub_dimension_name is not None
            else None
        )
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
                if datapoint.sub_dimension_name is None:
                    if dimension_in_metric_interface[0].dimension_to_value is None:
                        dimension_in_metric_interface[0].dimension_to_value = {}
                    dimension_in_metric_interface[0].dimension_to_value[
                        dimension_enum_member
                    ] = get_value(datapoint=datapoint)
                else:
                    if (
                        dimension_enum_member
                        not in dimension_in_metric_interface[
                            0
                        ].dimension_to_other_sub_dimension_to_value
                    ):
                        dimension_in_metric_interface[
                            0
                        ].dimension_to_other_sub_dimension_to_value[
                            dimension_enum_member
                        ] = {}
                    dimension_in_metric_interface[
                        0
                    ].dimension_to_other_sub_dimension_to_value[dimension_enum_member][
                        datapoint.sub_dimension_name
                    ] = get_value(
                        datapoint=datapoint
                    )

                continue

            # Dimension is not in `aggregated_dimensions`. Add a new entry for it.
            dimension_data = MetricAggregatedDimensionData()
            dimension_data.dimension_to_value = {
                dimension_enum_member: get_value(datapoint=datapoint)
            }
            metric_interface.aggregated_dimensions.append(dimension_data)

        return metric_key_to_metric_interface

    @staticmethod
    def normalize_dimension(dim: Optional[Union[str, dict]]) -> str:
        if dim is None:
            return ""
        if isinstance(dim, str):
            try:
                dim = json.loads(dim)
            except Exception:
                return dim  # type: ignore[return-value]
        return json.dumps(dim)

    ### Save Path: Report Datapoints ###
    @staticmethod
    def add_report_datapoint(
        *,
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
        sub_dimension: Optional[str] = None,
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
            DatapointInterface.normalize_dimension(
                {dimension.dimension_identifier(): dimension.dimension_name}
                if dimension
                else None
            ),
            sub_dimension,
        )
        existing_datapoint = existing_datapoints_dict.get(datapoint_key)
        if uploaded_via_breakdown_sheet:
            # If this flag is set and there is an existing aggregate value, keep the
            # existing aggregate value.
            if existing_datapoint is not None and existing_datapoint.value is not None:
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
            sub_dimension_name=(
                sub_dimension.upper() if sub_dimension is not None else None
            ),  # Normalize all sub_dimensions to be all-caps
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
