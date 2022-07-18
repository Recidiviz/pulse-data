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
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.exceptions import (
    JusticeCountsBadRequestError,
    JusticeCountsDataError,
)
from recidiviz.justice_counts.utils.persistence_utils import (
    delete_existing,
    update_existing_or_create,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.types import assert_type


class DatapointInterface:
    """Contains methods for working with Datapoint.
    Datapoints can either be numeric metric values, or contexts associated with a metric.
    In either case, metric_definition_key indicates which metric the datapoint applies to.
    If context_key is not None, the datapoint is a context. value_type will be numeric for non-context
    datapoints, and otherwise will indicate the type of context. If dimension_identifier_to_member is not
    None the numeric value applies to a particular dimension.
    """

    @staticmethod
    def update_agency_metric(
        session: Session,
        agency: schema.Agency,
        metric_json: Dict[str, Any],
    ) -> None:
        """
        Agency datapoints are not used to store data, like report datapoints are; rather, they are used to
        indicate which metrics are enabled for an agency, and what their default contexts should be.
        Agency datapoints have an agency_id in the source_id column and a null value in the report column.

        This method takes in the JSON for a single metric, which might then get converted to several datapoints.
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
        contexts = metric_json.get("contexts")
        disaggregations = metric_json.get("disaggregations")
        key = assert_type(metric_json.get("key"), str)
        is_metric_enabled = metric_json.get("enabled")
        if contexts is None and disaggregations is None and is_metric_enabled is False:
            # Turn off the whole metric
            update_existing_or_create(
                ingested_entity=schema.Datapoint(
                    metric_definition_key=key,
                    source=agency,
                    enabled=False,
                    dimension_identifier_to_member=None,
                ),
                session=session,
            )

        if is_metric_enabled is True:
            # When metric is re-enabled, delete corresponding agency datapoint.
            delete_existing(
                session=session,
                ingested_entity=schema.Datapoint(
                    source=agency,
                    metric_definition_key=key,
                    dimension_identifier_to_member=None,
                ),
                entity_cls=schema.Datapoint,
            )

        if is_metric_enabled is True and contexts is not None:
            # Pre-fill contexts
            for context in contexts:
                context_key = assert_type(context.get("key"), str)
                update_existing_or_create(
                    schema.Datapoint(
                        metric_definition_key=key,
                        source=agency,
                        context_key=context_key,
                        value=context.get("value"),
                        dimension_identifier_to_member=None,
                    ),
                    session,
                )

        if is_metric_enabled is True and disaggregations is not None:
            for disaggregation in disaggregations:
                is_disaggregation_enabled = disaggregation.get("enabled", True)
                dimension_identifier = assert_type(disaggregation.get("key"), str)
                dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
                    dimension_identifier
                ]
                for member in dimension_class or []:
                    is_breakdown_enabled = disaggregation.get("dimensions", {}).get(
                        member.dimension_name
                    )

                    if (
                        is_disaggregation_enabled is False
                        and is_breakdown_enabled is True
                    ):
                        raise JusticeCountsBadRequestError(
                            code="invalid_metric_setting",
                            description="Invalid metric setting: disaggregation is disabled but breakdown is on.",
                        )
                    create_disabled_datapoint = is_disaggregation_enabled is False or (
                        is_disaggregation_enabled is True
                        and is_breakdown_enabled is False
                    )
                    if create_disabled_datapoint:
                        update_existing_or_create(
                            ingested_entity=schema.Datapoint(
                                metric_definition_key=key,
                                source=agency,
                                dimension_identifier_to_member={
                                    member.dimension_identifier(): member.dimension_name
                                },
                                enabled=False,
                            ),
                            session=session,
                        )
                    if is_breakdown_enabled is True:
                        delete_existing(
                            session=session,
                            ingested_entity=schema.Datapoint(
                                source=agency,
                                metric_definition_key=key,
                                dimension_identifier_to_member={
                                    member.dimension_identifier(): member.dimension_name
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
    ) -> Optional[schema.Datapoint]:
        """Given a Report and a ReportMetric, add a row to the datapoint table.
        All datapoints associated with a metric are saved, even if no value was reported.
        An empty form field is represented by a None value.
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

        datapoint, existing_datapoint = update_existing_or_create(
            schema.Datapoint(
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
            ),
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
