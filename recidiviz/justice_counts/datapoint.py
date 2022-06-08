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
from typing import Any, Optional

from sqlalchemy.orm import Session

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.exceptions import JusticeCountsDataError
from recidiviz.justice_counts.utils.persistence_utils import update_existing_or_create
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
                int(value)
            except ValueError as e:
                raise JusticeCountsDataError(
                    code="invalid_datapoint_value",
                    description=(
                        "Datapoint represents a int value, but is a string. "
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
