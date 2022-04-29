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
"""Interface for working with the ReportTableInstance model."""

import datetime
import decimal
from typing import Optional, Union

from sqlalchemy.orm import Session

from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportMetric,
)
from recidiviz.justice_counts.utils.persistence_utils import (
    delete_existing,
    update_existing_or_create,
)
from recidiviz.persistence.database.schema.justice_counts import schema


class ReportTableInstanceInterface:
    """Contains methods for working with ReportTableInstances."""

    @staticmethod
    def create_or_update_from_report_metric(
        session: Session,
        report: schema.Report,
        report_table_definition: schema.ReportTableDefinition,
        report_metric: ReportMetric,
        user_account: schema.UserAccount,
        current_time: datetime.datetime,
        aggregated_dimension: Optional[ReportedAggregatedDimension] = None,
    ) -> schema.ReportTableDefinition:
        """Given a Report, a ReportTableDefinition, a ReportMetric, and an
        (optional) aggregated dimension, create (or update) the corresponding
        ReportTableInstances and Cells.
        """
        table_instance, _ = update_existing_or_create(
            schema.ReportTableInstance(
                report=report,
                report_table_definition=report_table_definition,
                time_window_start=report.date_range_start,
                time_window_end=report.date_range_end,
                methodology=None,
            ),
            session,
        )

        if not aggregated_dimension:
            # If aggregated_dimension is None, we are currently creating
            # the ReportTableInstance corresponding to the aggregate metric value.
            if report_metric.value is not None:
                cell, existing_cell = update_existing_or_create(
                    schema.Cell(
                        value=report_metric.value,
                        aggregated_dimension_values=[],
                        report_table_instance=table_instance,
                    ),
                    session,
                )
                if existing_cell:
                    cell_history = schema.CellHistory(
                        user_account_id=user_account.id,
                        timestamp=current_time,
                        # For some reason, SQLAlchemy returns an instance of decimal.decimal here
                        old_value=_decimal_to_python_type(number=existing_cell.value),
                        new_value=report_metric.value,
                    )
                    cell.cell_histories.append(cell_history)
                cells = [cell]
            else:
                cells = []

            # Right now we only support per-metric contexts, so we store all
            # contexts on the ReportTableInstance that corresponds to the
            # aggregate metric value.
            contexts = []
            for reported_context in report_metric.contexts or []:
                if reported_context.value is None:
                    continue
                context: schema.Context
                context, _ = update_existing_or_create(
                    schema.Context(
                        key=reported_context.key.value,
                        value=reported_context.value,
                        report_table_instance=table_instance,
                    ),
                    session,
                )
                contexts.append(context)
        else:
            # If aggregated_dimension is None, we are currently creating
            # the ReportTableInstance corresponding to this dimension.
            cells = []
            for key, value in aggregated_dimension.dimension_to_value.items():
                if value is None:
                    continue
                cell, existing_cell = update_existing_or_create(
                    schema.Cell(
                        value=value,
                        aggregated_dimension_values=[key.dimension_value],
                        report_table_instance=table_instance,
                    ),
                    session,
                )
                if existing_cell:
                    cell_history = schema.CellHistory(
                        user_account_id=user_account.id,
                        timestamp=current_time,
                        # For some reason, SQLAlchemy returns an instance of decimal.decimal here
                        old_value=_decimal_to_python_type(number=existing_cell.value),
                        new_value=value,
                    )
                    cell.cell_histories.append(cell_history)
                cells.append(cell)

            # TODO(#12433) Support per-dimension contexts
            contexts = []

        table_instance.cells = cells
        table_instance.contexts = contexts

        return table_instance

    @staticmethod
    def delete_from_reported_metric(
        session: Session,
        report: schema.Report,
        report_table_definition: schema.ReportTableDefinition,
    ) -> None:
        delete_existing(
            session,
            schema.ReportTableInstance(
                report=report,
                report_table_definition=report_table_definition,
                time_window_start=report.date_range_start,
                time_window_end=report.date_range_end,
            ),
            schema.ReportTableInstance,
        )


def _decimal_to_python_type(number: decimal.Decimal) -> Union[int, float]:
    """Convert an instance of decimal.Decimal to either int or float."""
    if number % 1 == 0:
        return int(number)
    return float(number)
