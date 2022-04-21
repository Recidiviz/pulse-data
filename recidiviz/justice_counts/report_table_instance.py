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

from typing import Optional

from sqlalchemy.orm import Session

from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportMetric,
)
from recidiviz.justice_counts.utils.persistence_utils import (
    delete_existing,
    update_existing_or_create,
)
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Cell,
    Report,
    ReportTableDefinition,
    ReportTableInstance,
)


class ReportTableInstanceInterface:
    """Contains methods for working with ReportTableInstances."""

    @staticmethod
    def create_or_update_from_reported_metric(
        session: Session,
        report: Report,
        report_table_definition: ReportTableDefinition,
        reported_metric: ReportMetric,
        aggregated_dimension: Optional[ReportedAggregatedDimension] = None,
    ) -> ReportTableDefinition:
        """Given a Report, a ReportTableDefinition, a ReportMetric, and an
        (optional) aggregated dimension, create (or update) the corresponding
        ReportTableInstances and Cells.
        """
        table_instance = update_existing_or_create(
            ReportTableInstance(
                report=report,
                report_table_definition=report_table_definition,
                time_window_start=report.date_range_start,
                time_window_end=report.date_range_end,
                # TODO(#12050): Populate `methodology` column with `ReportedContexts`
                methodology=None,
            ),
            session,
        )

        if not aggregated_dimension:
            cells = [
                update_existing_or_create(
                    Cell(
                        value=reported_metric.value,
                        aggregated_dimension_values=[],
                        report_table_instance=table_instance,
                    ),
                    session,
                )
            ]

        else:
            cells = [
                update_existing_or_create(
                    Cell(
                        value=value,
                        aggregated_dimension_values=[key.dimension_value],
                        report_table_instance=table_instance,
                    ),
                    session,
                )
                for key, value in aggregated_dimension.dimension_to_value.items()
            ]
        table_instance.cells = cells
        return table_instance

    @staticmethod
    def delete_from_reported_metric(
        session: Session,
        report: Report,
        report_table_definition: ReportTableDefinition,
    ) -> None:
        delete_existing(
            session,
            ReportTableInstance(
                report=report,
                report_table_definition=report_table_definition,
                time_window_start=report.date_range_start,
                time_window_end=report.date_range_end,
            ),
            ReportTableInstance,
        )
