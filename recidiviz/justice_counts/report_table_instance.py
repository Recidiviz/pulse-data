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
from recidiviz.persistence.database.schema.justice_counts import schema


class ReportTableInstanceInterface:
    """Contains methods for working with ReportTableInstances."""

    @staticmethod
    def create_or_update_from_report_metric(
        session: Session,
        report: schema.Report,
        report_table_definition: schema.ReportTableDefinition,
        report_metric: ReportMetric,
        aggregated_dimension: Optional[ReportedAggregatedDimension] = None,
    ) -> schema.ReportTableDefinition:
        """Given a Report, a ReportTableDefinition, a ReportMetric, and an
        (optional) aggregated dimension, create (or update) the corresponding
        ReportTableInstances and Cells.
        """
        table_instance = update_existing_or_create(
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
            cells = [
                update_existing_or_create(
                    schema.Cell(
                        value=report_metric.value,
                        aggregated_dimension_values=[],
                        report_table_instance=table_instance,
                    ),
                    session,
                )
            ]

            # Right now we only support per-metric contexts, so we store all
            # contexts on the ReportTableInstance that corresponds to the
            # aggregate metric value.
            contexts = [
                update_existing_or_create(
                    schema.Context(
                        key=context.key.value,
                        value=context.value,
                        report_table_instance=table_instance,
                    ),
                    session,
                )
                for context in report_metric.contexts or []
                if context.value is not None
            ]
        else:
            # If aggregated_dimension is None, we are currently creating
            # the ReportTableInstance corresponding to this dimension.
            cells = [
                update_existing_or_create(
                    schema.Cell(
                        value=value,
                        aggregated_dimension_values=[key.dimension_value],
                        report_table_instance=table_instance,
                    ),
                    session,
                )
                for key, value in aggregated_dimension.dimension_to_value.items()
            ]
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
