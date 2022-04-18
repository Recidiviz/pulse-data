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
"""Interface for working with the ReportTableDefinition model."""

from typing import List, Optional, Tuple

from sqlalchemy.orm import Session

from recidiviz.justice_counts.dimensions.location import Country, County, State
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.reported_metric import (
    ReportedAggregatedDimension,
    ReportedMetric,
)
from recidiviz.justice_counts.utils.persistence_utils import update_existing_or_create
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportTableDefinition,
)


class ReportTableDefinitionInterface:
    """Contains methods for working with ReportTableDefinitions."""

    @staticmethod
    def create_or_update_from_reported_metric(
        session: Session,
        reported_metric: ReportedMetric,
        aggregated_dimension: Optional[ReportedAggregatedDimension] = None,
    ) -> ReportTableDefinition:
        """Given a Report, a ReportedMetric, and an (optional) aggregated dimension,
        create (or update) a corresponding ReportTableDefinition.
        """
        metric_definition = reported_metric.metric_definition
        (
            filtered_dimensions,
            filtered_dimension_values,
        ) = ReportTableDefinitionInterface.get_filtered_dimensions(
            metric_definition=metric_definition
        )

        return update_existing_or_create(
            ingested_entity=ReportTableDefinition(
                system=metric_definition.system,
                metric_type=metric_definition.metric_type,
                measurement_type=metric_definition.measurement_type,
                filtered_dimensions=filtered_dimensions,
                filtered_dimension_values=filtered_dimension_values,
                aggregated_dimensions=[aggregated_dimension.dimension_identifier()]
                if aggregated_dimension
                else [],
                label=reported_metric.metric_definition.key
                + ("_AGGREGATED" if not aggregated_dimension else ""),
            ),
            session=session,
        )

    @staticmethod
    def get_filtered_dimensions(
        metric_definition: MetricDefinition,
    ) -> Tuple[List[str], List[str]]:
        """Returns a tuple of filtered dimension keys and filtered dimension values.
        Filtered dimensions are a combination of the filtered dimensions defined on the
         metric, and location properties of the agency.
        """
        filtered_dimensions = metric_definition.filtered_dimensions or []

        # TODO(#12064) Add test for storing a ReportedMetric whose MetricDefinition
        # has non-empty `filtered_dimensions`
        filtered_dimension_keys, filtered_dimension_values = [
            [
                dimension.dimension.dimension_identifier()
                for dimension in filtered_dimensions
            ],
            [dimension.dimension.dimension_value for dimension in filtered_dimensions],
        ]

        filtered_dimension_keys += [
            Country.dimension_identifier(),
            State.dimension_identifier(),
            County.dimension_identifier(),
        ]
        # TODO(#11973): Add filtered_dimension values from report.source
        filtered_dimension_values += []

        return filtered_dimension_keys, filtered_dimension_values
