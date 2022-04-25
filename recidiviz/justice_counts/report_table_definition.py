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
from recidiviz.justice_counts.metrics.report_metric import ReportMetric
from recidiviz.justice_counts.utils.persistence_utils import update_existing_or_create
from recidiviz.persistence.database.schema.justice_counts import schema


class ReportTableDefinitionInterface:
    """Contains methods for working with ReportTableDefinitions."""

    @staticmethod
    def get_by_ids(
        session: Session, ids: List[int]
    ) -> List[schema.ReportTableDefinition]:
        return (
            session.query(schema.ReportTableDefinition)
            .filter(schema.ReportTableDefinition.id.in_(ids))
            .all()
        )

    @staticmethod
    def build_entity(
        report_metric: ReportMetric,
        report: schema.Report,
        aggregated_dimension_identifier: Optional[str] = None,
    ) -> schema.ReportTableDefinition:
        """Given a Report, a ReportMetric, and an (optional) aggregated dimension,
        build a corresponding ReportTableDefinition object.
        """
        metric_definition = report_metric.metric_definition
        (
            filtered_dimensions,
            filtered_dimension_values,
        ) = ReportTableDefinitionInterface.get_filtered_dimensions(
            metric_definition=metric_definition, report=report
        )

        return schema.ReportTableDefinition(
            system=metric_definition.system,
            metric_type=metric_definition.metric_type,
            measurement_type=metric_definition.measurement_type,
            filtered_dimensions=filtered_dimensions,
            filtered_dimension_values=filtered_dimension_values,
            aggregated_dimensions=[aggregated_dimension_identifier]
            if aggregated_dimension_identifier
            else [],
            label=report_metric.metric_definition.key,
        )

    @staticmethod
    def create_or_update_from_report_metric(
        session: Session,
        report_metric: ReportMetric,
        report: schema.Report,
        aggregated_dimension_identifier: Optional[str] = None,
    ) -> schema.ReportTableDefinition:
        """Given a Report, a ReportMetric, and an (optional) aggregated dimension,
        create (or update) a corresponding ReportTableDefinition.
        """
        report_table_definition = ReportTableDefinitionInterface.build_entity(
            report_metric=report_metric,
            aggregated_dimension_identifier=aggregated_dimension_identifier,
            report=report,
        )

        return update_existing_or_create(
            ingested_entity=report_table_definition,
            session=session,
        )

    @staticmethod
    def get_by_id(session: Session, _id: int) -> schema.ReportTableDefinition:
        return (
            session.query(schema.ReportTableDefinition)
            .filter(schema.ReportTableDefinition.id == _id)
            .one()
        )

    @staticmethod
    def get_filtered_dimensions(
        metric_definition: MetricDefinition, report: schema.Report
    ) -> Tuple[List[str], List[str]]:
        """Returns a tuple of filtered dimension keys and filtered dimension values.
        Filtered dimensions are a combination of the filtered dimensions defined on the
         metric, and location properties of the agency.
        """
        filtered_dimensions = metric_definition.filtered_dimensions or []
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
        filtered_dimension_values += [
            Country.US.value,
            report.source.state_code,
            report.source.fips_county_code,
        ]

        return filtered_dimension_keys, filtered_dimension_values
