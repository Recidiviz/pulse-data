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
"""Interface for working with the Reports model."""
import datetime
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, cast

from sqlalchemy.orm import Session

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.exceptions import JusticeCountsPermissionError
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    ReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_registry import METRICS
from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportedContext,
    ReportMetric,
)
from recidiviz.justice_counts.report_table_definition import (
    ReportTableDefinitionInterface,
)
from recidiviz.justice_counts.report_table_instance import ReportTableInstanceInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.persistence_utils import get_existing_entity
from recidiviz.persistence.database.schema.justice_counts import schema


class ReportInterface:
    """Contains methods for setting and getting Report info."""

    @staticmethod
    def _raise_if_user_is_unauthorized(
        session: Session, agency_id: int, user_account_id: int
    ) -> None:
        user = UserAccountInterface.get_user_by_id(
            session=session, user_account_id=user_account_id
        )
        if not agency_id in {a.id for a in user.agencies}:
            raise JusticeCountsPermissionError(
                code="justice_counts_agency_permission",
                description=f"User (user_id: {user_account_id}) does not have access to reports from current agency (agency_id: {agency_id}).",
            )

    @staticmethod
    def get_report_by_id(session: Session, report_id: int) -> schema.Report:
        return session.query(schema.Report).filter(schema.Report.id == report_id).one()

    @staticmethod
    def get_reports_by_agency_id(
        session: Session, agency_id: int, user_account_id: int
    ) -> List[schema.Report]:
        ReportInterface._raise_if_user_is_unauthorized(
            session=session, agency_id=agency_id, user_account_id=user_account_id
        )
        return (
            session.query(schema.Report)
            .filter(schema.Report.source_id == agency_id)
            .order_by(schema.Report.date_range_end.desc())
            .all()
        )

    @staticmethod
    def _get_report_instance(
        report_type: str,
        date_range_start: datetime.date,
    ) -> str:
        if report_type == ReportingFrequency.MONTHLY.value:
            month = date_range_start.strftime("%m")
            return f"{month} {str(date_range_start.year)} Metrics"
        return f"{str(date_range_start.year)} Annual Metrics"

    @staticmethod
    def create_report(
        session: Session,
        agency_id: int,
        user_account_id: int,
        year: int,
        month: int,
        frequency: str,
    ) -> schema.Report:
        """Creates empty report in Justice Counts DB"""
        ReportInterface._raise_if_user_is_unauthorized(
            session=session, agency_id=agency_id, user_account_id=user_account_id
        )
        report_type = (
            ReportingFrequency.MONTHLY.value
            if frequency == ReportingFrequency.MONTHLY.value
            else ReportingFrequency.ANNUAL.value
        )
        date_range_start = datetime.date(year, month, 1)
        date_range_end = (
            datetime.date(year, month + 1, 1)
            if frequency == ReportingFrequency.MONTHLY.value
            else datetime.date(year + 1, month, 1)
        )
        report = schema.Report(
            source_id=agency_id,
            type=report_type,
            instance=ReportInterface._get_report_instance(
                report_type=report_type, date_range_start=date_range_start
            ),
            created_at=datetime.date.today(),
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            status=schema.ReportStatus.NOT_STARTED,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )
        session.add(report)
        session.commit()
        return report

    @staticmethod
    def to_json_response(session: Session, report: schema.Report) -> Dict[str, Any]:
        editor_names = [
            UserAccountInterface.get_user_by_id(
                session=session, user_account_id=id
            ).name
            for id in report.modified_by or []
        ]
        reporting_frequency = report.get_reporting_frequency()
        return {
            "id": report.id,
            "year": report.date_range_start.year,
            "month": report.date_range_start.month
            if reporting_frequency == ReportingFrequency.MONTHLY
            else None,
            "frequency": reporting_frequency.value,
            "last_modified_at": report.last_modified_at,
            "editors": editor_names,
            "status": report.status.value,
        }

    @staticmethod
    def add_or_update_metric(
        session: Session, report: schema.Report, report_metric: ReportMetric
    ) -> None:
        """Given a Report and a ReportMetric, either add this metric
        to the report, or if the metric already exists on the report,
        update the existing metric in-place.

        Adding (or updating) a metric to a report actually involves
        adding (or updating) several different database objects:
            - ReportTableDefinition that defines the aggregated metric
              (e.g. total arrests)
            - ReportTableInstance that defines the time period for the
              aggregated metric
              (e.g. total arrests between Jan and Feb)
            - Cell that contains the value for the aggregated metric
              over that time period
              (e.g. 1000 total arrests between Jan and Feb)
            - ReportTableDefinition that defines the disaggregated metric
              (e.g. arrests broken down by gender)
            - ReportTableInstance that defines the time period for the
              disaggregated metric
              (e.g. arrests broken down by gender between Jan and Feb)
            - Cells that contain the values for the disaggregated metric
              (e.g. 100 arrests of men, 23 arrests of women, etc,
              each in their own Cell object)
        """

        # First, define a ReportTableDefinition + Instance + Cells for the
        # aggregate metric value (summed across all dimensions).
        report_table_definition = (
            ReportTableDefinitionInterface.create_or_update_from_report_metric(
                session=session, report=report, report_metric=report_metric
            )
        )
        ReportTableInstanceInterface.create_or_update_from_report_metric(
            session=session,
            report=report,
            report_table_definition=report_table_definition,
            report_metric=report_metric,
        )

        # Next, define a ReportTableDefinition + Instance + Cells for
        # each disaggregated dimension of the metric.
        for dimension in report_metric.aggregated_dimensions or []:
            report_table_definition = (
                ReportTableDefinitionInterface.create_or_update_from_report_metric(
                    session=session,
                    report_metric=report_metric,
                    aggregated_dimension_identifier=dimension.dimension_identifier(),
                    report=report,
                )
            )
            ReportTableInstanceInterface.create_or_update_from_report_metric(
                session=session,
                report=report,
                report_table_definition=report_table_definition,
                report_metric=report_metric,
                aggregated_dimension=dimension,
            )

        # Finally, if any disaggregated dimensions that are defined on the metric
        # were explicitly not reported by the agency, this means that the agency
        # decided to remove them, so delete them from the DB
        definition_dimension_identifiers = {
            dimension.dimension_identifier()
            for dimension in report_metric.metric_definition.aggregated_dimensions or []
        }
        reported_dimension_identifiers = {
            dimension.dimension_identifier()
            for dimension in report_metric.aggregated_dimensions or []
        }
        dimension_identifiers_to_delete = (
            definition_dimension_identifiers - reported_dimension_identifiers
        )
        for dimension_identifier in dimension_identifiers_to_delete:
            # No need to delete the ReportTableDefinition, but we do need
            # to load it so we can identify/delete the proper ReportTableInstance
            # (which will also delete all child Cell objects too)
            report_table_definition_row = get_existing_entity(
                ingested_entity=ReportTableDefinitionInterface.build_entity(
                    report_metric=report_metric,
                    aggregated_dimension_identifier=dimension_identifier,
                    report=report,
                ),
                session=session,
            )
            if report_table_definition_row:
                # If no report_table_definition_row is found, then this dimension hasn't been
                # reported yet, so there's nothing to delete.
                report_table_definition = ReportTableDefinitionInterface.get_by_id(
                    session=session, _id=report_table_definition_row.id
                )
                ReportTableInstanceInterface.delete_from_reported_metric(
                    session=session,
                    report=report,
                    report_table_definition=report_table_definition,
                )

    @staticmethod
    def get_metrics_by_report_id(
        session: Session,
        report_id: int,
    ) -> List[ReportMetric]:
        """Given a report_id, determine all MetricDefinitions that must be populated
        on this report, and convert them to ReportMetrics. If the agency has already
        started filling out the report, populate the ReportMetrics with those values.
        This method will be used to send a list of Metrics to the frontend to render
        the report form in the Control Panel.

        This involves the following logic:
        1. Filter the MetricDefinitions in our registry to just those that are applicable
           to this report, i.e. those that belong to the same criminal justice system pillar
           as the reporting agency, and those that match the reporting frequency of
           the given report.
        2. Look up data that already exists on the report that is stored in the
           ReportTableDefinition and ReportTableInstance models.
        3. Perform matching between the MetricDefinitions and the existing data.
        4. Return a list of ReportMetrics. If the agency has not filled out data for a
           metric, its values will be None; otherwise they will be populated from the data
           already stored in our database.
        """
        report = ReportInterface.get_report_by_id(session=session, report_id=report_id)

        # We determine which metrics to include on this report based on:
        #   - Agency system (e.g. only law enforcement)
        #   - Report frequency (e.g. only annual metrics)
        metric_definitions = [
            metric
            for metric in METRICS
            # TODO(#11973): Add system field to `Agency` model and remove this System.LAW_ENFORCEMENT placeholder
            if metric.system == schema.System.LAW_ENFORCEMENT
            and report.type in {freq.value for freq in metric.reporting_frequencies}
        ]

        # If data has already been reported for some metrics on this report,
        # then `report.report_table_instances` will be non-empty.
        # For each reported metric, there will be one definition/instance pair for the
        # aggregated metric value, and another definition/instance pair for each
        # reported disaggregated dimension.
        table_definition_instance_pairs = [
            (instance.report_table_definition, instance)
            for instance in report.report_table_instances
        ]

        # Group the reported metrics (if there are any) by MetricDefinitions.
        # One MetricDefinition will correspond to multiple ReportTableDefinitions;
        # one for the aggregate value and one for each disaggregated dimension.
        # ReportTableDefinition.label should match MetricDefinition.key, and is
        # the means by which we connect these two objects.
        metric_key_to_table_definition_instance_pairs = defaultdict(list)
        for pair in table_definition_instance_pairs:
            metric_key_to_table_definition_instance_pairs[pair[0].label].append(pair)

        report_metrics = []
        # For each metric that should be filled out on this report,
        # construct a ReportMetric object
        for metric_definition in metric_definitions:
            table_definition_instance_pairs = (
                metric_key_to_table_definition_instance_pairs[metric_definition.key]
            )

            # First see if aggregate data has been reported
            reported_aggregated_value = ReportInterface._get_aggregate_value(
                table_definition_instance_pairs=table_definition_instance_pairs,
            )

            # Then check if data has been reported for each disaggregated dimension
            reported_aggregated_dimensions = ReportInterface._get_aggregated_dimensions(
                aggregated_dimensions=metric_definition.aggregated_dimensions or [],
                table_definition_instance_pairs=table_definition_instance_pairs,
            )

            # TODO(#12050) Store ReportedContexts in the methodology column
            reported_contexts = [
                ReportedContext(key=context.key, value=None)
                for context in metric_definition.contexts or []
            ]

            report_metrics.append(
                ReportMetric(
                    key=metric_definition.key,
                    value=reported_aggregated_value,
                    contexts=reported_contexts,
                    aggregated_dimensions=reported_aggregated_dimensions,
                )
            )

        return report_metrics

    @staticmethod
    def _get_aggregate_value(
        table_definition_instance_pairs: List[
            Tuple[schema.ReportTableDefinition, schema.ReportTableInstance]
        ]
    ) -> Optional[int]:
        """Given a list of ReportTableDefinitions from the database, find the
        one without aggregated dimensions (which therefore corresponds to the
        aggregate metric value). Pull its corresponding ReportTableInstance,
        and extract the aggregate metric value from its Cell.
        """
        aggregated_pair = [
            p for p in table_definition_instance_pairs if not p[0].aggregated_dimensions
        ]
        if not aggregated_pair:
            return None
        if len(aggregated_pair) > 1:
            raise ValueError(
                "More than one ReportTableDefinition found with no aggregated dimensions."
            )

        aggregated_instance = aggregated_pair[0][1]
        if len(aggregated_instance.cells) != 1:
            raise ValueError(
                "More than one cell found on a ReportTableInstance with no aggregated dimensions."
            )
        return aggregated_instance.cells[0].value

    @staticmethod
    def _get_aggregated_dimensions(
        aggregated_dimensions: List[AggregatedDimension],
        table_definition_instance_pairs: List[
            Tuple[schema.ReportTableDefinition, schema.ReportTableInstance]
        ],
    ) -> List[ReportedAggregatedDimension]:
        """Given a list of AggregatedDimensions that are expected for a metric,
        and a list of ReportTableDefinitions from the database, iterate through
        each AggregateDimension and find the corresponding ReportTableDefinition.
        Pull its corresponding ReportTableInstance, and extract a dictionary of
        dimension instances to metric values from its cells.
        """
        reported_aggregated_dimensions: List[ReportedAggregatedDimension] = []
        for dimension in aggregated_dimensions:
            # e.g. dimension = AggregatedDimension(dimension=RaceAndEthnicity)
            #      disaggregated_definition = [ReportTableDefinition(aggregated_dimensions=["global/race_and_ethnicity"])]
            disaggregated_pair = [
                p
                for p in table_definition_instance_pairs
                if dimension.dimension_identifier() in p[0].aggregated_dimensions
            ]

            # dimension.dimension is an Enum class, e.g. Gender or Populationtype
            # You can iterate over Enum classes to yield their instances, e.g.
            # list(Gender) -> [Gender.FEMALE, Gender.MALE]
            # This is hard to do properly with mypy, though
            iterable_dimension_enum_class = cast(
                Iterable[DimensionBase], dimension.dimension
            )
            # dimension_to_value will be a dict like {Gender.FEMALE: None, Gender.MALE: None, etc}
            dimension_to_value: Dict[DimensionBase, Optional[float]] = {
                d: None for d in iterable_dimension_enum_class
            }
            if not disaggregated_pair:
                # If no matching ReportTableDefinition exists in the database, the agency must
                # not have reported it yet. In this case, return a dictionary with null values,
                reported_aggregated_dimensions.append(
                    ReportedAggregatedDimension(dimension_to_value=dimension_to_value)
                )
                continue

            if len(disaggregated_pair) > 1:
                raise ValueError(
                    "More than one ReportTableDefinition found with "
                    f"aggregated dimension: {dimension.dimension_identifier()}."
                )

            disaggregated_instance = disaggregated_pair[0][1]
            # The logic below iterates through the Cells in the database, each of which corresponds
            # to one category (White, Asian, etc), converts the cell.aggregated_dimension_value
            # (which will be a string like "WHITE") to an instance of the dimension enum
            # (e.g. RaceAndEthnicity.WHITE), and populates a dictionary mapping the dimension instance
            # to the cell values, e.g. {RaceAndEthnicity.WHITE: 100, RaceAndEthnicity.ASIAN: 25, etc}
            dimension_to_value = {}
            for cell in disaggregated_instance.cells:
                # dimension.dimension is an Enum class, e.g. Gender or Populationtype
                # You can index Enum classes with a string to get the corresponding index, e.g.
                # Gender["MALE"] -> Gender.MALE
                # This is hard to do properly with mypy, though
                indexable_dimension_enum_class = cast(
                    Mapping[str, DimensionBase], dimension.dimension
                )
                _dimension = indexable_dimension_enum_class[
                    cell.aggregated_dimension_values[0]
                ]
                dimension_to_value[_dimension] = cell.value
            reported_aggregated_dimensions.append(
                ReportedAggregatedDimension(dimension_to_value=dimension_to_value)
            )
        return reported_aggregated_dimensions
