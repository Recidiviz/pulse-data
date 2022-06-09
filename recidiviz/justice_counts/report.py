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
from typing import Any, Dict, List, Optional, Set

import attr
from sqlalchemy.orm import Session

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.exceptions import JusticeCountsDataError
from recidiviz.justice_counts.metrics.metric_definition import (
    MetricDefinition,
    ReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS,
)
from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportedContext,
    ReportMetric,
)
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema


class ReportInterface:
    """Contains methods for setting and getting Report info."""

    @attr.define
    class DatapointsForMetricDefinition:
        """Class that maps datapoints to their correspoinding category (aggregate value, dimension, context)"""

        context_datapoints: List[schema.Datapoint] = attr.field(factory=list)
        dimension_datapoints: List[schema.Datapoint] = attr.field(factory=list)
        aggregated_value: Optional[int] = None

        def get_reported_contexts(
            self, metric_definition: MetricDefinition
        ) -> List[ReportedContext]:
            """
            - This method first determines which contexts we expect for this dimension definition
            - Then it looks at the contexts already reported in the database, and fills in any of
            the expected contexts that have already been reported with their reported value
            """
            context_key_to_context_value = {
                datapoint.context_key: datapoint.get_value()
                for datapoint in self.context_datapoints
            }
            return [
                ReportedContext(
                    key=context.key,
                    value=context_key_to_context_value.get(context.key.value),
                )
                for context in metric_definition.contexts
            ]

        def get_reported_aggregated_dimensions(
            self, metric_definition: MetricDefinition
        ) -> List[ReportedAggregatedDimension]:
            """
            - This method first looks at all dimensions that have already been reported in
            the database, and extracts them into a dictionary dimension_id_to_dimension_values_dicts.
            - As we fill out this dictionary, we make sure that each dimension_values dict is "complete",
            i.e. contains all member values for that dimension. If one of the members hasn't been reported yet, it's value will be None.
            - It's possible that not all dimensions we expect for this metric are in this dictionary,
            i.e. if the user hasn't filled out any values for a particular dimension yet.
            - Thus, at the end of the function, we look at all the dimensions that are expected for this metric,
            and if one doesn't exist in the dictionary, we add it with all values set to None.
            """

            # dimension_id_to_dimension_values_dicts maps dimension identifier to their correspoinding dimension_to_values dictionary
            # e.g global/gender/restricted -> {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20...}
            dimension_id_to_dimension_values_dicts: Dict[
                str, Dict[DimensionBase, Optional[float]]
            ] = {}
            for dimension_datapoint in self.dimension_datapoints:

                if len(dimension_datapoint.dimension_identifier_to_member) > 1:
                    raise JusticeCountsDataError(
                        code="invalid_datapoint",
                        description=(
                            "Datapoint represents multiple dimensions. "
                            f"Datapoint ID: {dimension_datapoint.id}."
                        ),
                    )
                # example: dimension_identifier = "global/gender/restricted"
                dimension_identifier = list(
                    dimension_datapoint.dimension_identifier_to_member.keys()
                ).pop()
                # example: dimension_name = "MALE"
                dimension_name = list(
                    dimension_datapoint.dimension_identifier_to_member.values()
                ).pop()

                dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
                    dimension_identifier
                ]  # example: dimension_class = GenderRestricted

                curr_dimension_to_values = dimension_id_to_dimension_values_dicts.get(
                    dimension_identifier,
                    {d: None for d in dimension_class},
                )  # example: curr_dimension_to_values = {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: None, GenderRestricted.NON_BINARY: None...}

                curr_dimension_to_values[
                    dimension_class[dimension_name]
                ] = dimension_datapoint.get_value()
                # update curr_dimension_to_values to add new dimension datapoint.
                # example: curr_dimension_to_values = {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20, GenderRestricted.NON_BINARY: None...}
                dimension_id_to_dimension_values_dicts[
                    dimension_identifier
                ] = curr_dimension_to_values
                # update dimension_id_to_dimension_values_dicts dictionary -> {"global/gender/restricted": {GenderRestricted.FEMALE: 10, GenderRestricted.MALE: 20, GenderRestricted.NON_BINARY: None...}

            return [
                ReportedAggregatedDimension(
                    dimension_to_value=dimension_id_to_dimension_values_dicts.get(
                        aggregated_dimension.dimension_identifier(),
                        {
                            d: None
                            for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                                aggregated_dimension.dimension.dimension_identifier()
                            ]
                        },
                    )
                )
                for aggregated_dimension in metric_definition.aggregated_dimensions
                or []
                if not aggregated_dimension.disabled
            ]

    @staticmethod
    def get_report_by_id(session: Session, report_id: int) -> schema.Report:
        return session.query(schema.Report).filter(schema.Report.id == report_id).one()

    @staticmethod
    def get_reports_by_agency_id(
        session: Session, agency_id: int
    ) -> List[schema.Report]:
        return (
            session.query(schema.Report)
            .filter(schema.Report.source_id == agency_id)
            .order_by(schema.Report.date_range_end.desc())
            .all()
        )

    @staticmethod
    def update_report_metadata(
        session: Session,
        report_id: int,
        editor_id: int,
        status: Optional[str] = None,
    ) -> schema.Report:
        report = ReportInterface.get_report_by_id(session=session, report_id=report_id)

        if status and report.status.value != status:
            report.status = schema.ReportStatus[status]

        already_modified_by = set(report.modified_by or [])
        if editor_id not in already_modified_by:
            modified_by = list(already_modified_by) + [editor_id]
        else:
            # bump most recent modifier to end of list
            modified_by = list(already_modified_by - {editor_id}) + [editor_id]
        report.modified_by = modified_by

        report.last_modified_at = datetime.datetime.utcnow()

        session.commit()
        return report

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
        is_recurring: bool = False,
        recurring_report: Optional[schema.Report] = None,
    ) -> schema.Report:
        """Creates empty report in Justice Counts DB"""
        report = ReportInterface.create_report_object(
            agency_id=agency_id,
            user_account_id=user_account_id,
            year=year,
            month=month,
            frequency=frequency,
            is_recurring=is_recurring,
            recurring_report=recurring_report,
        )
        session.add(report)
        session.commit()
        return report

    @staticmethod
    def create_report_object(
        agency_id: int,
        user_account_id: int,
        year: int,
        month: int,
        frequency: str,
        is_recurring: bool = False,
        recurring_report: Optional[schema.Report] = None,
        last_modified_at: Optional[datetime.datetime] = None,
    ) -> schema.Report:
        report_type = (
            ReportingFrequency.MONTHLY.value
            if frequency == ReportingFrequency.MONTHLY.value
            else ReportingFrequency.ANNUAL.value
        )
        date_range_start = datetime.date(year, month, 1)
        date_range_end = (
            datetime.date(
                year if month != 12 else (year + 1),
                ((month + 1) if month != 12 else 1),
                1,
            )
            if frequency == ReportingFrequency.MONTHLY.value
            else datetime.date(year + 1, month, 1)
        )
        return schema.Report(
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
            last_modified_at=last_modified_at or datetime.datetime.utcnow(),
            modified_by=[user_account_id],
            is_recurring=is_recurring,
            recurring_report=recurring_report,
        )

    @staticmethod
    def to_json_response(session: Session, report: schema.Report) -> Dict[str, Any]:
        editor_names = [
            UserAccountInterface.get_user_by_id(
                session=session, user_account_id=id
            ).name_or_email()
            for id in report.modified_by or []
        ]

        reporting_frequency = report.get_reporting_frequency()
        return {
            "id": report.id,
            "agency_id": report.source_id,
            "year": report.date_range_start.year,
            "month": report.date_range_start.month,
            "frequency": reporting_frequency.value,
            "last_modified_at": report.last_modified_at,
            "editors": editor_names,
            "status": report.status.value,
            "is_recurring": report.is_recurring,
        }

    @staticmethod
    def add_or_update_metric(
        session: Session,
        report: schema.Report,
        report_metric: ReportMetric,
        user_account: schema.UserAccount,
    ) -> None:
        """Given a Report and a ReportMetric, either add this metric
        to the report, or if the metric already exists on the report,
        update the existing metric in-place.

        Adding (or updating) a metric to a report actually involves adding
        a row to the datapoint table for each value submitted (total and
        breakdown values) as well as context. If no value is reported for a
        particular field, a new datapoint will be added to the datapoint table
        with a value of None.
        """
        # First, add a datapoint for the aggregated_value
        current_time = datetime.datetime.utcnow()
        metric_definition = METRIC_KEY_TO_METRIC[report_metric.key]
        DatapointInterface.add_datapoint(
            session=session,
            user_account=user_account,
            current_time=current_time,
            metric_definition_key=metric_definition.key,
            report=report,
            value=report_metric.value,
        )

        # Next, add a datapoint for each dimension
        all_dimensions_to_values: Dict[DimensionBase, Optional[float]] = {}
        for reported_aggregated_dimension in report_metric.aggregated_dimensions or []:
            all_dimensions_to_values.update(
                reported_aggregated_dimension.dimension_to_value
            )

        for aggregated_dimension in metric_definition.aggregated_dimensions or []:
            for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                aggregated_dimension.dimension.dimension_identifier()
            ]:
                DatapointInterface.add_datapoint(
                    session=session,
                    user_account=user_account,
                    current_time=current_time,
                    metric_definition_key=metric_definition.key,
                    report=report,
                    value=all_dimensions_to_values.get(d),
                    dimension=d,
                )

        # Finally, add a datapoint for each context
        context_key_to_value = {
            context.key: context.value for context in report_metric.contexts or []
        }
        for context in metric_definition.contexts or []:
            DatapointInterface.add_datapoint(
                session=session,
                user_account=user_account,
                current_time=current_time,
                metric_definition_key=metric_definition.key,
                report=report,
                value=context_key_to_value.get(context.key),
                context_key=context.key,
                value_type=context.value_type,
            )
        session.commit()

    @staticmethod
    def get_metrics_by_report_id(
        session: Session, report_id: int
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
        2. Look up data that already exists on the report that is stored in Datapoint model.
        3. Perform matching between the MetricDefinitions and the existing data.
        4. Return a list of ReportMetrics. If the agency has not filled out data for a
           metric, its values will be None; otherwise they will be populated from the data
           already stored in our database.
        """

        report = ReportInterface.get_report_by_id(session=session, report_id=report_id)
        # We determine which metrics to include on this report based on:
        #   - Agency system (e.g. only law enforcement)
        #   - Report frequency (e.g. only annual metrics)
        metric_definitions = ReportInterface.get_metric_definitions_by_report_type(
            report_type=report.type,
            systems=set(report.source.systems) if report.source.systems is not None
            # TODO(#13216): Get rid of this `source.system` after migrating over to `systems`
            else {report.source.system.value},
        )
        # If data has already been reported for some metrics on this report,
        # then `report.datapoints` will be non-empty.
        metric_key_to_data_points = ReportInterface._build_metric_key_to_data_points(
            datapoints=report.datapoints
        )

        report_metrics = []
        # For each metric that should be filled out on this report,
        # construct a ReportMetric object
        for metric_definition in metric_definitions:
            reported_datapoints = metric_key_to_data_points.get(
                metric_definition.key, ReportInterface.DatapointsForMetricDefinition()
            )
            report_metrics.append(
                ReportMetric(
                    key=metric_definition.key,
                    value=reported_datapoints.aggregated_value,
                    contexts=reported_datapoints.get_reported_contexts(
                        # convert context datapoints to ReportedContexts
                        metric_definition=metric_definition
                    ),
                    aggregated_dimensions=reported_datapoints.get_reported_aggregated_dimensions(
                        # convert dimension datapoints to ReportedAggregatedDimensions
                        metric_definition=metric_definition
                    ),
                )
            )

        return report_metrics

    @staticmethod
    def _build_metric_key_to_data_points(
        datapoints: List[schema.Datapoint],
    ) -> Dict[str, DatapointsForMetricDefinition]:
        """Associate the datapoints with their metric and sort each datapoint by what
        they represent (context, dimension, or aggregated_value). metric_key_to_data_points
        is a dictionary of DatapointsForMetricDefinition. Each metric definition key points to a
        DatapointsForMetricDefinition instance that stores datapoints by context, disaggregations,
        or aggregated_value.
        """
        metric_key_to_data_points = {}
        for datapoint in datapoints:
            if datapoint.metric_definition_key not in metric_key_to_data_points:
                metric_key_to_data_points[
                    datapoint.metric_definition_key
                ] = ReportInterface.DatapointsForMetricDefinition()
            metric_datapoints = metric_key_to_data_points[
                datapoint.metric_definition_key
            ]
            if datapoint.context_key is not None:
                metric_datapoints.context_datapoints.append(datapoint)
            elif datapoint.dimension_identifier_to_member is not None:
                metric_datapoints.dimension_datapoints.append(datapoint)
            elif (
                datapoint.dimension_identifier_to_member is None
                and datapoint.context_key is None
            ):
                metric_datapoints.aggregated_value = datapoint.get_value()
            else:
                raise JusticeCountsDataError(
                    code="invalid_datapoint",
                    description=(
                        "Datapoint does not represent a dimension, "
                        "aggregate value, or context.",
                    ),
                )
        return metric_key_to_data_points

    @staticmethod
    def get_metric_definitions_by_report_type(
        report_type: schema.ReportingFrequency,
        systems: Set[str],
    ) -> List[MetricDefinition]:
        return [
            metric
            for metric in METRICS
            if metric.system.value in systems
            and report_type in {freq.value for freq in metric.reporting_frequencies}
        ]
