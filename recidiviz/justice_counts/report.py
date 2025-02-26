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
import json
from typing import Any, Dict, List, Optional, Set, Tuple, TypeVar

from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Query, Session, joinedload, lazyload

from recidiviz.justice_counts.datapoint import DatapointInterface, DatapointUniqueKey
from recidiviz.justice_counts.datapoints_for_metric import DatapointsForMetric
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_definition import (
    MetricDefinition,
    ReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.persistence.database.schema.justice_counts import schema

from .utils.date_utils import convert_date_range_to_year_month

DatapointsForMetricT = TypeVar("DatapointsForMetricT", bound="DatapointsForMetric")


class ReportInterface:
    """Contains methods for setting and getting Report info."""

    ### Fetch from the DB ###

    @staticmethod
    def _get_report_query(session: Session, include_datapoints: bool = True) -> Query:
        q = session.query(schema.Report)

        # Always lazily load report table instances -- we never need those for the Control Panel
        q = q.options(lazyload(schema.Report.report_table_instances))

        # Always eagerly load the report source since we'll need to lookup the systems
        q = q.options(joinedload(schema.Report.agency))

        if not include_datapoints:
            # If we don't need the datapoints for a given report in this query
            # (e.g. we're just showing the reports page, so we only need report metadata),
            # then we should lazily load them too
            q = q.options(lazyload(schema.Report.datapoints))

        return q

    @staticmethod
    def get_report_by_id(
        session: Session, report_id: int, include_datapoints: bool = True
    ) -> schema.Report:
        q = ReportInterface._get_report_query(
            session, include_datapoints=include_datapoints
        )
        return q.filter(schema.Report.id == report_id).one()

    @staticmethod
    def get_reports_by_id(
        session: Session, report_ids: list, include_datapoints: bool = True
    ) -> schema.Report:
        q = ReportInterface._get_report_query(
            session, include_datapoints=include_datapoints
        )
        return q.filter(schema.Report.id.in_(report_ids)).all()

    @staticmethod
    def get_reports_by_agency_id(
        session: Session,
        agency_id: int,
        include_datapoints: bool = False,
        published_only: bool = False,
    ) -> List[schema.Report]:
        q = ReportInterface._get_report_query(
            session, include_datapoints=include_datapoints
        )

        if published_only:
            q = q.filter(schema.Report.status == schema.ReportStatus.PUBLISHED)

        return (
            q.filter(schema.Report.source_id == agency_id)
            .order_by(schema.Report.date_range_end.desc())
            .all()
        )

    @staticmethod
    def get_reports_for_agency_dashboard(
        session: Session,
        agency_id: int,
    ) -> List[schema.Report]:
        """Returns published reports for specified agency. To improve performance,
        rather than returning fully instantiated report objects, we return a tuple
        containing just the properties we need.
        """
        return (
            session.query(
                schema.Report.id,
                schema.Report.type,
                schema.Report.date_range_start,
                schema.Report.date_range_end,
            )
            .filter(schema.Report.status == schema.ReportStatus.PUBLISHED)
            .filter(schema.Report.source_id == agency_id)
        ).all()

    @staticmethod
    def get_reports_by_agency_ids(
        session: Session,
        agency_ids: List[int],
        include_datapoints: bool = False,
        published_only: bool = False,
    ) -> List[schema.Report]:
        q = ReportInterface._get_report_query(
            session, include_datapoints=include_datapoints
        )

        if published_only:
            q = q.filter(schema.Report.status == schema.ReportStatus.PUBLISHED)

        return (
            q.filter(schema.Report.source_id.in_(agency_ids))
            .order_by(schema.Report.date_range_end.desc())
            .all()
        )

    @staticmethod
    def get_report_ids_by_agency_id(
        session: Session,
        agency_id: int,
    ) -> List[int]:
        """Get a list of report IDs by agency ID"""
        report_id_tuples = (
            session.query(schema.Report.id)
            .filter(schema.Report.source_id == agency_id)
            .all()
        )
        return [tuple[0] for tuple in report_id_tuples]

    ### Update DB ###

    @staticmethod
    def delete_reports_by_id(session: Session, report_ids: List[int]) -> None:
        session.query(schema.Report).filter(schema.Report.id.in_(report_ids)).delete()

    @staticmethod
    def update_report_metadata(
        report: schema.Report,
        editor_id: Optional[int] = None,
        status: Optional[str] = None,
    ) -> schema.Report:
        if status == schema.ReportStatus.PUBLISHED.value:
            report.publish_date = datetime.date.today()

        if status and report.status.value != status:
            report.status = schema.ReportStatus[status]

        already_modified_by = set(report.modified_by or [])
        if editor_id not in already_modified_by:
            modified_by = list(already_modified_by) + [editor_id]
        else:
            # bump most recent modifier to end of list
            modified_by = list(already_modified_by - {editor_id}) + [editor_id]
        report.modified_by = modified_by
        report.last_modified_at = datetime.datetime.now(tz=datetime.timezone.utc)
        return report

    ### Insert into DB ###

    @staticmethod
    def create_report_if_not_exists(
        session: Session,
        agency_id: int,
        user_account_id: Optional[int],
        year: int,
        month: int,
        frequency: str,
    ) -> Optional[schema.Report]:
        """Creates a new report for the agency if a report for the given year, month,
        and frequency does not already exist. If report exists, return None."""
        try:
            report = ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                user_account_id=user_account_id,
                month=month,
                year=year,
                frequency=frequency,
            )
            session.add(report)
            session.commit()
            return report
        except IntegrityError as e:
            session.rollback()
            if isinstance(e.orig, UniqueViolation):
                return None
            raise e

    @staticmethod
    def create_new_reports(
        session: Session,
        agency_id: int,
        user_account_id: Optional[int],
        current_month: int,
        current_year: int,
        previous_month: int,
        previous_year: int,
        systems: Set[schema.System],
        metric_key_to_datapoints: Dict[str, DatapointsForMetricT],
    ) -> Tuple[
        Optional[schema.Report],
        Optional[schema.Report],
        List[MetricDefinition],
        List[MetricDefinition],
    ]:
        """Creates a new monthly report (for the most recent previous month/year) and annual report
        (for the most recent previous month/year) for the agency if those reports do not already
        exist and reports have metrics available."""
        monthly_report = None
        yearly_report = None

        # Check that the agency required monthly metrics for this month
        try:
            monthly_metric_defs = DatapointsForMetric.get_metric_definitions_for_report(
                systems=systems,
                metric_key_to_datapoints=metric_key_to_datapoints,
                report_frequency=ReportingFrequency.MONTHLY.value,
                starting_month=previous_month,
            )
        except JusticeCountsServerError:
            monthly_metric_defs = []

        # Create monthly report if does not exist
        if len(monthly_metric_defs) > 0:
            monthly_report = ReportInterface.create_report_if_not_exists(
                session,
                agency_id,
                user_account_id,
                previous_year if previous_month == 12 else current_year,
                previous_month,
                ReportingFrequency.MONTHLY.value,
            )

        # Check that the agency required annual metrics for this month
        try:
            annual_metric_defs = DatapointsForMetric.get_metric_definitions_for_report(
                systems=systems,
                metric_key_to_datapoints=metric_key_to_datapoints,
                report_frequency=ReportingFrequency.ANNUAL.value,
                starting_month=current_month,
            )
        except JusticeCountsServerError:
            annual_metric_defs = []

        # Create yearly report if report does not exist
        if len(annual_metric_defs) > 0:
            yearly_report = ReportInterface.create_report_if_not_exists(
                session,
                agency_id,
                user_account_id,
                previous_year,
                current_month,
                ReportingFrequency.ANNUAL.value,
            )

        return monthly_report, yearly_report, monthly_metric_defs, annual_metric_defs

    @staticmethod
    def create_report(
        session: Session,
        agency_id: int,
        user_account_id: Optional[int],
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
        return report

    @staticmethod
    def create_report_object(
        agency_id: int,
        user_account_id: Optional[int],
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
        date_range_start, date_range_end = ReportInterface.get_date_range(
            year=year, month=month, frequency=frequency
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
            last_modified_at=last_modified_at
            or datetime.datetime.now(tz=datetime.timezone.utc),
            modified_by=[user_account_id] if user_account_id is not None else [],
            is_recurring=is_recurring,
            recurring_report=recurring_report,
        )

    ### Export to FE ###

    @staticmethod
    def to_json_response(
        report: schema.Report,
        editor_id_to_json: Dict[int, Dict[str, str]],
        agency_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Editor names will be displayed in reverse chronological order in
        # an agency's reports table.
        editors_reverse_chron = (
            reversed(report.modified_by) if report.modified_by is not None else []
        )
        editor_json = [
            editor_id_to_json[id]
            for id in editors_reverse_chron
            if id in editor_id_to_json
        ]

        reporting_frequency = ReportInterface.get_reporting_frequency(report=report)
        return {
            "id": report.id,
            "agency_id": report.source_id,
            "agency_name": agency_name,
            "year": report.date_range_start.year,
            "month": report.date_range_start.month,
            "frequency": reporting_frequency.value,
            "last_modified_at": report.last_modified_at,
            "last_modified_at_timestamp": report.last_modified_at.timestamp()
            if report.last_modified_at is not None
            else None,
            "editors": editor_json,
            "status": report.status.value,
            "is_recurring": report.is_recurring,
        }

    ### Get Path ###

    @staticmethod
    def get_metrics_by_report(
        session: Session,
        report: schema.Report,
        agency_datapoints: Optional[List[schema.Datapoint]] = None,
        is_superagency: Optional[bool] = False,
    ) -> List[MetricInterface]:
        """Given a report, determine all MetricDefinitions that must be populated
        on this report, and convert them to MetricInterfaces. If the agency has already
        started filling out the report, populate the MetricInterfaces with those values.
        This method will be used to send a list of Metrics to the frontend to render
        the report form in the Control Panel.

        This involves the following logic:
        1. Filter the MetricDefinitions in our registry to just those that are applicable
           to this report, i.e. those that belong to the same criminal justice system pillar
           as the reporting agency, and those that match the reporting frequency of
           the given report.
        2. Look up data that already exists on the report that is stored in Datapoint model.
        3. Perform matching between the MetricDefinitions and the existing data.
        4. Return a list of MetricInterfaces. If the agency has not filled out data for a
           metric, its values will be None; otherwise they will be populated from the data
           already stored in our database.
        """

        if agency_datapoints is None:
            agency_datapoints = DatapointInterface.get_agency_datapoints(
                session=session, agency_id=report.source_id
            )

        # If data has already been reported for some metrics on this report,
        # then `report.datapoints` will be non-empty. We also send build the
        # DatapointsForMetric class with agency datapoints to see
        # what metrics are enabled and disabled.
        metric_key_to_datapoints = DatapointInterface.build_metric_key_to_datapoints(
            datapoints=report.datapoints + agency_datapoints
        )

        # We determine which metrics to include on this report based on:
        #   - Agency system (e.g. only law enforcement)
        #   - Report frequency (e.g. only annual metrics)
        metric_definitions = DatapointsForMetric.get_metric_definitions_for_report(
            report_frequency=report.type,
            systems={schema.System[system] for system in report.agency.systems or []},
            starting_month=report.date_range_start.month,
            metric_key_to_datapoints=metric_key_to_datapoints,
        )

        report_metrics = []
        # For each metric that should be filled out on this report,
        # construct a MetricInterface object

        for metric_definition in metric_definitions:
            # For superagencies: only include superagency metrics and
            # exclude all other system metrics
            if (
                is_superagency is True
                and metric_definition.system != schema.System.SUPERAGENCY
            ):
                continue

            reported_datapoints = metric_key_to_datapoints.get(
                metric_definition.key,
                DatapointsForMetric(),
            )

            # If this is a supervision subsystem metric, and the metric is not
            # supposed to be disaggregated by supervision subsystems, then
            # disable the metric
            enabled = reported_datapoints.is_metric_enabled
            if reported_datapoints.is_metric_enabled is None:
                if (
                    metric_definition.system in schema.System.supervision_subsystems()
                    and not reported_datapoints.disaggregated_by_supervision_subsystems
                ):
                    enabled = False

            report_metrics.append(
                MetricInterface(
                    key=metric_definition.key,
                    value=reported_datapoints.aggregated_value,
                    is_metric_enabled=enabled,
                    aggregated_dimensions=reported_datapoints.get_aggregated_dimension_data(
                        # convert dimension datapoints to MetricAggregatedDimensionData
                        metric_definition=metric_definition
                    ),
                )
            )

        return report_metrics

    ### Save Path ###

    @staticmethod
    def add_or_update_metric(
        session: Session,
        report: schema.Report,
        report_metric: MetricInterface,
        uploaded_via_breakdown_sheet: bool = False,
        existing_datapoints_dict: Optional[
            Dict[DatapointUniqueKey, schema.Datapoint]
        ] = None,
        user_account: Optional[schema.UserAccount] = None,
        agency: Optional[schema.Agency] = None,
    ) -> List[DatapointJson]:
        """Given a Report and a MetricInterface, either add this metric
        to the report, or if the metric already exists on the report,
        update the existing metric in-place.

        Adding (or updating) a metric to a report actually involves adding
        a row to the datapoint table for each value submitted (total and
        breakdown values) as well as context. If no value is reported for a
        particular field, a new datapoint will be added to the datapoint table
        with a value of None.

        The only exception to the above is if `uploaded_via_breakdown_sheet`
        is True. in this case, if `datapoint.value` is None, we ignore it,
        and fallback to whatever value is already in the db. If `datapoint.value`
        is specified, prefer the existing value in the db, unless there isn't one,
        in which case we save the incoming value.
        """
        existing_datapoints_dict = (
            existing_datapoints_dict
            or ReportInterface.get_existing_datapoints_dict(reports=[report])
        )

        datapoint_json_list: List[Optional[DatapointJson]] = []
        # First, add a datapoint for the aggregated_value
        current_time = datetime.datetime.now(tz=datetime.timezone.utc)
        metric_definition = METRIC_KEY_TO_METRIC[report_metric.key]

        # If we're not supposed to use the existing aggregate value, then we should
        # definitely perform the add/update. If we're supposed to use the existing
        # value but the incoming datapoint has its own value, we should still go
        # into this method, because if there is no existing value in the DB,
        # we should save the incoming one.
        if not uploaded_via_breakdown_sheet or report_metric.value is not None:
            datapoint_json_list.append(
                DatapointInterface.add_datapoint(
                    session=session,
                    existing_datapoints_dict=existing_datapoints_dict,
                    user_account=user_account,
                    current_time=current_time,
                    metric_definition_key=metric_definition.key,
                    report=report,
                    value=report_metric.value,
                    uploaded_via_breakdown_sheet=uploaded_via_breakdown_sheet,
                    agency=agency,
                )
            )

        # Next, add a datapoint for each dimension
        all_dimensions_to_values: Dict[DimensionBase, Any] = {}
        for reported_aggregated_dimension in report_metric.aggregated_dimensions:
            if reported_aggregated_dimension.dimension_to_value:
                all_dimensions_to_values.update(
                    reported_aggregated_dimension.dimension_to_value
                )

        for aggregated_dimension in metric_definition.aggregated_dimensions or []:
            for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                aggregated_dimension.dimension.dimension_identifier()
            ]:  # type: ignore[attr-defined]
                if d not in all_dimensions_to_values:
                    # If this dimension wasn't reported, skip it. Don't add a blank
                    # datapoint, which will overwrite any previously reported values.
                    continue

                datapoint_json_list.append(
                    DatapointInterface.add_datapoint(
                        session=session,
                        existing_datapoints_dict=existing_datapoints_dict,
                        user_account=user_account,
                        current_time=current_time,
                        metric_definition_key=metric_definition.key,
                        report=report,
                        value=all_dimensions_to_values[d],
                        dimension=d,
                        agency=agency,
                    )
                )

        # Finally, add a datapoint for each context
        context_key_to_value = {
            context.key: context.value for context in report_metric.contexts
        }
        for context in metric_definition.contexts or []:
            if context.key not in context_key_to_value:
                # If this context wasn't reported, skip it. Don't add a blank
                # datapoint, which will overwrite any previously reported values.
                continue

            datapoint_json_list.append(
                DatapointInterface.add_datapoint(
                    session=session,
                    existing_datapoints_dict=existing_datapoints_dict,
                    user_account=user_account,
                    current_time=current_time,
                    metric_definition_key=metric_definition.key,
                    report=report,
                    value=context_key_to_value[context.key],
                    context_key=context.key,
                    value_type=context.value_type,
                    agency=agency,
                )
            )
        return [dp for dp in datapoint_json_list if dp is not None]

    ### Helpers ###

    @staticmethod
    def get_existing_datapoints_dict(
        reports: List[schema.Report],
    ) -> Dict[DatapointUniqueKey, schema.Datapoint]:
        """Fetches all datapoints from the given list of reports. Returns a
        dictionary of these datapoints keyed by their unique ID, which is a tuple of
        <report time range, metric definition, context key, disaggregations>
        """
        return {
            (
                report.date_range_start,
                report.date_range_end,
                report.source_id,
                datapoint.metric_definition_key,
                datapoint.context_key,
                datapoint.dimension_identifier_to_member
                if (
                    isinstance(datapoint.dimension_identifier_to_member, str)
                    or datapoint.dimension_identifier_to_member is None
                )
                else json.dumps(datapoint.dimension_identifier_to_member),
            ): datapoint
            for report in reports
            for datapoint in report.datapoints
        }

    @staticmethod
    def get_date_range(
        year: int, month: int, frequency: str
    ) -> Tuple[datetime.date, datetime.date]:
        """Given a year, month, and reporting frequency, determine the
        start and end date for the report.
        """
        if frequency == ReportingFrequency.MONTHLY.value:
            date_range_start = datetime.date(year, month, 1)
            date_range_end = datetime.date(
                year if month != 12 else (year + 1),
                ((month + 1) if month != 12 else 1),
                1,
            )
        else:
            if month != 1:
                # For non-calendar year annual reports, the year column
                # matches the end year of the record, not the start year.
                date_range_start = datetime.date(year - 1, month, 1)
                date_range_end = datetime.date(year, month, 1)
            else:
                date_range_start = datetime.date(year, month, 1)
                date_range_end = datetime.date(year + 1, month, 1)

        return (date_range_start, date_range_end)

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
    def get_reporting_frequency(report: schema.Report) -> ReportingFrequency:
        _, month = convert_date_range_to_year_month(
            start_date=report.date_range_start, end_date=report.date_range_end
        )
        if month is None:
            inferred_frequency = ReportingFrequency.ANNUAL
        else:
            inferred_frequency = ReportingFrequency.MONTHLY

        report_type_string = str(report.type)
        if report_type_string.strip() != str(inferred_frequency.value):
            raise ValueError(
                f"Invalid Report Type: Report type is {report_type_string}, "
                f"but inferred a reporting frequency of {str(inferred_frequency.value)} "
                "from the report date range."
            )
        return inferred_frequency

    ### Misc ###

    @staticmethod
    def create_reports_for_new_agency(
        session: Session, agency_id: int, user_account_id: Optional[int] = None
    ) -> None:
        date = datetime.date.today()
        # create twelve monthly reports for the last 12 months
        for month in range(1, min(13, date.month + 3)):
            # create reports for the months that already passed this year, plus for the two following months
            ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                month=month,
                user_account_id=user_account_id,
                year=date.year,
                frequency=schema.ReportingFrequency.MONTHLY.value,
            )
        for month in range(date.month, 13):
            # create monthly reports for the previous year
            ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                month=month,
                user_account_id=user_account_id,
                year=date.year - 1,
                frequency=schema.ReportingFrequency.MONTHLY.value,
            )
        # create an annual report for the agency for the previous calendar year
        ReportInterface.create_report(
            session=session,
            agency_id=agency_id,
            month=1,
            user_account_id=user_account_id,
            year=date.year - 1,
            frequency=schema.ReportingFrequency.ANNUAL.value,
        )
