# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""This class implements tests for the Justice Counts Publisher emails."""
import datetime
import itertools
from typing import Dict, List, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.justice_counts.metrics import law_enforcement, prisons, supervision
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS_BY_SYSTEM,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import (
    DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
    REPORTING_FREQUENCY_CONTEXT_KEY,
    UploadMethod,
)
from recidiviz.justice_counts.utils.email import (
    get_missing_metrics,
    get_missing_metrics_for_superagencies,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


class TestEmails(JusticeCountsDatabaseTestCase):
    """Implements tests for Publisher email functionality"""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.today = datetime.date.today()
        self.january_start_date = datetime.date(
            month=1, year=self.today.year - 1, day=1
        )
        self.january_end_date = datetime.date(month=1, year=self.today.year, day=1)
        self.july_start_date = datetime.date(
            month=7,
            year=self.today.year - 1 if self.today.month >= 7 else self.today.year - 2,
            day=1,
        )
        self.july_end_date = datetime.date(
            month=7,
            year=self.today.year if self.today.month >= 7 else self.today.year - 1,
            day=1,
        )

    def get_monthly_report(self, agency: schema.Agency) -> schema.Report:
        return schema.Report(
            source=agency,
            type="MONTHLY",
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            instance="Test Monthly Report",
            status=schema.ReportStatus.NOT_STARTED,
            date_range_start=(self.today - relativedelta(months=1)).replace(day=1),
            date_range_end=self.today.replace(day=1),
        )

    def get_annual_calendar_year_report(self, agency: schema.Agency) -> schema.Report:
        return schema.Report(
            source=agency,
            type="ANNUAL",
            instance="Test Annual Calendar-Year Report",
            status=schema.ReportStatus.NOT_STARTED,
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            date_range_start=self.january_start_date,
            date_range_end=self.january_end_date,
        )

    def annual_fiscal_year_report(self, agency: schema.Agency) -> schema.Report:
        return schema.Report(
            source=agency,
            type="ANNUAL",
            instance="Test Annual Fiscal-Year Report",
            status=schema.ReportStatus.NOT_STARTED,
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            date_range_start=self.july_start_date,
            date_range_end=self.july_end_date,
        )

    def get_enabled_metric_setting_datapoints(
        self, metric_definitions: List[MetricDefinition], agency: schema.Agency
    ) -> List[schema.Datapoint]:
        enabled_metrics = [
            schema.Datapoint(
                metric_definition_key=metric_definition.key,
                enabled=True,
                source=agency,
                is_report_datapoint=False,
            )
            for metric_definition in metric_definitions
        ]
        return enabled_metrics

    def get_custom_reported_metrics(
        self,
        agency: schema.Agency,
        starting_month: int,
        metric_definitions: List[MetricDefinition],
    ) -> List[schema.Datapoint]:
        fiscal_frequency = CustomReportingFrequency(
            starting_month=starting_month, frequency=schema.ReportingFrequency.ANNUAL
        )
        return [
            schema.Datapoint(
                metric_definition_key=metric_definition.key,
                context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
                value=fiscal_frequency.to_json_str(),
                source=agency,
                is_report_datapoint=False,
            )
            for metric_definition in metric_definitions
        ]

    def _test_missing_metrics_functionality_law_enforcement_and_prison_agency(
        self,
        system_to_missing_monthly_metrics: Dict[schema.System, List[MetricDefinition]],
        date_range_to_system_to_missing_annual_metrics: Dict[
            Tuple[datetime.date, datetime.date],
            Dict[schema.System, List[MetricDefinition]],
        ],
        agency: schema.Agency,
        monthly_report_date_range: Tuple[datetime.date, datetime.date],
    ) -> None:
        """
        Shared tests for test_get_missing_metrics_empty_reports and
        test_get_missing_metrics_non_existing_reports before metrics are
        disabled.
        """
        self.assertEqual(
            set(system_to_missing_monthly_metrics.keys()),
            {schema.System.LAW_ENFORCEMENT, schema.System.PRISONS},
        )
        self.assertEqual(
            set(date_range_to_system_to_missing_annual_metrics.keys()),
            {
                (self.july_start_date, self.july_end_date),
                (self.january_start_date, self.january_end_date),
            },
        )

        for system in agency.systems:
            # All metrics are missing from the monthly report
            self.assertEqual(
                {
                    definition.key
                    for definition in system_to_missing_monthly_metrics[
                        schema.System[system]
                    ]
                },
                {
                    metric_definition.key
                    for metric_definition in METRICS_BY_SYSTEM[system]
                    if metric_definition.reporting_frequency
                    == schema.ReportingFrequency.MONTHLY
                },
            )

            # All annual metrics (except expenses, that is reported fiscally) is
            # missing from the calendar-year report
            self.assertEqual(
                {
                    definition.key
                    for definition in date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ][schema.System[system]]
                },
                {
                    metric_definition.key
                    for metric_definition in METRICS_BY_SYSTEM[system]
                    if metric_definition.reporting_frequency
                    == schema.ReportingFrequency.ANNUAL
                    if "EXPENSES" not in metric_definition.key
                },
            )

            # The expense metric is missing from the fiscal-year report.
            self.assertEqual(
                {
                    definition.key
                    for definition in date_range_to_system_to_missing_annual_metrics[
                        (self.july_start_date, self.july_end_date)
                    ][schema.System[system]]
                },
                {
                    metric_definition.key
                    for metric_definition in METRICS_BY_SYSTEM[system]
                    if metric_definition.reporting_frequency
                    == schema.ReportingFrequency.ANNUAL
                    if "EXPENSES" in metric_definition.key
                },
            )

        for date_range in [
            (self.january_start_date, self.january_end_date),
            (self.july_start_date, self.july_end_date),
        ]:
            # Missing fiscal-year metrics and missing calendar-year metrics are represented in
            # date_range_to_system_to_missing_annual_metrics
            self.assertEqual(
                set(date_range_to_system_to_missing_annual_metrics[date_range].keys()),
                {schema.System.LAW_ENFORCEMENT, schema.System.PRISONS},
            )

        monthly_report = self.get_monthly_report(agency=agency)
        self.assertEqual(
            monthly_report_date_range,
            (monthly_report.date_range_start, monthly_report.date_range_end),
        )

    def _test_missing_metrics_functionality_law_enforcement_and_prison_agency_disabled_metrics(
        self,
        system_to_missing_monthly_metrics: Dict[schema.System, List[MetricDefinition]],
        date_range_to_system_to_missing_annual_metrics: Dict[
            Tuple[datetime.date, datetime.date],
            Dict[schema.System, List[MetricDefinition]],
        ],
        agency: schema.Agency,
        monthly_report_date_range: Tuple[datetime.date, datetime.date],
    ) -> None:
        """
        Shared tests for test_get_missing_metrics_empty_reports and
        test_get_missing_metrics_non_existing_reports after metrics are
        disabled.
        """
        for system in agency.systems:
            # All monthly metrics are missing
            self.assertEqual(
                {
                    definition.key
                    for definition in system_to_missing_monthly_metrics[
                        schema.System[system]
                    ]
                },
                {
                    metric_definition.key
                    for metric_definition in METRICS_BY_SYSTEM[system]
                    if metric_definition.reporting_frequency
                    == schema.ReportingFrequency.MONTHLY
                },
            )
            # Funding and Expenses metrics are not included as missing annual metrics
            self.assertEqual(
                {
                    definition.key
                    for definition in date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ][schema.System[system]]
                },
                {
                    metric_definition.key
                    for metric_definition in METRICS_BY_SYSTEM[system]
                    if metric_definition.reporting_frequency
                    == schema.ReportingFrequency.ANNUAL
                    and "FUNDING" not in metric_definition.key
                    and "EXPENSES" not in metric_definition.key
                },
            )

            monthly_report = self.get_monthly_report(agency=agency)
            self.assertEqual(
                monthly_report_date_range,
                (monthly_report.date_range_start, monthly_report.date_range_end),
            )

    def test_get_missing_metrics_empty_reports(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
                schema.System.PRISONS.value,
            ]

            monthly_report = self.get_monthly_report(agency=agency)
            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )
            annual_fiscal_year_report = self.annual_fiscal_year_report(agency=agency)

            # Enable all metrics
            enabled_metrics = self.get_enabled_metric_setting_datapoints(
                metric_definitions=METRICS_BY_SYSTEM[
                    schema.System.LAW_ENFORCEMENT.value
                ]
                + METRICS_BY_SYSTEM[schema.System.PRISONS.value],
                agency=agency,
            )

            # Set expenses to be reported fiscally
            custom_reported_metrics = self.get_custom_reported_metrics(
                starting_month=7,
                agency=agency,
                metric_definitions=[prisons.expenses, law_enforcement.expenses],
            )

            session.add_all(
                [
                    agency,
                    monthly_report,
                    annual_calendar_year_report,
                    annual_fiscal_year_report,
                ]
                + enabled_metrics
                + custom_reported_metrics
            )
            session.commit()

            (
                system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(agency=agency, session=session, today=self.today)

            self._test_missing_metrics_functionality_law_enforcement_and_prison_agency(
                system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics=date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range=monthly_report_date_range,
                agency=agency,
            )

            # Disable Funding Metrics!
            disabled_metrics = [
                schema.Datapoint(
                    metric_definition_key=law_enforcement.funding.key,
                    enabled=False,
                    source=agency,
                    is_report_datapoint=False,
                ),
                schema.Datapoint(
                    metric_definition_key=prisons.funding.key,
                    enabled=False,
                    source=agency,
                    is_report_datapoint=False,
                ),
            ]
        session.add_all(disabled_metrics)
        session.commit()

        (
            system_to_missing_monthly_metrics,
            date_range_to_system_to_missing_annual_metrics,
            monthly_report_date_range,
        ) = get_missing_metrics(agency=agency, session=session, today=self.today)

        self._test_missing_metrics_functionality_law_enforcement_and_prison_agency_disabled_metrics(
            agency=agency,
            system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
            date_range_to_system_to_missing_annual_metrics=date_range_to_system_to_missing_annual_metrics,
            monthly_report_date_range=monthly_report_date_range,
        )

    def test_get_missing_metrics_partially_filled_reports(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
            ]

            # Enable funding and expense metrics
            enabled_metrics = (
                enabled_metrics
            ) = self.get_enabled_metric_setting_datapoints(
                metric_definitions=[
                    law_enforcement.funding,
                    law_enforcement.expenses,
                ],
                agency=agency,
            )

            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )

            # Add data for expenses metric
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=annual_calendar_year_report,
                agency=agency,
                report_metric=MetricInterface(
                    key=law_enforcement.expenses.key,
                    value=12345,
                ),
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            session.add_all([agency, annual_calendar_year_report] + enabled_metrics)
            session.commit()

            (
                system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(agency=agency, session=session, today=self.today)

            # No monthly metrics are enabled, so no monthly metrics are missing
            self.assertEqual(
                len(system_to_missing_monthly_metrics.keys()),
                0,
            )

            self.assertEqual(
                set(date_range_to_system_to_missing_annual_metrics.keys()),
                {(self.january_start_date, self.january_end_date)},
            )
            self.assertEqual(
                set(
                    date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ].keys()
                ),
                {schema.System.LAW_ENFORCEMENT},
            )
            # Only funding is missing because it's the only enabled metric without data
            self.assertEqual(
                {
                    definition.key
                    for definition in date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ][schema.System.LAW_ENFORCEMENT]
                },
                {law_enforcement.funding.key},
            )

            monthly_report = self.get_monthly_report(agency=agency)
            self.assertEqual(
                monthly_report_date_range,
                (monthly_report.date_range_start, monthly_report.date_range_end),
            )

    def test_get_missing_metrics_no_missing_metrics(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
            ]

            # Enable funding metric
            enabled_metrics = self.get_enabled_metric_setting_datapoints(
                metric_definitions=[law_enforcement.funding],
                agency=agency,
            )

            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )
            # Add data for funding metric
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=annual_calendar_year_report,
                agency=agency,
                report_metric=MetricInterface(
                    key=law_enforcement.funding.key,
                    value=12345,
                ),
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            session.add_all([agency, annual_calendar_year_report] + enabled_metrics)
            session.commit()

            (
                system_to_missing_monthly_metrics,
                system_to_starting_month_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(agency=agency, session=session, today=self.today)

            # No monthly metrics are enabled
            self.assertEqual(
                len(system_to_missing_monthly_metrics),
                0,
            )

            # No missing annual metrics
            self.assertEqual(
                len(system_to_starting_month_to_missing_annual_metrics),
                0,
            )

            monthly_report = self.get_monthly_report(agency=agency)
            self.assertEqual(
                monthly_report_date_range,
                (monthly_report.date_range_start, monthly_report.date_range_end),
            )

    def test_get_missing_metrics_supervision_subsystem(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.SUPERVISION.value,
                schema.System.PAROLE.value,
                schema.System.PROBATION.value,
            ]

            # Enable funding and expense metrics, disaggregate funding by
            # parole and probation
            agency_datapoints = [
                schema.Datapoint(
                    metric_definition_key=supervision.expenses.key,
                    enabled=True,
                    source=agency,
                    is_report_datapoint=False,
                ),
                schema.Datapoint(
                    metric_definition_key="PROBATION_FUNDING",
                    enabled=True,
                    source=agency,
                    is_report_datapoint=False,
                ),
                schema.Datapoint(
                    metric_definition_key="PAROLE_FUNDING",
                    enabled=True,
                    source=agency,
                    is_report_datapoint=False,
                ),
                schema.Datapoint(
                    metric_definition_key=supervision.funding.key,
                    source=agency,
                    context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                    value=str(True),
                    is_report_datapoint=False,
                ),
                schema.Datapoint(
                    metric_definition_key="PROBATION_FUNDING",
                    source=agency,
                    context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                    value=str(True),
                    is_report_datapoint=False,
                ),
                schema.Datapoint(
                    metric_definition_key="PAROLE_FUNDING",
                    source=agency,
                    context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                    value=str(True),
                    is_report_datapoint=False,
                ),
            ]

            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )

            session.add_all([agency, annual_calendar_year_report] + agency_datapoints)
            session.commit()

            (
                system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(agency=agency, session=session, today=self.today)

            # No monthly metrics are enabled
            self.assertEqual(
                len(system_to_missing_monthly_metrics),
                0,
            )

            self.assertEqual(
                set(date_range_to_system_to_missing_annual_metrics.keys()),
                {(self.january_start_date, self.january_end_date)},
            )

            self.assertEqual(
                set(
                    date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ].keys()
                ),
                {
                    schema.System.SUPERVISION,
                    schema.System.PAROLE,
                    schema.System.PROBATION,
                },
            )

            # Missing funding metric for supervision
            self.assertEqual(
                set(
                    metric_definition.key
                    for metric_definition in date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ][
                        schema.System.SUPERVISION
                    ]
                ),
                {supervision.expenses.key},
            )

            # Missing expenses metric for parole and probation
            self.assertEqual(
                set(
                    metric_definition.key
                    for metric_definition in date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ][
                        schema.System.PAROLE
                    ]
                ),
                {METRIC_KEY_TO_METRIC["PAROLE_FUNDING"].key},
            )
            self.assertEqual(
                set(
                    metric_definition.key
                    for metric_definition in date_range_to_system_to_missing_annual_metrics[
                        (self.january_start_date, self.january_end_date)
                    ][
                        schema.System.PROBATION
                    ]
                ),
                {METRIC_KEY_TO_METRIC["PROBATION_FUNDING"].key},
            )

            monthly_report = self.get_monthly_report(agency=agency)
            self.assertEqual(
                monthly_report_date_range,
                (monthly_report.date_range_start, monthly_report.date_range_end),
            )

    def test_get_missing_metrics_superagency(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_prison_super_agency
            child_agency_A = self.test_schema_objects.test_prison_affiliate_A
            child_agency_B = self.test_schema_objects.test_prison_affiliate_B

            agency.systems = [
                schema.System.SUPERAGENCY.value,
                schema.System.PRISONS.value,
            ]

            child_agency_A.systems = [schema.System.PRISONS.value]
            child_agency_B.systems = [schema.System.PRISONS.value]

            monthly_report_superagency = self.get_monthly_report(agency=agency)
            monthly_report_child_agency_A = self.get_monthly_report(
                agency=child_agency_A
            )
            monthly_report_child_agency_B = self.get_monthly_report(
                agency=child_agency_B
            )
            annual_calendar_year_report_superagency = (
                self.get_annual_calendar_year_report(agency=agency)
            )
            annual_fiscal_year_report_superagency = self.annual_fiscal_year_report(
                agency=agency
            )
            annual_calendar_year_report_child_agency_A = (
                self.get_annual_calendar_year_report(agency=child_agency_A)
            )
            annual_calendar_year_report_child_agency_B = (
                self.get_annual_calendar_year_report(agency=child_agency_B)
            )

            annual_fiscal_year_report_child_agency_A = self.annual_fiscal_year_report(
                agency=child_agency_A
            )
            annual_fiscal_year_report_child_agency_B = self.annual_fiscal_year_report(
                agency=child_agency_B
            )

            # Enable all metrics
            enabled_metrics = list(
                itertools.chain(
                    *[
                        self.get_enabled_metric_setting_datapoints(
                            METRICS_BY_SYSTEM[schema.System.PRISONS.value],
                            agency=agency,
                        )
                        for agency in [child_agency_A, child_agency_B]
                    ]
                )
            ) + self.get_enabled_metric_setting_datapoints(
                METRICS_BY_SYSTEM[schema.System.SUPERAGENCY.value],
                agency=agency,
            )

            # Set grievances to be reported fiscally for child agencies
            custom_reported_metrics = self.get_custom_reported_metrics(
                starting_month=7,
                agency=child_agency_A,
                metric_definitions=[prisons.grievances_upheld],
            ) + self.get_custom_reported_metrics(
                starting_month=7,
                agency=child_agency_B,
                metric_definitions=[prisons.grievances_upheld],
            )

            session.add_all(
                [
                    agency,
                    child_agency_A,
                    child_agency_B,
                    monthly_report_superagency,
                    monthly_report_child_agency_A,
                    monthly_report_child_agency_B,
                    annual_calendar_year_report_superagency,
                    annual_fiscal_year_report_superagency,
                    annual_calendar_year_report_child_agency_A,
                    annual_calendar_year_report_child_agency_B,
                    annual_fiscal_year_report_child_agency_A,
                    annual_fiscal_year_report_child_agency_B,
                ]
                + enabled_metrics
                + custom_reported_metrics
            )
            session.commit()
            session.refresh(agency)
            (
                system_to_monthly_metric_to_num_child_agencies,
                date_range_to_system_to_annual_metric_to_num_child_agencies,
                monthly_report_date_range,
            ) = get_missing_metrics_for_superagencies(
                agencies=[agency, child_agency_A, child_agency_B],
                session=session,
                today=self.today,
            )

            self.assertEqual(
                set(system_to_monthly_metric_to_num_child_agencies.keys()),
                {
                    schema.System.PRISONS
                },  # There are no monthly Superagency metrics, so only prison metrics will be in this dictionary
            )

            self.assertEqual(
                set(date_range_to_system_to_annual_metric_to_num_child_agencies.keys()),
                {
                    (self.july_start_date, self.july_end_date),
                    (self.january_start_date, self.january_end_date),
                },
            )

            for metric in METRICS_BY_SYSTEM["PRISONS"]:
                if metric.key == prisons.grievances_upheld.key:
                    self.assertEqual(
                        date_range_to_system_to_annual_metric_to_num_child_agencies[
                            (self.july_start_date, self.july_end_date)
                        ][schema.System.PRISONS][metric.display_name.title()],
                        2,
                    )
                elif metric.reporting_frequency == schema.ReportingFrequency.MONTHLY:
                    self.assertEqual(
                        system_to_monthly_metric_to_num_child_agencies[
                            schema.System.PRISONS
                        ][metric.display_name.title()],
                        2,
                    )
                elif metric.reporting_frequency == schema.ReportingFrequency.ANNUAL:
                    self.assertEqual(
                        date_range_to_system_to_annual_metric_to_num_child_agencies[
                            (self.january_start_date, self.january_end_date)
                        ][schema.System.PRISONS][metric.display_name.title()],
                        2,
                    )

            for metric in METRICS_BY_SYSTEM["SUPERAGENCY"]:
                self.assertEqual(
                    date_range_to_system_to_annual_metric_to_num_child_agencies[
                        (self.january_start_date, self.january_end_date)
                    ][schema.System.SUPERAGENCY][metric.display_name.title()],
                    1,
                )

            self.assertEqual(
                monthly_report_date_range,
                (
                    monthly_report_superagency.date_range_start,
                    monthly_report_superagency.date_range_end,
                ),
            )

            # Disable Admission Metric for child_agency_A!
            session.add(
                schema.Datapoint(
                    metric_definition_key=prisons.admissions.key,
                    enabled=False,
                    source=child_agency_A,
                    is_report_datapoint=False,
                ),
            )
            session.commit()

            (
                system_to_monthly_metric_to_num_child_agencies,
                _,
                _,
            ) = get_missing_metrics_for_superagencies(
                agencies=[agency, child_agency_A, child_agency_B],
                session=session,
                today=self.today,
            )

            # Only one child agency will be missing the admissions metric
            # since it is disabled in the other one
            self.assertEqual(
                system_to_monthly_metric_to_num_child_agencies[schema.System.PRISONS][
                    prisons.admissions.display_name.title()
                ],
                1,
            )
