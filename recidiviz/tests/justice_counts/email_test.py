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
from typing import Any, Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.justice_counts.metric_setting import MetricSettingInterface
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
from recidiviz.justice_counts.utils.constants import UploadMethod
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

    def set_is_metric_enabled(
        self,
        session: Any,
        is_metric_enabled: bool,
        metric_definitions: List[MetricDefinition],
        agency: schema.Agency,
    ) -> None:
        for metric_definition in metric_definitions:
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=metric_definition.key, is_metric_enabled=is_metric_enabled
                ),
            )
        session.commit()

    def set_disaggregated_by_supervision_subsystems(
        self,
        session: Any,
        disaggregated_by_supervision_subsystems: bool,
        metric_definitions: List[MetricDefinition],
        agency: schema.Agency,
    ) -> None:
        for metric_definition in metric_definitions:
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=metric_definition.key,
                    disaggregated_by_supervision_subsystems=disaggregated_by_supervision_subsystems,
                ),
            )
        session.commit()

    def set_custom_reported_metric_settings(
        self,
        session: Any,
        agency: schema.Agency,
        starting_month: int,
        metric_definitions: List[MetricDefinition],
    ) -> None:
        for metric_definition in metric_definitions:
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=metric_definition.key,
                    custom_reporting_frequency=CustomReportingFrequency(
                        starting_month=starting_month,
                        frequency=schema.ReportingFrequency.ANNUAL,
                    ),
                ),
            )
        session.commit()

    def _test_missing_metrics_functionality_law_enforcement_and_prison_agency(
        self,
        system_to_missing_monthly_metrics: Dict[schema.System, List[MetricDefinition]],
        date_range_to_system_to_missing_annual_metrics: Dict[
            Tuple[datetime.date, datetime.date],
            Dict[schema.System, List[MetricDefinition]],
        ],
        agency: schema.Agency,
        monthly_report_date_range: Optional[Tuple[datetime.date, datetime.date]] = None,
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
        monthly_report_date_range: Optional[Tuple[datetime.date, datetime.date]] = None,
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

    @freeze_time(
        datetime.date.today().replace(month=datetime.date.today().month, day=15)
    )
    def test_get_missing_metrics_empty_reports(self) -> None:
        frozen_today = datetime.date.today()
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
                schema.System.PRISONS.value,
            ]
            session.add(agency)
            session.commit()
            session.refresh(agency)

            monthly_report = self.get_monthly_report(agency=agency)
            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )
            annual_fiscal_year_report = self.annual_fiscal_year_report(agency=agency)

            # Enable all metrics
            # Here.
            self.set_is_metric_enabled(
                session=session,
                is_metric_enabled=True,
                metric_definitions=METRICS_BY_SYSTEM[
                    schema.System.LAW_ENFORCEMENT.value
                ]
                + METRICS_BY_SYSTEM[schema.System.PRISONS.value],
                agency=agency,
            )

            # Set expenses to be reported fiscally
            self.set_custom_reported_metric_settings(
                session=session,
                starting_month=7,
                agency=agency,
                metric_definitions=[prisons.expenses, law_enforcement.expenses],
            )

            session.add_all(
                [
                    monthly_report,
                    annual_calendar_year_report,
                    annual_fiscal_year_report,
                ]
            )
            session.commit()

            (
                system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(
                agency=agency,
                session=session,
                today=frozen_today,
                days_after_time_period_to_send_email=15,
            )

            self._test_missing_metrics_functionality_law_enforcement_and_prison_agency(
                system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics=date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range=monthly_report_date_range,
                agency=agency,
            )

            # Disable Funding Metrics!
            self.set_is_metric_enabled(
                session=session,
                is_metric_enabled=False,
                metric_definitions=[law_enforcement.funding, prisons.funding],
                agency=agency,
            )

            (
                system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(
                agency=agency,
                session=session,
                today=frozen_today,
                days_after_time_period_to_send_email=15,
            )

            self._test_missing_metrics_functionality_law_enforcement_and_prison_agency_disabled_metrics(
                agency=agency,
                system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics=date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range=monthly_report_date_range,
            )

    @freeze_time(
        datetime.date.today().replace(month=datetime.date.today().month, day=15)
    )
    def test_get_missing_metrics_partially_filled_reports(self) -> None:
        frozen_today = datetime.date.today()
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
            ]
            session.add(agency)
            session.commit()
            session.refresh(agency)

            # Enable funding and expense metrics
            self.set_is_metric_enabled(
                session=session,
                is_metric_enabled=True,
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

            session.add_all([agency, annual_calendar_year_report])
            session.commit()

            (
                system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(
                agency=agency,
                session=session,
                today=frozen_today,
                days_after_time_period_to_send_email=15,
            )

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

            self.assertEqual(
                monthly_report_date_range,
                None,  # monthly_report_date_range is None because no monthly report was surfaced
            )

    @freeze_time(
        datetime.date.today().replace(month=datetime.date.today().month, day=15)
    )
    def test_get_missing_metrics_no_missing_metrics(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            frozen_today = datetime.date.today()
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
            ]
            session.add(agency)
            session.commit()
            session.refresh(agency)

            # Enable funding metric
            self.set_is_metric_enabled(
                session=session,
                is_metric_enabled=True,
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

            session.add_all([agency, annual_calendar_year_report])
            session.commit()

            (
                system_to_missing_monthly_metrics,
                system_to_starting_month_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(
                agency=agency,
                session=session,
                today=frozen_today,
                days_after_time_period_to_send_email=15,
            )

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

            self.assertEqual(
                monthly_report_date_range,
                None,  # monthly_report_date_range is None because no metrics were enabled
            )

    @freeze_time(
        datetime.date.today().replace(month=datetime.date.today().month, day=15)
    )
    def test_get_missing_metrics_supervision_subsystem(self) -> None:
        frozen_today = datetime.date.today()
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.SUPERVISION.value,
                schema.System.PAROLE.value,
                schema.System.PROBATION.value,
            ]
            session.add(agency)
            session.commit()
            session.refresh(agency)

            # Enable funding and expense metrics, disaggregate funding by
            # parole and probation
            self.set_is_metric_enabled(
                session=session,
                is_metric_enabled=True,
                metric_definitions=[supervision.expenses],
                agency=agency,
            )
            self.set_disaggregated_by_supervision_subsystems(
                session=session,
                disaggregated_by_supervision_subsystems=True,
                metric_definitions=[supervision.funding],
                agency=agency,
            )

            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )

            session.add_all([agency, annual_calendar_year_report])
            session.commit()

            (
                system_to_missing_monthly_metrics,
                date_range_to_system_to_missing_annual_metrics,
                monthly_report_date_range,
            ) = get_missing_metrics(
                agency=agency,
                session=session,
                today=frozen_today,
                days_after_time_period_to_send_email=15,
            )

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

            self.assertEqual(
                monthly_report_date_range,
                None,  # monthly_report_date_range is None because no monthly metrics were surfaced
            )

    @freeze_time(
        datetime.date.today().replace(month=datetime.date.today().month, day=15)
    )
    def test_get_missing_metrics_superagency(self) -> None:
        frozen_today = datetime.date.today()
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_prison_super_agency
            child_agency_A = self.test_schema_objects.test_prison_child_agency_A
            child_agency_B = self.test_schema_objects.test_prison_child_agency_B
            agency.systems = [
                schema.System.SUPERAGENCY.value,
                schema.System.PRISONS.value,
            ]
            child_agency_A.systems = [schema.System.PRISONS.value]
            child_agency_B.systems = [schema.System.PRISONS.value]
            session.add_all([agency, child_agency_A, child_agency_B])
            session.commit()
            for elem in [agency, child_agency_A, child_agency_B]:
                session.refresh(elem)

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
            for child in [child_agency_A, child_agency_B]:
                self.set_is_metric_enabled(
                    is_metric_enabled=True,
                    session=session,
                    metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
                    agency=child,
                )
            self.set_is_metric_enabled(
                is_metric_enabled=True,
                session=session,
                metric_definitions=METRICS_BY_SYSTEM[schema.System.SUPERAGENCY.value],
                agency=agency,
            )

            # Set grievances to be reported fiscally for child agencies
            for child in [child_agency_A, child_agency_B]:
                self.set_custom_reported_metric_settings(
                    session=session,
                    starting_month=7,
                    agency=child,
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
            )
            session.commit()
            session.refresh(agency)
            session.refresh(child_agency_A)
            session.refresh(child_agency_B)
            (
                system_to_monthly_metric_to_num_child_agencies,
                date_range_to_system_to_annual_metric_to_num_child_agencies,
                monthly_report_date_range,
            ) = get_missing_metrics_for_superagencies(
                agencies=[agency, child_agency_A, child_agency_B],
                session=session,
                today=frozen_today,
                days_after_time_period_to_send_email=15,
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
            self.set_is_metric_enabled(
                session=session,
                is_metric_enabled=False,
                metric_definitions=[prisons.admissions],
                agency=child_agency_A,
            )

            (
                system_to_monthly_metric_to_num_child_agencies,
                _,
                _,
            ) = get_missing_metrics_for_superagencies(
                agencies=[agency, child_agency_A, child_agency_B],
                session=session,
                today=frozen_today,
                days_after_time_period_to_send_email=15,
            )

            # Only one child agency will be missing the admissions metric
            # since it is disabled in the other one
            self.assertEqual(
                system_to_monthly_metric_to_num_child_agencies[schema.System.PRISONS][
                    prisons.admissions.display_name.title()
                ],
                1,
            )

    def test_no_missing_metrics_when_offset_is_for_different_date(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
                schema.System.PRISONS.value,
            ]
            session.add(agency)
            session.commit()
            session.refresh(agency)

            monthly_report = self.get_monthly_report(agency=agency)
            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )
            annual_fiscal_year_report = self.annual_fiscal_year_report(agency=agency)

            # Enable all metrics
            self.set_is_metric_enabled(
                session=session,
                is_metric_enabled=True,
                metric_definitions=METRICS_BY_SYSTEM[
                    schema.System.LAW_ENFORCEMENT.value
                ]
                + METRICS_BY_SYSTEM[schema.System.PRISONS.value],
                agency=agency,
            )

            # Set expenses to be reported fiscally
            self.set_custom_reported_metric_settings(
                session=session,
                starting_month=7,
                agency=agency,
                metric_definitions=[prisons.expenses, law_enforcement.expenses],
            )
            # custom_reported_metrics = self.get_custom_reported_metrics(
            #     starting_month=7,
            #     agency=agency,
            #     metric_definitions=[prisons.expenses, law_enforcement.expenses],
            # )
            session.add_all(
                [
                    agency,
                    monthly_report,
                    annual_calendar_year_report,
                    annual_fiscal_year_report,
                ]
            )
            session.commit()

            with freeze_time(
                datetime.date.today().replace(month=datetime.date.today().month, day=12)
            ):
                frozen_today = datetime.date.today()
                (
                    system_to_missing_monthly_metrics,
                    date_range_to_system_to_missing_annual_metrics,
                    monthly_report_date_range,
                ) = get_missing_metrics(
                    agency=agency,
                    session=session,
                    today=frozen_today,
                    days_after_time_period_to_send_email=15,
                )

                # No monthly metrics are surfaced as missing because the date does not match the offset. The default
                # offset is 15, and the date that this test is frozen at is the 12th of the current month.
                self.assertEqual(
                    len(system_to_missing_monthly_metrics),
                    0,
                )

                # No annual metrics are surfaced as missing because the date does not match the offset
                self.assertEqual(
                    len(date_range_to_system_to_missing_annual_metrics),
                    0,
                )

                monthly_report = self.get_monthly_report(agency=agency)
                self.assertEqual(
                    monthly_report_date_range,
                    None,  # monthly_report_date_range is None because no monthly report was surfaced
                )

            with freeze_time(
                datetime.date.today().replace(month=datetime.date.today().month, day=15)
            ):
                frozen_today = datetime.date.today()
                (
                    system_to_missing_monthly_metrics,
                    date_range_to_system_to_missing_annual_metrics,
                    monthly_report_date_range,
                ) = get_missing_metrics(
                    agency=agency,
                    session=session,
                    today=frozen_today,
                    days_after_time_period_to_send_email=15,
                )

                # All monthly metrics are surfaced as missing because the date matches the offset. The default
                # offset is 15.
                self.assertEqual(
                    len(system_to_missing_monthly_metrics),
                    2,
                )

                # All annual metrics are surfaced as missing because the date matches the offset.
                self.assertEqual(
                    len(date_range_to_system_to_missing_annual_metrics),
                    2,
                )

                monthly_report = self.get_monthly_report(agency=agency)
                self.assertEqual(
                    monthly_report_date_range,
                    (monthly_report.date_range_start, monthly_report.date_range_end),
                )

                self._test_missing_metrics_functionality_law_enforcement_and_prison_agency(
                    system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
                    date_range_to_system_to_missing_annual_metrics=date_range_to_system_to_missing_annual_metrics,
                    monthly_report_date_range=monthly_report_date_range,
                    agency=agency,
                )
