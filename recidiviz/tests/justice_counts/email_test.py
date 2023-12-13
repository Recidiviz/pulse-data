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
from typing import Dict, List

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
from recidiviz.justice_counts.utils.email import get_missing_metrics
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

    def get_monthly_report(self, agency: schema.Agency) -> schema.Report:
        today = datetime.date.today()
        return schema.Report(
            source=agency,
            type="MONTHLY",
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            instance="Test Monthly Report",
            status=schema.ReportStatus.NOT_STARTED,
            date_range_start=datetime.date(
                month=today.month - 1, year=today.year, day=1
            ),
            date_range_end=datetime.date(
                month=today.month + 1 if today.month != 12 else 1,
                year=today.year,
                day=1,
            ),
        )

    def get_annual_calendar_year_report(self, agency: schema.Agency) -> schema.Report:
        today = datetime.date.today()
        return schema.Report(
            source=agency,
            type="ANNUAL",
            instance="Test Annual Calendar-Year Report",
            status=schema.ReportStatus.NOT_STARTED,
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            date_range_start=datetime.date(month=1, year=today.year - 1, day=1),
            date_range_end=datetime.date(month=1, year=today.year, day=1),
        )

    def annual_fiscal_year_report(self, agency: schema.Agency) -> schema.Report:
        today = datetime.date.today()
        return schema.Report(
            source=agency,
            type="ANNUAL",
            instance="Test Annual Fiscal-Year Report",
            status=schema.ReportStatus.NOT_STARTED,
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            date_range_start=datetime.date(month=7, year=today.year - 2, day=1),
            date_range_end=datetime.date(month=7, year=today.year - 1, day=1),
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
        system_to_starting_month_to_missing_annual_metrics: Dict[
            schema.System, Dict[int, List[MetricDefinition]]
        ],
        agency: schema.Agency,
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
            set(system_to_starting_month_to_missing_annual_metrics.keys()),
            {schema.System.LAW_ENFORCEMENT, schema.System.PRISONS},
        )

        for system in agency.systems:
            # Missing fiscal-year metrics and missing calendar-year metrics are represented in
            # system_to_starting_month_to_missing_annual_metrics
            self.assertEqual(
                set(
                    system_to_starting_month_to_missing_annual_metrics[
                        schema.System[system]
                    ].keys()
                ),
                {1, 7},
            )
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
                    for definition in system_to_starting_month_to_missing_annual_metrics[
                        schema.System[system]
                    ][
                        1
                    ]
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
                    for definition in system_to_starting_month_to_missing_annual_metrics[
                        schema.System[system]
                    ][
                        7
                    ]
                },
                {
                    metric_definition.key
                    for metric_definition in METRICS_BY_SYSTEM[system]
                    if metric_definition.reporting_frequency
                    == schema.ReportingFrequency.ANNUAL
                    if "EXPENSES" in metric_definition.key
                },
            )

    def _test_missing_metrics_functionality_law_enforcement_and_prison_agency_disabled_metrics(
        self,
        system_to_missing_monthly_metrics: Dict[schema.System, List[MetricDefinition]],
        system_to_starting_month_to_missing_annual_metrics: Dict[
            schema.System, Dict[int, List[MetricDefinition]]
        ],
        agency: schema.Agency,
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
                    for definition in system_to_starting_month_to_missing_annual_metrics[
                        schema.System[system]
                    ][
                        1
                    ]
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
                system_to_starting_month_to_missing_annual_metrics,
            ) = get_missing_metrics(agency=agency, session=session)

            self._test_missing_metrics_functionality_law_enforcement_and_prison_agency(
                system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
                system_to_starting_month_to_missing_annual_metrics=system_to_starting_month_to_missing_annual_metrics,
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
            system_to_starting_month_to_missing_annual_metrics,
        ) = get_missing_metrics(agency=agency, session=session)

        self._test_missing_metrics_functionality_law_enforcement_and_prison_agency_disabled_metrics(
            agency=agency,
            system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
            system_to_starting_month_to_missing_annual_metrics=system_to_starting_month_to_missing_annual_metrics,
        )

    def test_get_missing_metrics_non_existing_reports(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            agency.systems = [
                schema.System.LAW_ENFORCEMENT.value,
                schema.System.PRISONS.value,
            ]
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

            session.add_all([agency] + enabled_metrics + custom_reported_metrics)
            session.commit()
            (
                system_to_missing_monthly_metrics,
                system_to_starting_month_to_missing_annual_metrics,
            ) = get_missing_metrics(agency=agency, session=session)

            self._test_missing_metrics_functionality_law_enforcement_and_prison_agency(
                system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
                system_to_starting_month_to_missing_annual_metrics=system_to_starting_month_to_missing_annual_metrics,
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
            system_to_starting_month_to_missing_annual_metrics,
        ) = get_missing_metrics(agency=agency, session=session)

        self._test_missing_metrics_functionality_law_enforcement_and_prison_agency_disabled_metrics(
            agency=agency,
            system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
            system_to_starting_month_to_missing_annual_metrics=system_to_starting_month_to_missing_annual_metrics,
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
                metric_definitions=[law_enforcement.funding, law_enforcement.expenses],
                agency=agency,
            )

            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )

            # Add data for expenses metric
            ReportInterface.add_or_update_metric(
                session=session,
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
                system_to_starting_month_to_missing_annual_metrics,
            ) = get_missing_metrics(agency=agency, session=session)

            # No monthly metrics are enabled, so no monthly metrics are missing
            self.assertEqual(
                len(system_to_missing_monthly_metrics.keys()),
                0,
            )

            self.assertEqual(
                set(system_to_starting_month_to_missing_annual_metrics.keys()),
                {schema.System.LAW_ENFORCEMENT},
            )
            self.assertEqual(
                set(
                    system_to_starting_month_to_missing_annual_metrics[
                        schema.System.LAW_ENFORCEMENT
                    ].keys()
                ),
                {1},
            )
            # Only funding is missing because it's the only enabled metric without data
            self.assertEqual(
                {
                    definition.key
                    for definition in system_to_starting_month_to_missing_annual_metrics[
                        schema.System.LAW_ENFORCEMENT
                    ][
                        1
                    ]
                },
                {law_enforcement.funding.key},
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
            ReportInterface.add_or_update_metric(
                session=session,
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
            ) = get_missing_metrics(agency=agency, session=session)

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
            agency_datapoints = self.get_enabled_metric_setting_datapoints(
                metric_definitions=[supervision.funding, supervision.expenses],
                agency=agency,
            ) + [
                schema.Datapoint(
                    metric_definition_key=supervision.funding.key,
                    source=agency,
                    context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                    value=str(True),
                    is_report_datapoint=False,
                )
            ]

            annual_calendar_year_report = self.get_annual_calendar_year_report(
                agency=agency
            )

            session.add_all([agency, annual_calendar_year_report] + agency_datapoints)
            session.commit()

            (
                system_to_missing_monthly_metrics,
                system_to_starting_month_to_missing_annual_metrics,
            ) = get_missing_metrics(agency=agency, session=session)

            # No monthly metrics are enabled
            self.assertEqual(
                len(system_to_missing_monthly_metrics),
                0,
            )

            self.assertEqual(
                set(system_to_starting_month_to_missing_annual_metrics.keys()),
                {
                    schema.System.SUPERVISION,
                    schema.System.PAROLE,
                    schema.System.PROBATION,
                },
            )

            for system in agency.systems:
                self.assertEqual(
                    set(
                        system_to_starting_month_to_missing_annual_metrics[
                            schema.System[system]
                        ].keys()
                    ),
                    {1},
                )

            # Missing funding metric for supervision
            self.assertEqual(
                set(
                    metric_definition.key
                    for metric_definition in system_to_starting_month_to_missing_annual_metrics[
                        schema.System.SUPERVISION
                    ][
                        1
                    ]
                ),
                {supervision.expenses.key},
            )

            # Missing expenses metric for parole and probation
            self.assertEqual(
                set(
                    metric_definition.key
                    for metric_definition in system_to_starting_month_to_missing_annual_metrics[
                        schema.System.PAROLE
                    ][
                        1
                    ]
                ),
                {METRIC_KEY_TO_METRIC["PAROLE_FUNDING"].key},
            )
            self.assertEqual(
                set(
                    metric_definition.key
                    for metric_definition in system_to_starting_month_to_missing_annual_metrics[
                        schema.System.PROBATION
                    ][
                        1
                    ]
                ),
                {METRIC_KEY_TO_METRIC["PROBATION_FUNDING"].key},
            )
