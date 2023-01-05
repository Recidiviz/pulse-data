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
"""Implements tests for Justice Counts Control Panel bulk upload functionality."""

import itertools
import os
from typing import Dict, List

import pandas as pd
import pytest

from recidiviz.justice_counts.bulk_upload.bulk_upload import BulkUploader
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.jails_and_prisons import PrisonsOffenseType
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    ForceType,
    OffenseType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseSeverityType,
    ProsecutionAndDefenseStaffType,
)
from recidiviz.justice_counts.dimensions.supervision import SupervisionStaffType
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metrics import law_enforcement, prisons, prosecution
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


@pytest.mark.uses_db
class TestJusticeCountsBulkUpload(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel bulk upload functionality."""

    def setUp(self) -> None:
        super().setUp()

        self.uploader = BulkUploader()

        self.prisons_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/prison/prison_metrics.xlsx",
        )
        self.law_enforcement_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/law_enforcement/law_enforcement_metrics.xlsx",
        )
        self.law_enforcement_excel_mismatched_totals = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/law_enforcement/law_enforcement_mismatched_totals.xlsx",
        )
        self.law_enforcement_missing_metrics = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/law_enforcement/law_enforcement_missing_metrics.xlsx",
        )
        self.law_enforcement_empty_template = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/law_enforcement/law_enforcement_empty_template.xlsx",
        )
        self.supervision_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/supervision/supervision_metrics.xlsx",
        )
        self.prosecution_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/prosecution/prosecution_metrics.xlsx",
        )
        self.invalid_excel = os.path.join(
            os.path.dirname(__file__), "bulk_upload_fixtures/invalid/invalid.xlsx"
        )
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

        user_account = self.test_schema_objects.test_user_A
        law_enforcement_agency = self.test_schema_objects.test_agency_A
        prison_agency = self.test_schema_objects.test_agency_G
        prosecution_agency = self.test_schema_objects.test_agency_F
        supervision_agency = self.test_schema_objects.test_agency_E

        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    user_account,
                    prison_agency,
                    prosecution_agency,
                    supervision_agency,
                    law_enforcement_agency,
                ]
            )
            session.commit()
            session.flush()
            self.prison_agency_id = prison_agency.id
            self.prosecution_agency_id = prosecution_agency.id
            self.law_enforcement_agency_id = law_enforcement_agency.id
            self.supervision_agency_id = supervision_agency.id
            self.user_account_id = user_account.id

    def test_validation(self) -> None:
        """Test that errors are thrown on invalid inputs."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            _, metric_key_to_errors = self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.invalid_excel),
                agency_id=self.prosecution_agency_id,
                system=schema.System.PROSECUTION,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            self.assertEqual(len(metric_key_to_errors), 7)
            invalid_file_name_error = metric_key_to_errors.get(None, []).pop()
            self.assertTrue(
                "The following sheet names do not correspond to a metric for your agency: gender."
                in invalid_file_name_error.description
            )
            cases_disposed_errors: List[
                JusticeCountsBulkUploadException
            ] = metric_key_to_errors.get(prosecution.cases_disposed.key, [])

            self.assertEqual(len(cases_disposed_errors), 1)
            cases_disposed_by_type_error = cases_disposed_errors[0]

            self.assertTrue(
                (
                    "The valid values for this column are January, February, March, "
                    "April, May, June, July, August, September, October, "
                    "November, December."
                )
                in cases_disposed_by_type_error.description
            )

    def test_prison(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.prisons_excel),
                agency_id=self.prison_agency_id,
                system=schema.System.PRISONS,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prison_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}

            # Spot check a report
            monthly_report = reports_by_instance["01 2022 Metrics"]
            metrics = ReportInterface.get_metrics_by_report(
                session=session, report=monthly_report
            )

            admissions_metric = list(
                filter(lambda m: m.key == prisons.admissions.key, metrics)
            )[0]

            self.assertEqual(len(metrics), 3)
            self.assertEqual(admissions_metric.value, 294)
            self.assertEqual(
                admissions_metric.aggregated_dimensions[  # type: ignore[index]
                    0
                ].dimension_to_value[PrisonsOffenseType.DRUG],
                2,
            )
            self.assertEqual(metrics[1].value, 2151.29)

    def test_prosecution(self) -> None:
        """Bulk upload prosecution metrics from excel spreadsheet."""

        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.prosecution_excel),
                agency_id=self.prosecution_agency_id,
                system=schema.System.PROSECUTION,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prosecution_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_prosecution(reports_by_instance=reports_by_instance)

    def test_law_enforcement(self) -> None:
        """Bulk upload law_enforcement metrics from excel spreadsheet."""

        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.law_enforcement_excel),
                agency_id=self.law_enforcement_agency_id,
                system=schema.System.LAW_ENFORCEMENT,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.law_enforcement_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_law_enforcement(reports_by_instance=reports_by_instance)

    def test_supervision(self) -> None:
        """Bulk upload supervision metrics from excel spreadsheet."""

        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )

            self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.supervision_excel),
                agency_id=self.supervision_agency_id,
                system=schema.System.SUPERVISION,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.supervision_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_supervision(reports_by_instance=reports_by_instance)

    def _test_prosecution(self, reports_by_instance: Dict[str, schema.Report]) -> None:
        """Spot check an annual and monthly report."""
        with SessionFactory.using_database(self.database_key) as session:
            annual_report = reports_by_instance["2021 Annual Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=annual_report, session=session
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 3)
            self.assertEqual(metrics[0].value, 500)
            self.assertEqual(metrics[1].value, 100)
            self.assertIsNotNone(metrics[1].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[1]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[ProsecutionAndDefenseStaffType.ATTORNEY],
                50,
            )
            self.assertEqual(metrics[2].value, 4)

            monthly_report = reports_by_instance["01 2021 Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=monthly_report, session=session
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 4)
            self.assertEqual(metrics[0].value, 100)
            self.assertIsNotNone(metrics[0].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[0]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[CaseSeverityType.FELONY],
                50,
            )
            self.assertEqual(metrics[2].value, 100)
            self.assertEqual(len(metrics[2].aggregated_dimensions), 3)
            self.assertIsNotNone(metrics[2].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[2]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[CaseSeverityType.FELONY],
                50,
            )
            self.assertIsNotNone(metrics[2].aggregated_dimensions[1].dimension_to_value)
            self.assertEqual(
                metrics[2]  # type: ignore[index]
                .aggregated_dimensions[1]
                .dimension_to_value[GenderRestricted.FEMALE],
                25,
            )
            self.assertIsNotNone(metrics[2].aggregated_dimensions[2].dimension_to_value)
            self.assertEqual(
                metrics[2]  # type: ignore[index]
                .aggregated_dimensions[2]
                .dimension_to_value[RaceAndEthnicity.HISPANIC_BLACK],
                50,
            )

    def _test_supervision(self, reports_by_instance: Dict[str, schema.Report]) -> None:
        """Spot check an annual report."""
        with SessionFactory.using_database(self.database_key) as session:
            annual_report_1 = reports_by_instance["2021 Annual Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    session=session, report=annual_report_1
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(metrics[0].key, "SUPERVISION_BUDGET")
            self.assertEqual(metrics[0].value, 400)
            self.assertEqual(metrics[1].key, "SUPERVISION_EXPENSES")
            self.assertEqual(metrics[1].value, 300)
            self.assertEqual(metrics[2].key, "SUPERVISION_RECONVICTIONS")
            self.assertEqual(metrics[2].value, None)
            self.assertEqual(metrics[3].key, "SUPERVISION_TOTAL_STAFF")
            self.assertEqual(metrics[3].value, 150)
            self.assertEqual(
                metrics[3]
                .aggregated_dimensions[0]  # type: ignore[index]
                .dimension_to_value[SupervisionStaffType.SUPERVISION],
                100,
            )
            self.assertEqual(
                metrics[3]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[SupervisionStaffType.MANAGEMENT_AND_OPERATIONS],
                50,
            )

            annual_report_2 = reports_by_instance["2022 Annual Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    session=session, report=annual_report_2
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(metrics[0].key, "SUPERVISION_BUDGET")
            self.assertEqual(metrics[0].value, 1000)
            self.assertEqual(metrics[1].key, "SUPERVISION_EXPENSES")
            self.assertEqual(metrics[1].value, 900)
            self.assertEqual(metrics[2].key, "SUPERVISION_RECONVICTIONS")
            self.assertEqual(metrics[2].value, None)
            self.assertEqual(metrics[3].key, "SUPERVISION_TOTAL_STAFF")
            self.assertEqual(metrics[3].value, None)

    def _test_law_enforcement(
        self, reports_by_instance: Dict[str, schema.Report]
    ) -> None:
        """Spot check an annual report."""
        with SessionFactory.using_database(self.database_key) as session:
            annual_report = reports_by_instance["2021 Annual Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=annual_report, session=session
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 5)
            # Civilian Complaints Sustained metric
            self.assertEqual(metrics[0].value, 200)
            # Expenses metric
            self.assertEqual(metrics[1].value, 500)
            # Funding metric
            self.assertEqual(metrics[2].value, 500)
            # Total Staff metric
            self.assertEqual(metrics[3].value, 90)
            # Use of force  metric
            self.assertEqual(metrics[4].value, 200)
            self.assertEqual(len(metrics[4].aggregated_dimensions), 1)
            self.assertEqual(
                metrics[4]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value,
                {
                    ForceType.PHYSICAL: 5,
                    ForceType.RESTRAINT: None,
                    ForceType.VERBAL: None,
                    ForceType.WEAPON: None,
                    ForceType.UNKNOWN: None,
                },
            )
            monthly_report = reports_by_instance["01 2021 Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=monthly_report, session=session
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 3)
            # Arrests
            self.assertEqual(metrics[0].value, 200)
            # Calls for Service
            self.assertEqual(metrics[1].value, 800)
            self.assertEqual(len(metrics[1].aggregated_dimensions), 1)
            self.assertIsNotNone(metrics[1].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[1]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value,
                {
                    CallType.UNKNOWN: None,
                    CallType.OTHER: None,
                    CallType.EMERGENCY: 700,
                    CallType.NON_EMERGENCY: 700,
                },
            )
            # Reported Crime
            self.assertEqual(metrics[2].value, 800)
            self.assertEqual(len(metrics[2].aggregated_dimensions), 1)
            self.assertIsNotNone(metrics[2].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[2]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value,
                {
                    OffenseType.PERSON: 700,
                    OffenseType.DRUG: None,
                    OffenseType.OTHER: None,
                    OffenseType.PROPERTY: None,
                    OffenseType.UNKNOWN: None,
                },
            )

    def test_infer_aggregate_value_with_total(
        self,
    ) -> None:
        """Check that bulk upload defers to a aggregate total if it was exported explicitly."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            # This test uploads spreadsheet that will create one report with two metrics:
            # 1) an arrest metric with an aggregate value that is reported and breakdowns
            # that do not match the total and 2) a reported_crime_by_type sheet where no
            # totals are reported.
            self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.law_enforcement_excel_mismatched_totals),
                agency_id=self.law_enforcement_agency_id,
                system=schema.System.LAW_ENFORCEMENT,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.law_enforcement_agency_id,
                include_datapoints=True,
            )
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=reports[0], session=session
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 3)
            # the arrest metric should have the aggregate value that is reported, not any of the inferred
            # values from the breakdown
            arrest_metric = list(
                filter(lambda m: m.key == law_enforcement.total_arrests.key, metrics)
            )[0]
            self.assertEqual(arrest_metric.value, 200)
            for aggregated_dimension in arrest_metric.aggregated_dimensions:
                if aggregated_dimension.dimension_to_value is not None:
                    inferred_value = list(
                        filter(
                            lambda item: item is not None,
                            aggregated_dimension.dimension_to_value.values(),
                        )
                    )
                    self.assertNotEqual(sum(inferred_value), 200)
            # the reported_crime metric should have the aggregate value that is inferred, since no
            # aggregate value was reported explicitly.
            reported_crime_metric = list(
                filter(lambda m: m.key == law_enforcement.reported_crime.key, metrics)
            )[0]
            self.assertEqual(reported_crime_metric.value, 1000)
            for aggregated_dimension in reported_crime_metric.aggregated_dimensions:
                if aggregated_dimension.dimension_to_value is not None:
                    inferred_value = list(
                        filter(
                            lambda item: item is not None,
                            aggregated_dimension.dimension_to_value.values(),
                        )
                    )
                    self.assertEqual(sum(inferred_value), 1000)

    def test_missing_total_and_metric_validation(
        self,
    ) -> None:
        """Checks that we warn the user when a metric is missing or a total sheet is missing."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            _, metric_key_to_errors = self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.law_enforcement_missing_metrics),
                agency_id=self.law_enforcement_agency_id,
                system=schema.System.LAW_ENFORCEMENT,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )

            self.assertEqual(len(metric_key_to_errors), 3)
            arrest_errors = metric_key_to_errors[law_enforcement.total_arrests.key]
            self.assertEqual(len(arrest_errors), 1)
            arrest_error = arrest_errors[0]
            self.assertEqual(arrest_error.title, "Missing Total Value")
            self.assertEqual(arrest_error.message_type, BulkUploadMessageType.WARNING)
            self.assertTrue(
                "No total values were provided for this metric"
                in arrest_error.description,
            )

            use_of_force_errors = metric_key_to_errors[
                law_enforcement.officer_use_of_force_incidents.key
            ]
            self.assertEqual(len(use_of_force_errors), 1)
            use_of_force_error = use_of_force_errors[0]
            self.assertEqual(use_of_force_error.title, "Missing Metric")
            self.assertEqual(
                use_of_force_error.message_type, BulkUploadMessageType.ERROR
            )
            self.assertTrue(
                "No data for the Use of Force Incidents metric was provided. "
                in use_of_force_error.description,
            )
            staff_errors = metric_key_to_errors[law_enforcement.staff.key]
            self.assertEqual(len(staff_errors), 2)
            for error in staff_errors:
                if error.sheet_name == "staff_by_race":
                    self.assertEqual(error.title, "Missing Breakdown Sheet")
                    self.assertEqual(error.message_type, BulkUploadMessageType.WARNING)
                    self.assertTrue(
                        "No data for the Race / Ethnicity breakdown was provided. Please provide data in a sheet named staff_by_race"
                        in error.description,
                    )
                elif error.sheet_name == "staff_by_biological_sex":
                    self.assertEqual(error.title, "Missing Breakdown Sheet")
                    self.assertEqual(
                        error.message_type,
                        BulkUploadMessageType.WARNING,
                    )
                    self.assertTrue(
                        "No data for the Biological Sex breakdown was provided. Please provide data in a sheet named staff_by_biological_sex"
                        in error.description,
                    )
                else:
                    raise ValueError(
                        "There should only be errors for the staff_by_race and staff_by_biological_sex sheets."
                    )

    def test_missing_metrics_disabled_metrics(
        self,
    ) -> None:
        """Checks that we do not raise Missing Metric errors if
        the metric is disabled."""
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])

            # Turn off calls for service metric.
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                is_metric_enabled=False
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )

            # Get agency metric and construct metric_key_to_agency_datapoints dict.
            agency_datapoints = DatapointInterface.get_agency_datapoints(
                session=session, agency_id=agency.id
            )
            agency_datapoints_sorted_by_metric_key = sorted(
                agency_datapoints, key=lambda d: d.metric_definition_key
            )
            metric_key_to_agency_datapoints = {
                k: list(v)
                for k, v in itertools.groupby(
                    agency_datapoints_sorted_by_metric_key,
                    key=lambda d: d.metric_definition_key,
                )
            }

            # Upload empty spreadsheet.
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            _, metric_key_to_errors = self.uploader.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.law_enforcement_empty_template),
                agency_id=self.law_enforcement_agency_id,
                system=schema.System.LAW_ENFORCEMENT,
                user_account=user_account,
                metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            )
            # There should be 7 missing metric warnings, one for each enabled
            # metric that is NOT Calls For Service. Since Calls For Service
            # is turned off, there should not be a Missing Metric or Missing
            # Breakdown error.
            self.assertEqual(len(metric_key_to_errors), 7)
            for metric_key, errors in metric_key_to_errors.items():
                self.assertNotEqual(metric_key, law_enforcement.calls_for_service.key)
                self.assertEqual(len(errors), 1)
                self.assertEqual(errors[0].title, "Missing Metric")
