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
from typing import Dict, List

import pandas as pd
import pytest

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.workbook_uploader import WorkbookUploader
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.common import CaseSeverityType
from recidiviz.justice_counts.dimensions.law_enforcement import CallType, ForceType
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseDeclinedSeverityType,
    FundingType,
)
from recidiviz.justice_counts.dimensions.supervision import StaffType
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metrics import (
    law_enforcement,
    prisons,
    prosecution,
    supervision,
)
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import (
    DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.spreadsheet_helpers import (
    create_csv_file,
    create_excel_file,
)
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


@pytest.mark.uses_db
class TestJusticeCountsBulkUpload(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel bulk upload functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        user_account = self.test_schema_objects.test_user_A
        law_enforcement_agency = self.test_schema_objects.test_agency_A
        prison_agency = self.test_schema_objects.test_agency_G
        prosecution_agency = self.test_schema_objects.test_agency_F
        supervision_agency = self.test_schema_objects.test_agency_E
        prison_super_agency = self.test_schema_objects.test_prison_super_agency
        prison_affiliate_A = self.test_schema_objects.test_prison_affiliate_A
        prison_affiliate_B = self.test_schema_objects.test_prison_affiliate_B

        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    user_account,
                    prison_agency,
                    prosecution_agency,
                    supervision_agency,
                    law_enforcement_agency,
                    prison_super_agency,
                    prison_affiliate_A,
                    prison_affiliate_B,
                ]
            )
            session.commit()
            session.flush()
            self.prison_agency_id = prison_agency.id
            self.prison_super_agency_id = prison_super_agency.id
            self.prosecution_agency_id = prosecution_agency.id
            self.law_enforcement_agency_id = law_enforcement_agency.id
            self.supervision_agency_id = supervision_agency.id
            self.user_account_id = user_account.id
            prison_affiliate_A.super_agency_id = self.prison_super_agency_id
            prison_affiliate_B.super_agency_id = self.prison_super_agency_id

    def test_validation(self) -> None:
        """Test that errors are thrown on invalid inputs."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prosecution_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prosecution_agency_id
            )
            # Create an excel sheet that has an invalid month
            # in the month column of the cases_disposed metric and a
            # sheet with an invalid sheet name.
            file_path = create_excel_file(
                system=schema.System.PROSECUTION,
                invalid_month_sheet_name="cases_disposed_by_type",
                add_invalid_sheet_name=True,
                file_name="test_validation.xlsx",
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PROSECUTION,
                agency=prosecution_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            _, metric_key_to_errors = workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PROSECUTION.value],
            )

            self.assertEqual(len(metric_key_to_errors), 2)
            invalid_file_name_error = metric_key_to_errors.get(None, []).pop()
            self.assertTrue(
                "The following sheet names do not correspond to a metric for your agency: gender."
                in invalid_file_name_error.description
            )
            cases_disposed_errors: List[
                JusticeCountsBulkUploadException
            ] = metric_key_to_errors.get(prosecution.cases_disposed.key, [])
            self.assertEqual(len(cases_disposed_errors), 2)
            cases_disposed_by_type_month_error = cases_disposed_errors[0]
            cases_disposed_by_type_sum_error = cases_disposed_errors[1]

            self.assertTrue(
                (
                    "The valid values for this column are January, February, March, April, May, June, July, August, September, October, November, December."
                )
                in cases_disposed_by_type_month_error.description,
            )
            self.assertEqual(
                (
                    "The sum of all values (0.0) in the cases_disposed_by_type sheet for 01/01/2021-02/01/2021 does not equal the total value provided in the aggregate sheet (10.0)."
                ),
                cases_disposed_by_type_sum_error.description,
            )

    def test_prison(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.PRISONS, file_name="test_prison.xlsx"
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PRISONS,
                agency=prison_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
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
            self.assertEqual(admissions_metric.value, 20)
            self.assertEqual(
                admissions_metric.aggregated_dimensions[  # type: ignore[index]
                    0
                ].dimension_to_value[OffenseType.DRUG],
                0,
            )
            self.assertEqual(metrics[1].value, 20)

    def test_prison_csv(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_name = "test_prison_csv.csv"
            file_path = create_csv_file(
                system=schema.System.PRISONS, metric="admissions", file_name=file_name
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PRISONS,
                agency=prison_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )

            # Convert csv file to excel
            csv_df = pd.read_csv(file_path)
            excel_file_path = file_path[0 : file_path.index(".csv")] + ".xlsx"
            csv_df.to_excel(excel_file_path, sheet_name="admissions")

            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(excel_file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prison_agency_id,
                include_datapoints=True,
            )

            self.assertEqual(len(reports), 6)
            self.assertEqual(reports[0].instance, "02 2023 Metrics")
            self.assertEqual(reports[0].datapoints[0].value, "30.0")
            self.assertEqual(reports[1].instance, "01 2023 Metrics")
            self.assertEqual(reports[1].datapoints[0].value, "30.0")
            self.assertEqual(reports[2].instance, "02 2022 Metrics")
            self.assertEqual(reports[2].datapoints[0].value, "20.0")
            self.assertEqual(reports[3].instance, "01 2022 Metrics")
            self.assertEqual(reports[3].datapoints[0].value, "20.0")
            self.assertEqual(reports[4].instance, "02 2021 Metrics")
            self.assertEqual(reports[4].datapoints[0].value, "10.0")
            self.assertEqual(reports[5].instance, "01 2021 Metrics")
            self.assertEqual(reports[5].datapoints[0].value, "10.0")

    def test_prosecution(self) -> None:
        """Bulk upload prosecution metrics from excel spreadsheet."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prosecution_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prosecution_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.PROSECUTION, file_name="test_prosecution.xlsx"
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PROSECUTION,
                agency=prosecution_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PROSECUTION.value],
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
            law_enforcement_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.law_enforcement_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                file_name="test_law_enforcement.xlsx",
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.LAW_ENFORCEMENT,
                agency=law_enforcement_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[
                    schema.System.LAW_ENFORCEMENT.value
                ],
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
            supervision_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.supervision_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.SUPERVISION, file_name="test_supervision.xlsx"
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.SUPERVISION,
                agency=supervision_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.SUPERVISION.value],
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
            self.assertEqual(len(metrics), 4)
            self.assertEqual(metrics[0].value, 10)
            self.assertEqual(metrics[1].value, 10)
            self.assertIsNotNone(metrics[1].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[1]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[FundingType.STATE_APPROPRIATIONS],
                10,
            )
            self.assertEqual(metrics[2].value, 10)

            monthly_report = reports_by_instance["01 2021 Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=monthly_report, session=session
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 7)
            self.assertEqual(metrics[0].value, 10)
            self.assertIsNotNone(metrics[0].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[0]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[CaseSeverityType.FELONY],
                10,
            )

            self.assertEqual(metrics[3].value, 10)
            self.assertEqual(len(metrics[3].aggregated_dimensions), 3)
            self.assertIsNotNone(metrics[3].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[3]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[CaseDeclinedSeverityType.FELONY],
                10,
            )
            self.assertIsNotNone(metrics[3].aggregated_dimensions[1].dimension_to_value)
            self.assertEqual(
                metrics[3]  # type: ignore[index]
                .aggregated_dimensions[1]
                .dimension_to_value[BiologicalSex.MALE],
                10,
            )
            self.assertIsNotNone(metrics[3].aggregated_dimensions[2].dimension_to_value)
            self.assertEqual(
                metrics[3]  # type: ignore[index]
                .aggregated_dimensions[2]
                .dimension_to_value[
                    RaceAndEthnicity.HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE
                ],
                10,
            )

    def _test_supervision(self, reports_by_instance: Dict[str, schema.Report]) -> None:
        """Spot check an annual report."""
        with SessionFactory.using_database(self.database_key) as session:
            annual_report_1 = reports_by_instance["2021 Annual Metrics"]
            metrics = ReportInterface.get_metrics_by_report(
                session=session, report=annual_report_1
            )

            subsystem_metrics = list(
                filter(lambda m: "SUPERVISION" not in m.key, metrics)
            )

            for metric in subsystem_metrics:
                self.assertIsNone(metric.value)

            supervision_metrics = list(
                filter(lambda m: "SUPERVISION" in m.key, metrics)
            )
            self.assertEqual(supervision_metrics[0].key, "SUPERVISION_FUNDING")
            self.assertEqual(supervision_metrics[0].value, 10)
            self.assertEqual(supervision_metrics[1].key, "SUPERVISION_EXPENSES")
            self.assertEqual(supervision_metrics[1].value, 10)
            self.assertEqual(supervision_metrics[2].key, "SUPERVISION_TOTAL_STAFF")
            self.assertEqual(supervision_metrics[2].value, 10)
            self.assertEqual(
                supervision_metrics[2]
                .aggregated_dimensions[0]  # type: ignore[index]
                .dimension_to_value[StaffType.SUPERVISION],
                10,
            )
            self.assertEqual(
                supervision_metrics[2]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[StaffType.MANAGEMENT_AND_OPERATIONS],
                0,
            )
            self.assertEqual(supervision_metrics[3].key, "SUPERVISION_RECONVICTIONS")
            self.assertEqual(supervision_metrics[3].value, 10)

            annual_report_2 = reports_by_instance["2022 Annual Metrics"]
            metrics = ReportInterface.get_metrics_by_report(
                session=session, report=annual_report_2
            )
            subsystem_metrics = list(
                filter(lambda m: "SUPERVISION" not in m.key, metrics)
            )

            for metric in subsystem_metrics:
                self.assertIsNone(metric.value)

            supervision_metrics = list(
                filter(lambda m: "SUPERVISION" in m.key, metrics)
            )
            self.assertEqual(supervision_metrics[0].key, "SUPERVISION_FUNDING")
            self.assertEqual(supervision_metrics[0].value, 20)
            self.assertEqual(supervision_metrics[1].key, "SUPERVISION_EXPENSES")
            self.assertEqual(supervision_metrics[1].value, 20)
            self.assertEqual(supervision_metrics[2].key, "SUPERVISION_TOTAL_STAFF")
            self.assertEqual(supervision_metrics[2].value, 20)
            self.assertEqual(supervision_metrics[3].key, "SUPERVISION_RECONVICTIONS")
            self.assertEqual(supervision_metrics[3].value, 20)

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
            self.assertEqual(metrics[0].value, 10)
            # Expenses metric
            self.assertEqual(metrics[1].value, 10)
            # Funding metric
            self.assertEqual(metrics[2].value, 10)
            # Total Staff metric
            self.assertEqual(metrics[3].value, 10)
            # Use of force  metric
            self.assertEqual(metrics[4].value, 10)
            self.assertEqual(len(metrics[4].aggregated_dimensions), 1)
            self.assertEqual(
                metrics[4]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value,
                {
                    ForceType.PHYSICAL: 10,
                    ForceType.RESTRAINT: 0,
                    ForceType.FIREARM: 0,
                    ForceType.OTHER_WEAPON: 0,
                    ForceType.OTHER: 0,
                    ForceType.UNKNOWN: 0,
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
            self.assertEqual(metrics[0].value, 10)
            # Calls for Service
            self.assertEqual(metrics[1].value, 10)
            self.assertEqual(len(metrics[1].aggregated_dimensions), 1)
            self.assertIsNotNone(metrics[1].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[1]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value,
                {
                    CallType.EMERGENCY: 10,
                    CallType.NON_EMERGENCY: 0,
                    CallType.OTHER: 0,
                    CallType.UNKNOWN: 0,
                },
            )
            # Reported Crime
            self.assertEqual(metrics[2].value, 10)
            self.assertEqual(len(metrics[2].aggregated_dimensions), 1)
            self.assertIsNotNone(metrics[2].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[2]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value,
                {
                    OffenseType.PERSON: 10,
                    OffenseType.PROPERTY: 0,
                    OffenseType.DRUG: 0,
                    OffenseType.PUBLIC_ORDER: 0,
                    OffenseType.OTHER: 0,
                    OffenseType.UNKNOWN: 0,
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
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            law_enforcement_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.law_enforcement_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                sheet_names_to_skip={"reported_crime"},
                sheet_names_to_vary_values={"arrests"},
                file_name="test_infer_aggregate_value_with_total.xlsx",
            )

            workbook_uploader = WorkbookUploader(
                system=schema.System.LAW_ENFORCEMENT,
                agency=law_enforcement_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[
                    schema.System.LAW_ENFORCEMENT.value
                ],
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.law_enforcement_agency_id,
                include_datapoints=True,
            )
            monthly_report = list(
                filter(
                    lambda m: m.date_range_start.month != m.date_range_end.month,
                    reports,
                )
            )[0]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=monthly_report, session=session
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 3)
            # the arrest metric should have the aggregate value that is reported, not any of the inferred
            # values from the breakdown
            arrest_metric = list(
                filter(lambda m: m.key == law_enforcement.arrests.key, metrics)
            )[0]
            self.assertEqual(arrest_metric.value, 30)
            for aggregated_dimension in arrest_metric.aggregated_dimensions:
                if aggregated_dimension.dimension_to_value is not None:
                    inferred_value = list(
                        filter(
                            lambda item: item is not None,
                            aggregated_dimension.dimension_to_value.values(),
                        )
                    )
                    self.assertNotEqual(sum(inferred_value), 10)
            # the reported_crime metric should have the aggregate value that is inferred, since no
            # aggregate value was reported explicitly.
            reported_crime_metric = list(
                filter(lambda m: m.key == law_enforcement.reported_crime.key, metrics)
            )[0]
            self.assertEqual(reported_crime_metric.value, 30)
            for aggregated_dimension in reported_crime_metric.aggregated_dimensions:
                if aggregated_dimension.dimension_to_value is not None:
                    inferred_value = list(
                        filter(
                            lambda item: item is not None,
                            aggregated_dimension.dimension_to_value.values(),
                        )
                    )
                    self.assertEqual(sum(inferred_value), 30)

    def test_missing_total_and_metric_validation(
        self,
    ) -> None:
        """Checks that we warn the user when a metric is missing or a total sheet is missing."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            law_enforcement_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.law_enforcement_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                sheet_names_to_skip={
                    "arrests",
                    "use_of_force",
                    "use_of_force_by_type",
                    "staff_by_race",
                    "staff_by_biological_sex",
                    "staff_by_type",
                },
                file_name="test_missing_total_and_metric_validation.xlsx",
            )

            workbook_uploader = WorkbookUploader(
                system=schema.System.LAW_ENFORCEMENT,
                agency=law_enforcement_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            _, metric_key_to_errors = workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[
                    schema.System.LAW_ENFORCEMENT.value
                ],
            )

            # Case 1 (Missing entire metric)
            # Use of Force has 1 breakdown
            # We expect niether a Missing Breakdown Sheet nor Missing Total Sheet warning
            # because entire metric is missing. This will be handled on the frontend
            self.assertEqual(len(metric_key_to_errors), 2)

            # Case 2 (Missing Total Sheet)
            # Arrests has 3 breakdowns
            # We expect 1 Missing Total Sheet warning
            arrest_errors = metric_key_to_errors[law_enforcement.arrests.key]
            self.assertEqual(len(arrest_errors), 1)
            arrest_error = arrest_errors[0]
            self.assertEqual(arrest_error.title, "Missing Total Sheet")
            self.assertEqual(arrest_error.message_type, BulkUploadMessageType.WARNING)
            self.assertIn(
                "No total values sheet was provided for this metric. The total values will be assumed to be equal to the sum of the breakdown values provided in arrests_by_",
                arrest_error.description,
            )

            # Case 3 (Missing breakdown sheets)
            # Staff has 3 breakdowns
            # We expect 3 Missing Breakdown Sheet warnings
            staff_errors = metric_key_to_errors[law_enforcement.staff.key]
            self.assertEqual(len(staff_errors), 3)
            for error in staff_errors:
                if error.sheet_name == "staff_by_race":
                    self.assertEqual(error.title, "Missing Breakdown Sheet")
                    self.assertEqual(error.message_type, BulkUploadMessageType.WARNING)
                    self.assertEqual(
                        "Please provide data in a sheet named staff_by_race.",
                        error.description,
                    )
                elif error.sheet_name == "staff_by_biological_sex":
                    self.assertEqual(error.title, "Missing Breakdown Sheet")
                    self.assertEqual(
                        error.message_type,
                        BulkUploadMessageType.WARNING,
                    )
                    self.assertEqual(
                        "Please provide data in a sheet named staff_by_biological_sex.",
                        error.description,
                    )
                elif error.sheet_name == "staff_by_type":
                    self.assertEqual(error.title, "Missing Breakdown Sheet")
                    self.assertEqual(
                        error.message_type,
                        BulkUploadMessageType.WARNING,
                    )
                    self.assertEqual(
                        "Please provide data in a sheet named staff_by_type.",
                        error.description,
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
            file_path = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                sheet_names_to_skip={
                    "calls_for_service",
                    "calls_for_service_by_type",
                },
                file_name="test_missing_metrics_disabled_metrics.xlsx",
            )

            workbook_uploader = WorkbookUploader(
                system=schema.System.LAW_ENFORCEMENT,
                agency=agency,
                user_account=user,
                metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            )
            _, metric_key_to_errors = workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[
                    schema.System.LAW_ENFORCEMENT.value
                ],
            )
            # There should be no errors because calls for service metric is missing
            # but it is disabled.
            self.assertEqual(len(metric_key_to_errors), 0)

    def test_metrics_disaggregated_by_supervision(
        self,
    ) -> None:
        """Checks that we ingest the supervision subsystem metrics if metric is
        disaggregated by supervision."""
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_E
            user = self.test_schema_objects.test_user_A
            disaggregation_datapoint = schema.Datapoint(  # Funding metric will be disaggregated by supervision subsystem
                metric_definition_key=supervision.funding.key,
                source_id=self.supervision_agency_id,
                context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                dimension_identifier_to_member=None,
                value=str(True),
            )

            session.add_all([user, agency, disaggregation_datapoint])

            metric_key_to_agency_datapoints = {
                supervision.funding.key: [disaggregation_datapoint]
            }
            file_path = create_excel_file(
                system=schema.System.SUPERVISION,
                metric_key_to_subsystems={
                    supervision.funding.key: [
                        schema.System.PAROLE,
                        schema.System.PROBATION,
                    ]
                },
                file_name="test_metrics_disaggregated_by_supervision.xlsx",
            )

            workbook_uploader = WorkbookUploader(
                system=schema.System.SUPERVISION,
                agency=agency,
                user_account=user,
                metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            )
            metric_key_to_datapoint_jsons, _ = workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.SUPERVISION.value],
            )

            # There should be datapoints for parole and probation funding, but none for supervision
            self.assertTrue(
                len(metric_key_to_datapoint_jsons.get("SUPERVISION_FUNDING", [])) == 0
            )
            self.assertTrue(
                len(metric_key_to_datapoint_jsons.get("PAROLE_FUNDING", [])) > 0
            )
            self.assertTrue(
                len(metric_key_to_datapoint_jsons.get("PROBATION_FUNDING", [])) > 0
            )

    def test_unexpected_column_name(
        self,
    ) -> None:
        """Checks that we warn the user when a sheet is uploaded with an unexpected column name."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )

            # Warnings will include:
            # 'month' when frequency is annual
            # 'system' when not a Supervision system
            # disaggregation with disaggregation_column_name is None
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                unexpected_month_sheet_name="staff",
                unexpected_system_sheet_name="admissions",
                unexpected_disaggregation_sheet_name="population_by_type",
                file_name="test_unexpected_column_name.xlsx",
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PRISONS,
                agency=prison_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            _, metric_key_to_errors = workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            self.assertEqual(len(metric_key_to_errors), 3)
            # 1 warning because metric was reported as monthly even though it is an annual metric.
            self.assertEqual(len(metric_key_to_errors[prisons.staff.key]), 1)
            # 1 warning because an unexpected system column was in the sheet.
            self.assertEqual(len(metric_key_to_errors[prisons.admissions.key]), 1)
            # 1 warning for unexpected disaggregation.
            self.assertEqual(len(metric_key_to_errors[prisons.daily_population.key]), 1)

    def test_breakdown_sum_warning(
        self,
    ) -> None:
        """Checks that we warn the user when a the sum of values in a breakdown sheet is uploaded
        and does not equal the sum of values in the aggregate sheet.
        """
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={
                    "funding_by_type",
                    "expenses_by_type",
                    "staff_by_type",
                    "readmissions_by_type",
                    "admissions_by_type",
                    "population_by_type",
                    "population_by_race",
                    "population_by_biological_sex",
                    "releases_by_type",
                    "grievances_upheld_by_type",
                },
                file_name="test_breakdown_sum_warning.xlsx",
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PRISONS,
                agency=prison_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            _, metric_key_to_errors = workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            self.assertEqual(len(metric_key_to_errors), 8)
            # Annual metrics have a row for 2 years
            # Monthly metrics have a row for 2 years, 2 months for each year
            # admissions (monthly) x admissions_by_type = 4
            self.assertEqual(len(metric_key_to_errors[prisons.admissions.key]), 4)
            # expenses (annual) x expenses_by_type = 2
            self.assertEqual(len(metric_key_to_errors[prisons.expenses.key]), 2)
            # funding (annual) x funding_by_type = 2
            self.assertEqual(len(metric_key_to_errors[prisons.funding.key]), 2)
            # grievances_upheld (annual) x grievances_upheld_by_type = 2
            self.assertEqual(
                len(metric_key_to_errors[prisons.grievances_upheld.key]), 2
            )
            # population (monthly), population_by_type, population_by_race, population_by_biological_sex
            # 4 x 3 = 12
            self.assertEqual(
                len(metric_key_to_errors[prisons.daily_population.key]), 12
            )
            # readmission (annual) x readmissions_by_type = 2
            self.assertEqual(len(metric_key_to_errors[prisons.readmissions.key]), 2)
            # releases (monthly) x releases_by_type = 4
            self.assertEqual(len(metric_key_to_errors[prisons.releases.key]), 4)
            # total_staff (annual) x staff_by_type = 2
            self.assertEqual(len(metric_key_to_errors[prisons.staff.key]), 2)

    def test_update_report_status(
        self,
    ) -> None:
        """Checks that we only set reports to draft if changes were made to the report."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_name = "test_update_report_status.xlsx"
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_skip={"funding", "funding_by_type"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PRISONS,
                agency=prison_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )
            # Get existing reports for this agency (there should not be any at first)
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(reports, [])
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            # Make sure reports were created
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            # Set all report statuses to PUBLISHED
            for report in reports:
                ReportInterface.update_report_metadata(
                    report=report,
                    editor_id=user_account.id,
                    status="PUBLISHED",
                )

            # Case 1: Upload workbook with no changes to the datapoints
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            # Check that reports have not been set to draft (no changes were actually made)
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            for report in reports:
                self.assertEqual(report.status.value, "PUBLISHED")

            # Case 2: Upload workbook with changes to the datapoints (admissions sheet)
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"admissions"},
                sheet_names_to_skip={"funding", "funding_by_type"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            # 4 reports have been set to draft
            self.assertEqual(reports[4].status.value, "DRAFT")
            self.assertEqual(reports[5].status.value, "DRAFT")
            self.assertEqual(reports[7].status.value, "DRAFT")
            self.assertEqual(reports[8].status.value, "DRAFT")

            # Set all reports back to published
            for report in reports:
                ReportInterface.update_report_metadata(
                    report=report,
                    editor_id=user_account.id,
                    status="PUBLISHED",
                )

            # Case 3: Upload workbook with new/additional datapoints (funding and funding_by_type sheets)
            # In this case, the sheets did not exist at all previously
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"admissions"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            # 3 reports have been set to draft
            self.assertEqual(reports[0].status.value, "DRAFT")
            self.assertEqual(reports[3].status.value, "DRAFT")
            self.assertEqual(reports[6].status.value, "DRAFT")

            # Set all reports back to published
            for report in reports:
                ReportInterface.update_report_metadata(
                    report=report,
                    editor_id=user_account.id,
                    status="PUBLISHED",
                )

            # Case 4: Upload workbook with new/additional datapoints (use_of_force sheet)
            # In this case, the sheet previously existed but no data was provided
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"admissions"},
                file_name=file_name,
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            # 3 reports have been set to draft
            self.assertEqual(reports[0].status.value, "DRAFT")
            self.assertEqual(reports[3].status.value, "DRAFT")
            self.assertEqual(reports[6].status.value, "DRAFT")

    def test_update_existing_report_and_creating_new_report(self) -> None:
        """
        When uploading a spreadsheet that modifies an existing report and
        creates a new report, the response should include:
            * `updated_report_ids`: a list of existing modified reports' IDs
            * `new_reports`: a list of newly created reports (overview dict)
        """
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_skip={"funding"},
                sheetnames_with_null_data={"use_of_force"},
                file_name="test_update_report_status.xlsx",
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PRISONS,
                agency=prison_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
            )

            # Create 9 reports
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)

            # There should be no existing report ids and all 9 newly created
            # reports should appear in `new_report_ids`
            all_report_ids = ReportInterface.get_report_ids_by_agency_id(
                session=session, agency_id=self.prison_agency_id
            )
            new_report_ids = [
                id
                for id in all_report_ids
                if id not in workbook_uploader.existing_report_ids
            ]
            self.assertEqual(len(workbook_uploader.existing_report_ids), 0)
            self.assertEqual(len(new_report_ids), 9)

            # Upload workbook with no changes to the datapoints
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            self.assertEqual(len(workbook_uploader.updated_report_ids), 0)

            # Upload workbook with changes to the datapoints that affect Report IDs 7 & 8
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"funding"},
                sheetnames_with_null_data={"use_of_force"},
                file_name="test_update_report_status.xlsx",
            )
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            self.assertEqual(len(workbook_uploader.uploaded_report_ids), 9)
            self.assertEqual(len(workbook_uploader.updated_report_ids), 2)
            self.assertEqual(
                workbook_uploader.updated_report_ids, {reports[3].id, reports[6].id}
            )
            unchanged_report_ids = {
                id
                for id in workbook_uploader.uploaded_report_ids
                if id not in workbook_uploader.updated_report_ids
            }
            # Since only 2 out of 9 reports were updated, we should expect the other 7 reports to be in the `unchanged_report_ids` set
            self.assertEqual(len(unchanged_report_ids), 7)

    def test_ingest_super_agency(self) -> None:
        """
        When uploading a spreadsheet with an `agency` column, we should
        ingest the data for each agency.
        """
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            super_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_super_agency_id
            )
            child_agencies = AgencyInterface.get_child_agencies_by_super_agency_id(
                session=session, agency_id=self.prison_super_agency_id
            )
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                child_agencies=child_agencies,
                file_name="test_update_report_status.xlsx",
            )
            workbook_uploader = WorkbookUploader(
                system=schema.System.PRISONS,
                agency=super_agency,
                user_account=user_account,
                metric_key_to_agency_datapoints={},
                child_agency_name_to_id={
                    a.name.strip().lower(): a.id for a in child_agencies
                },
            )

            # Create 18 reports, 9 reports for each child agency
            workbook_uploader.upload_workbook(
                session=session,
                xls=pd.ExcelFile(file_path),
                metric_definitions=METRICS_BY_SYSTEM[schema.System.PRISONS.value],
            )
            super_agency_reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_super_agency_id
            )
            self.assertEqual(len(super_agency_reports), 0)
            affiliate_agency_A_reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=child_agencies[0].id
            )
            self.assertEqual(len(affiliate_agency_A_reports), 9)
            affiliate_agency_B_reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=child_agencies[1].id
            )
            self.assertEqual(len(affiliate_agency_B_reports), 9)
