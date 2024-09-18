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

import os
from typing import Dict, List

import pandas as pd
import pytest

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.bulk_upload.workbook_uploader import WorkbookUploader
from recidiviz.justice_counts.dimensions.common import CaseSeverityType
from recidiviz.justice_counts.dimensions.law_enforcement import CallType, ForceType
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseDeclinedSeverityType,
    FundingType,
    ProsecutedCaseSeverityType,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    StaffType as ProsecutionStaffType,
)
from recidiviz.justice_counts.dimensions.supervision import StaffType
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics import (
    law_enforcement,
    prisons,
    prosecution,
    supervision,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.spreadsheet_helpers import (
    create_combined_excel_file,
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
        prison_child_agency_A = self.test_schema_objects.test_prison_child_agency_A
        prison_child_agency_B = self.test_schema_objects.test_prison_child_agency_B

        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    user_account,
                    prison_agency,
                    prosecution_agency,
                    supervision_agency,
                    law_enforcement_agency,
                    prison_super_agency,
                    prison_child_agency_A,
                    prison_child_agency_B,
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
            prison_child_agency_A.super_agency_id = self.prison_super_agency_id
            prison_child_agency_B.super_agency_id = self.prison_super_agency_id

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
            file_name = "test_validation.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.PROSECUTION,
                invalid_month_sheet_name="cases_disposed_by_type",
                add_invalid_sheet_name=True,
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PROSECUTION,
                agency=prosecution_agency,
                session=session,
                user_account=user_account,
            )
            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

            self.assertEqual(len(metadata.metric_key_to_errors), 2)
            invalid_file_name_error = metadata.metric_key_to_errors.get(None, []).pop()
            self.assertTrue(
                "The following sheet names do not correspond to a metric for your agency: gender."
                in invalid_file_name_error.description
            )
            cases_disposed_errors: List[
                JusticeCountsBulkUploadException
            ] = metadata.metric_key_to_errors.get(prosecution.cases_disposed.key, [])
            self.assertEqual(len(cases_disposed_errors), 2)
            cases_disposed_by_type_month_error = cases_disposed_errors[0]
            cases_disposed_by_type_sum_error = cases_disposed_errors[1]

            self.assertTrue(
                ("Row 2: The 'month' column has an invalid value: Marchuary.")
                in cases_disposed_by_type_month_error.description,
            )
            self.assertEqual(
                (
                    "The sum of all values (0) in the cases_disposed_by_type sheet for 01/01/2021-02/01/2021 does not equal the total value provided in the aggregate sheet (10)."
                ),
                cases_disposed_by_type_sum_error.description,
            )
            os.remove(file_name)

    def test_prison(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_name = "test_prison.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS, file_name=file_name
            )

            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)
            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

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
            os.remove(file_name)

    def test_prison_csv(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            csv_file_name = "test_prison_csv.csv"
            file_path = create_csv_file(
                system=schema.System.PRISONS,
                metric="admissions",
                file_name=csv_file_name,
            )

            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)

            # Convert csv file to excel
            csv_df = pd.read_csv(file_path)
            excel_file_name = "test_prison_csv.xlsx"
            excel_file_path = file_path[0 : file_path.index(".csv")] + ".xlsx"
            csv_df.to_excel(excel_file_path, sheet_name="admissions")

            with open(
                excel_file_path,
                mode="rb",
            ) as file:
                workbook_uploader.upload_workbook(file=file, file_name=excel_file_name)
            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prison_agency_id,
                include_datapoints=True,
            )

            self.assertEqual(len(reports), 6)
            self.assertEqual(reports[0].instance, "02 2023 Metrics")
            self.assertEqual(reports[0].datapoints[0].value, "30")
            self.assertEqual(reports[1].instance, "01 2023 Metrics")
            self.assertEqual(reports[1].datapoints[0].value, "30")
            self.assertEqual(reports[2].instance, "02 2022 Metrics")
            self.assertEqual(reports[2].datapoints[0].value, "20")
            self.assertEqual(reports[3].instance, "01 2022 Metrics")
            self.assertEqual(reports[3].datapoints[0].value, "20")
            self.assertEqual(reports[4].instance, "02 2021 Metrics")
            self.assertEqual(reports[4].datapoints[0].value, "10")
            self.assertEqual(reports[5].instance, "01 2021 Metrics")
            self.assertEqual(reports[5].datapoints[0].value, "10")
            os.remove(excel_file_name)

    def test_prosecution(self) -> None:
        """Bulk upload prosecution metrics from excel spreadsheet."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prosecution_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prosecution_agency_id
            )
            file_name = "test_prosecution.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.PROSECUTION, file_name=file_name
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PROSECUTION,
                agency=prosecution_agency,
                session=session,
                user_account=user_account,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prosecution_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_prosecution(reports_by_instance=reports_by_instance)
            os.remove(file_name)

    def test_law_enforcement(self) -> None:
        """Bulk upload law_enforcement metrics from excel spreadsheet."""

        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            law_enforcement_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.law_enforcement_agency_id
            )
            file_name = "test_law_enforcement.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT, file_name=file_name
            )
            metadata = BulkUploadMetadata(
                system=schema.System.LAW_ENFORCEMENT,
                agency=law_enforcement_agency,
                session=session,
                user_account=user_account,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)
            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.law_enforcement_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_law_enforcement(reports_by_instance=reports_by_instance)
            os.remove(file_name)

    def test_supervision(self) -> None:
        """Bulk upload supervision metrics from excel spreadsheet."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            supervision_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.supervision_agency_id
            )
            file_name = "test_supervision.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.SUPERVISION, file_name=file_name
            )
            metadata = BulkUploadMetadata(
                system=schema.System.SUPERVISION,
                agency=supervision_agency,
                session=session,
                user_account=user_account,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)
            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.supervision_agency_id,
                include_datapoints=True,
            )

            reports_by_instance = {report.instance: report for report in reports}
            self._test_supervision(reports_by_instance=reports_by_instance)
            os.remove(file_name)

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
            file_name = "test_infer_aggregate_value_with_total.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                sheet_names_to_skip={"reported_crime"},
                sheet_names_to_vary_values={"arrests"},
                file_name=file_name,
            )

            metadata = BulkUploadMetadata(
                system=schema.System.LAW_ENFORCEMENT,
                agency=law_enforcement_agency,
                session=session,
                user_account=user_account,
            )

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

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
            os.remove(file_name)

    def test_no_missing_breakdown_sheet_warnings_for_csv(
        self,
    ) -> None:
        """
        Checks that no Missing Breakdown Sheet warnings are triggered when the user
        submits a CSV representing aggregate metrics. Since only one sheet can be
        uploaded at a time via CSV, a missing sheet warning could be confusing.
        """
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            law_enforcement_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.law_enforcement_agency_id
            )
            csv_file_name = "test_law_enforcement_csv.csv"
            file_path = create_csv_file(
                system=schema.System.LAW_ENFORCEMENT,
                metric="arrests",
                file_name=csv_file_name,
            )

            # Convert csv file to excel
            csv_df = pd.read_csv(file_path)
            excel_file_path = file_path.replace(".csv", ".xlsx")
            excel_file_name = "test_law_enforcement_csv.xlsx"
            csv_df.to_excel(
                excel_file_path,
                sheet_name="arrests",
                columns=["year", "month", "value"],
            )

            metadata = BulkUploadMetadata(
                system=schema.System.LAW_ENFORCEMENT,
                agency=law_enforcement_agency,
                session=session,
                user_account=user_account,
            )

            with open(
                excel_file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file=file, file_name=excel_file_name)

            # No Missing Breakdown Sheet warnings.
            self.assertEqual(len(metadata.metric_key_to_errors), 0)
            os.remove(excel_file_name)

    def test_no_missing_total_sheet_warnings_for_csv(
        self,
    ) -> None:
        """
        Checks that no Missing Total Sheet warnings are triggered when the user submits
        a CSV representing breakdown metrics. Since only one sheet can be uploaded at a
        time via CSV, a missing sheet warning could be confusing.
        """
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            law_enforcement_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.law_enforcement_agency_id
            )
            csv_file_name = "test_law_enforcement_csv.csv"
            file_path = create_csv_file(
                system=schema.System.LAW_ENFORCEMENT,
                metric="arrests_by_type",
                file_name=csv_file_name,
            )

            # Convert csv file to excel
            csv_df = pd.read_csv(file_path)
            excel_file_path = file_path.replace(".csv", ".xlsx")
            excel_file_name = "test_law_enforcement_csv.xlsx"
            csv_df.to_excel(
                excel_file_path,
                sheet_name="arrests_by_type",
                columns=["year", "month", "offense_type", "value"],
            )

            metadata = BulkUploadMetadata(
                system=schema.System.LAW_ENFORCEMENT,
                agency=law_enforcement_agency,
                session=session,
                user_account=user_account,
            )

            with open(
                excel_file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file=file, file_name=excel_file_name)

            # No Missing Total Sheet warnings.
            self.assertEqual(len(metadata.metric_key_to_errors), 0)
            os.remove(excel_file_name)

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
            MetricSettingInterface.add_or_update_agency_metric_setting(
                agency_metric_updates=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            file_name = "test_missing_metrics_disabled_metrics.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                sheet_names_to_skip={
                    "calls_for_service",
                    "calls_for_service_by_type",
                },
                file_name=file_name,
            )

            metadata = BulkUploadMetadata(
                system=schema.System.LAW_ENFORCEMENT,
                agency=agency,
                session=session,
                user_account=user,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)
            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

            # There should be no errors because calls for service metric is missing
            # but it is disabled.
            self.assertEqual(len(metadata.metric_key_to_errors), 0)
            os.remove(file_name)

    def test_metrics_disaggregated_by_supervision(
        self,
    ) -> None:
        """Checks that we ingest the supervision subsystem metrics if metric is
        disaggregated by supervision."""
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_E
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            session.commit()
            session.refresh(agency)

            metric_interface = MetricInterface(
                key=supervision.funding.key,
                disaggregated_by_supervision_subsystems=True,
            )
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session, agency=agency, agency_metric_updates=metric_interface
            )
            session.commit()
            file_name = "test_metrics_disaggregated_by_supervision.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.SUPERVISION,
                metric_key_to_subsystems={
                    supervision.funding.key: [
                        schema.System.PAROLE,
                        schema.System.PROBATION,
                    ]
                },
                file_name=file_name,
            )

            metadata = BulkUploadMetadata(
                system=schema.System.SUPERVISION,
                agency=agency,
                session=session,
                user_account=user,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)
            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader.upload_workbook(
                    file=file,
                    file_name=file_name,
                )

            # There should be datapoints for parole and probation funding, but none for supervision
            self.assertTrue(
                len(
                    metadata.metric_key_to_datapoint_jsons.get(
                        "SUPERVISION_FUNDING", []
                    )
                )
                == 0
            )
            self.assertTrue(
                len(metadata.metric_key_to_datapoint_jsons.get("PAROLE_FUNDING", []))
                > 0
            )
            self.assertTrue(
                all(
                    d["agency_name"] == agency.name
                    for d in metadata.metric_key_to_datapoint_jsons["PAROLE_FUNDING"]
                )
            )
            self.assertTrue(
                len(metadata.metric_key_to_datapoint_jsons.get("PROBATION_FUNDING", []))
                > 0
            )
            self.assertTrue(
                all(
                    d["agency_name"] == agency.name
                    for d in metadata.metric_key_to_datapoint_jsons["PROBATION_FUNDING"]
                )
            )
            os.remove(file_name)

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
            file_name = "test_unexpected_column_name.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                unexpected_month_sheet_name="staff",
                unexpected_system_sheet_name="admissions",
                unexpected_disaggregation_sheet_name="population_by_type",
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

            self.assertEqual(len(metadata.metric_key_to_errors), 3)
            # 1 warning because metric was reported as monthly even though it is an annual metric.
            self.assertEqual(len(metadata.metric_key_to_errors[prisons.staff.key]), 1)
            # 1 warning because an unexpected system column was in the sheet.
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.admissions.key]), 1
            )
            # 1 warning for unexpected disaggregation.
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.daily_population.key]), 1
            )
            os.remove(file_name)

    def test_breakdown_sum_warning(
        self,
    ) -> None:
        """Checks that we warn the user when the sum of values in a breakdown sheet is uploaded
        and does not equal the sum of values in the aggregate sheet.
        """
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_name = "test_breakdown_sum_warning.xlsx"
            file_path, _ = create_excel_file(
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
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )
            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file_name=file_name, file=file)
            self.assertEqual(len(metadata.metric_key_to_errors), 8)
            # Annual metrics have a row for 2 years
            # Monthly metrics have a row for 2 years, 2 months for each year
            # admissions (monthly) x admissions_by_type = 4
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.admissions.key]), 4
            )
            # expenses (annual) x expenses_by_type = 2
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.expenses.key]), 2
            )
            # funding (annual) x funding_by_type = 2
            self.assertEqual(len(metadata.metric_key_to_errors[prisons.funding.key]), 2)
            # grievances_upheld (annual) x grievances_upheld_by_type = 2
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.grievances_upheld.key]), 2
            )
            # population (monthly), population_by_type, population_by_race, population_by_biological_sex
            # 4 x 3 = 12
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.daily_population.key]), 12
            )
            # readmission (annual) x readmissions_by_type = 2
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.readmissions.key]), 2
            )
            # releases (monthly) x releases_by_type = 4
            self.assertEqual(
                len(metadata.metric_key_to_errors[prisons.releases.key]), 4
            )
            # total_staff (annual) x staff_by_type = 2
            self.assertEqual(len(metadata.metric_key_to_errors[prisons.staff.key]), 2)
            os.remove(file_name)

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
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_skip={"funding", "funding_by_type"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )
            # Get existing reports for this agency (there should not be any at first)
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(reports, [])

            with open(
                file_path,
                mode="rb",
            ) as file:

                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file_name=file_name, file=file)

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
            with open(
                file_path,
                mode="rb",
            ) as file:

                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file_name=file_name, file=file)

            session.commit()
            # Check that reports have not been set to draft (no changes were actually made)
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            for report in reports:
                self.assertEqual(report.status.value, "PUBLISHED")

            # Case 2: Upload workbook with changes to the datapoints (admissions sheet)
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"admissions"},
                sheet_names_to_skip={"funding", "funding_by_type"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )
            with open(
                file_path,
                mode="rb",
            ) as file:

                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file_name=file_name, file=file)

            session.commit()
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
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"admissions"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )
            with open(
                file_path,
                mode="rb",
            ) as file:

                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file_name=file_name, file=file)

            session.commit()
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
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"admissions"},
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )
            with open(
                file_path,
                mode="rb",
            ) as file:

                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file_name=file_name, file=file)
            session.commit()
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            # 3 reports have been set to draft
            self.assertEqual(reports[0].status.value, "DRAFT")
            self.assertEqual(reports[3].status.value, "DRAFT")
            self.assertEqual(reports[6].status.value, "DRAFT")
            os.remove(file_name)

    def test_update_existing_report_and_creating_new_report(self) -> None:
        """
        When uploading a spreadsheet that modifies an existing report and
        creates a new report, the response should include:
            * `updated_reports`: a list of existing modified reports
            * `new_reports`: a list of newly created reports (overview dict)
        """
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            file_name = "test_update_report_status.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_skip={"funding"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )

            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(
                    file=file,
                    file_name=file_name,
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
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file=file, file_name=file_name)
            self.assertEqual(len(workbook_uploader.updated_reports), 0)

            # Upload workbook with changes to the datapoints that affect Report IDs 7 & 8
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                sheet_names_to_vary_values={"funding"},
                sheetnames_with_null_data={"use_of_force"},
                file_name=file_name,
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_agency,
                session=session,
                user_account=user_account,
            )

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_uploader = WorkbookUploader(metadata=metadata)
                workbook_uploader.upload_workbook(file=file, file_name=file_name)
            reports = ReportInterface.get_reports_by_agency_id(
                session, agency_id=self.prison_agency_id, include_datapoints=True
            )
            self.assertEqual(len(reports), 9)
            self.assertEqual(len(workbook_uploader.uploaded_reports), 9)
            self.assertEqual(len(workbook_uploader.updated_reports), 2)
            self.assertEqual(
                workbook_uploader.updated_reports, {reports[3], reports[6]}
            )
            unchanged_reports = {
                report
                for report in workbook_uploader.uploaded_reports
                if report not in workbook_uploader.updated_reports
            }
            # Since only 2 out of 9 reports were updated, we should expect the other 7 reports to be in the `unchanged_reports` set
            self.assertEqual(len(unchanged_reports), 7)
            os.remove(file_name)

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
            child_agencies = AgencyInterface.get_child_agencies_for_agency(
                session=session, agency=super_agency
            )
            metadata = BulkUploadMetadata(
                agency=super_agency,
                system=schema.System.PRISONS,
                session=session,
                user_account=user_account,
            )
            file_name = "test_update_report_status.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                child_agencies=child_agencies,
                file_name=file_name,
            )

            workbook_uploader = WorkbookUploader(metadata=metadata)
            with open(
                file_path,
                mode="rb",
            ) as file:
                # Create 18 reports, 9 reports for each child agency
                workbook_uploader.upload_workbook(file=file, file_name=file_name)

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
            os.remove(file_name)

    def test_single_page_multiple_metrics(self) -> None:
        """Bulk upload single page file that contains data for mulitple metrics."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            prosecution_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prosecution_agency_id
            )
            file_name = "test_single_page_combined.xlsx"
            file_path, _ = create_combined_excel_file(
                system=schema.System.PROSECUTION,
                file_name=file_name,
            )

            metadata = BulkUploadMetadata(
                system=schema.System.PROSECUTION,
                agency=prosecution_agency,
                session=session,
                user_account=user_account,
            )
            workbook_uploader = WorkbookUploader(metadata=metadata)
            with open(
                file_path,
                mode="rb",
            ) as file:

                workbook_uploader.upload_workbook(file=file, file_name=file_name)

            self.assertEqual(len(metadata.metric_key_to_errors), 0)

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prosecution_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}

            # Spot check monthly report
            monthly_report = reports_by_instance["01 2023 Metrics"]
            monthly_metrics = ReportInterface.get_metrics_by_report(
                session=session, report=monthly_report
            )
            self.assertEqual(len(monthly_metrics), 7)

            cases_prosecuted_metric = list(
                filter(
                    lambda m: m.key == prosecution.cases_prosecuted.key, monthly_metrics
                )
            )[0]

            self.assertEqual(cases_prosecuted_metric.value, 120)
            self.assertEqual(
                cases_prosecuted_metric.aggregated_dimensions[  # type: ignore[index]
                    0
                ].dimension_to_value[ProsecutedCaseSeverityType.FELONY],
                30,
            )

            # Spot check annual report
            annual_report = reports_by_instance["2023 Annual Metrics"]
            annual_metrics = ReportInterface.get_metrics_by_report(
                session=session, report=annual_report
            )
            self.assertEqual(len(annual_metrics), 4)

            total_staff_metric = list(
                filter(lambda m: m.key == prosecution.staff.key, annual_metrics)
            )[0]

            self.assertEqual(total_staff_metric.value, 70)
            self.assertEqual(
                total_staff_metric.aggregated_dimensions[  # type: ignore[index]
                    0
                ].dimension_to_value[ProsecutionStaffType.LEGAL_STAFF],
                10,
            )
        os.remove(file_name)
