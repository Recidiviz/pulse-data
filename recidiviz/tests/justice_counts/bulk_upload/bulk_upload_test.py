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
from typing import Dict

import pandas as pd
import pytest

from recidiviz.justice_counts.bulk_upload.bulk_upload import BulkUploader
from recidiviz.justice_counts.dimensions.jails_and_prisons import PrisonPopulationType
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
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.justice_counts.control_panel.generate_fixtures import (
    LAW_ENFORCEMENT_AGENCY_ID,
    PROSECUTION_AGENCY_ID,
)
from recidiviz.tools.justice_counts.control_panel.load_fixtures import (
    reset_justice_counts_fixtures,
)


@pytest.mark.uses_db
class TestJusticeCountsBulkUpload(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel bulk upload functionality."""

    def setUp(self) -> None:
        super().setUp()

        self.uploader = BulkUploader(catch_errors=False)
        self.uploader_infer = BulkUploader(
            catch_errors=False, infer_aggregate_value=True
        )
        self.uploader_infer_catch_errors = BulkUploader(infer_aggregate_value=True)

        self.prisons_directory = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/prison",
        )
        self.prosecution_directory = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/prosecution",
        )
        self.law_enforcement_directory = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/law_enforcement",
        )
        self.law_enforcement_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/law_enforcement/law_enforcement_metrics.xlsx",
        )
        self.law_enforcement_excel_mismatched_totals = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/law_enforcement/law_enforcement_mismatched_totals.xlsx",
        )
        self.supervision_directory = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/supervision",
        )
        self.supervision_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/supervision/supervision_metrics.xlsx",
        )
        self.prosecution_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/prosecution/prosecution_metrics.xlsx",
        )
        self.invalid_directory = os.path.join(
            os.path.dirname(__file__), "bulk_upload_fixtures/invalid"
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
            filename_to_error = self.uploader_infer_catch_errors.upload_directory(
                session=session,
                directory=self.invalid_directory,
                agency_id=self.prosecution_agency_id,
                system=schema.System.PROSECUTION,
                user_account=user_account,
            )
            self.assertEqual(len(filename_to_error), 2)
            self.assertTrue(
                "No metric corresponds to the filename `gender`"
                in str(filename_to_error["gender.csv"])
            )
            self.assertTrue(
                "No fuzzy matches found with high enough score. Input=Xxx"
                in str(filename_to_error["cases_disposed_by_type.csv"])
            )

    def test_prison_new(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            self.uploader.upload_directory(
                session=session,
                directory=self.prisons_directory,
                agency_id=self.prison_agency_id,
                system=schema.System.PRISONS,
                user_account=user_account,
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prison_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}

            # Spot check a report
            monthly_report = reports_by_instance["01 2022 Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    session=session, report=monthly_report
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(len(metrics), 4)
            self.assertEqual(metrics[0].value, 294)
            self.assertEqual(
                metrics[0]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value[
                    PrisonPopulationType.SUPERVISION_VIOLATION_OR_REVOCATION
                ],
                2,
            )
            self.assertEqual(metrics[1].value, 2151.29)

    def test_prosecution_new(self) -> None:
        """Bulk upload prosecution metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            self.uploader.upload_directory(
                session=session,
                directory=self.prosecution_directory,
                agency_id=self.prosecution_agency_id,
                system=schema.System.PROSECUTION,
                user_account=user_account,
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prosecution_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self.assertEqual(
                set(reports_by_instance.keys()),
                {
                    "2021 Annual Metrics",
                    "2022 Annual Metrics",
                    "01 2021 Metrics",
                    "02 2021 Metrics",
                    "03 2022 Metrics",
                },
            )
            self._test_prosecution(reports_by_instance=reports_by_instance)

    def test_prosecution_update(self) -> None:
        """Bulk upload prosecution metrics into a database already
        populated with fixtures.
        """

        reset_justice_counts_fixtures(self.engine)

        with SessionFactory.using_database(self.database_key) as session:
            user_account = session.query(schema.UserAccount).limit(1).one()

            self.uploader.upload_directory(
                session=session,
                directory=self.prosecution_directory,
                agency_id=PROSECUTION_AGENCY_ID,
                system=schema.System.PROSECUTION,
                user_account=user_account,
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=PROSECUTION_AGENCY_ID,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_prosecution(reports_by_instance=reports_by_instance)

    def test_prosecution_excel(self) -> None:
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
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.prosecution_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_prosecution(reports_by_instance=reports_by_instance)

    def test_law_enforcement_new(self) -> None:
        """Bulk upload prosecution metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            self.uploader.upload_directory(
                session=session,
                directory=self.law_enforcement_directory,
                agency_id=self.law_enforcement_agency_id,
                system=schema.System.LAW_ENFORCEMENT,
                user_account=user_account,
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.law_enforcement_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self.assertEqual(
                set(reports_by_instance.keys()),
                {
                    "2021 Annual Metrics",
                    "2022 Annual Metrics",
                    "01 2021 Metrics",
                    "02 2021 Metrics",
                    "08 2021 Metrics",
                    "2020 Annual Metrics",
                    "07 2021 Metrics",
                    "04 2021 Metrics",
                    "05 2021 Metrics",
                    "06 2021 Metrics",
                },
            )
            self._test_law_enforcement(reports_by_instance=reports_by_instance)

    def test_law_enforcement_update(self) -> None:
        """Bulk upload law_enforcement metrics into a database already
        populated with fixtures.
        """

        reset_justice_counts_fixtures(self.engine)

        with SessionFactory.using_database(self.database_key) as session:
            user_account = session.query(schema.UserAccount).limit(1).one()

            self.uploader.upload_directory(
                session=session,
                directory=self.law_enforcement_directory,
                agency_id=LAW_ENFORCEMENT_AGENCY_ID,
                system=schema.System.LAW_ENFORCEMENT,
                user_account=user_account,
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=LAW_ENFORCEMENT_AGENCY_ID,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_law_enforcement(reports_by_instance=reports_by_instance)

    def test_law_enforcement_excel(self) -> None:
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
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.law_enforcement_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_law_enforcement(reports_by_instance=reports_by_instance)

    def test_supervision_new(self) -> None:
        """Bulk upload supervision metrics into an empty database."""

        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )

            self.uploader_infer.upload_directory(
                session=session,
                directory=self.supervision_directory,
                agency_id=self.supervision_agency_id,
                system=schema.System.SUPERVISION,
                user_account=user_account,
            )

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=self.supervision_agency_id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_supervision(reports_by_instance=reports_by_instance)

    def test_supervision_excel(self) -> None:
        """Bulk upload supervision metrics from excel spreadsheet."""

        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )

            self.uploader_infer.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.supervision_excel),
                agency_id=self.supervision_agency_id,
                system=schema.System.SUPERVISION,
                user_account=user_account,
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
                .dimension_to_value[RaceAndEthnicity.BLACK],
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
            self.assertEqual(metrics[0].key, "PAROLE_BUDGET_")
            self.assertEqual(metrics[0].value, None)
            self.assertEqual(metrics[3].key, "PROBATION_BUDGET_")
            self.assertEqual(metrics[3].value, None)
            self.assertEqual(metrics[6].key, "SUPERVISION_BUDGET_")
            self.assertEqual(metrics[6].value, 400)

            self.assertEqual(
                metrics[7].key, "SUPERVISION_TOTAL_STAFF_metric/staff/supervision/type"
            )
            self.assertEqual(metrics[7].value, 150)
            self.assertEqual(
                metrics[7]
                .aggregated_dimensions[0]  # type: ignore[index]
                .dimension_to_value[SupervisionStaffType.SUPERVISION_OFFICERS],
                100,
            )

            annual_report_2 = reports_by_instance["2022 Annual Metrics"]
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    session=session, report=annual_report_2
                ),
                key=lambda x: x.key,
            )
            self.assertEqual(metrics[0].key, "PAROLE_BUDGET_")
            self.assertEqual(metrics[0].value, 2000)
            self.assertEqual(metrics[3].key, "PROBATION_BUDGET_")
            self.assertEqual(metrics[3].value, 300)
            self.assertEqual(metrics[6].key, "SUPERVISION_BUDGET_")
            self.assertEqual(metrics[6].value, 1000)

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
            self.assertEqual(len(metrics), 4)
            self.assertEqual(metrics[0].value, 500)
            self.assertEqual(metrics[1].value, 200)
            self.assertEqual(metrics[2].value, 90)
            self.assertEqual(metrics[3].value, 200)
            self.assertEqual(len(metrics[3].aggregated_dimensions), 1)
            self.assertEqual(
                metrics[3]  # type: ignore[index]
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
            self.assertEqual(metrics[0].value, 200)
            self.assertEqual(metrics[1].value, 800)
            self.assertEqual(len(metrics[1].aggregated_dimensions), 1)
            self.assertIsNotNone(metrics[1].aggregated_dimensions[0].dimension_to_value)
            self.assertEqual(
                metrics[1]  # type: ignore[index]
                .aggregated_dimensions[0]
                .dimension_to_value,
                {
                    CallType.UNKNOWN: None,
                    CallType.EMERGENCY: 700,
                    CallType.NON_EMERGENCY: 700,
                },
            )
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
        """Check that, when the infer_aggregate_value flag is on, bulk upload will still defer
        to a aggregate total if it was exported explicitly."""
        with SessionFactory.using_database(self.database_key) as session:
            user_account = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=self.user_account_id
            )
            # This test uploads spreadsheet that will create one report with two metrics:
            # 1) an arrest metric with an aggregate value that is reported and breakdowns
            # that do not match the total and 2) a reported_crime_by_type sheet where no
            # totals are reported.
            self.uploader_infer.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.law_enforcement_excel_mismatched_totals),
                agency_id=self.law_enforcement_agency_id,
                system=schema.System.LAW_ENFORCEMENT,
                user_account=user_account,
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
            # the arrest metric should have the aggregate value that is reported, not any of the infered
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
