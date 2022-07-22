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

from recidiviz.justice_counts.bulk_upload.bulk_upload import BulkUploadInterface
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseSeverityType,
    ProsecutionAndDefenseStaffType,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.justice_counts.control_panel.generate_fixtures import (
    PROSECUTION_AGENCY_ID,
)
from recidiviz.tools.justice_counts.control_panel.load_fixtures import (
    reset_justice_counts_fixtures,
)


@pytest.mark.uses_db
class TestJusticeCountsBulkUpload(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel bulk upload functionality."""

    def setUp(self) -> None:
        self.prosecution_directory = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/prosecution",
        )
        self.prosecution_excel = os.path.join(
            os.path.dirname(__file__),
            "bulk_upload_fixtures/prosecution/prosecution_metrics.xlsx",
        )
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        super().setUp()

    def test_prosecution_new(self) -> None:
        """Bulk upload prosecution metrics into an empty database."""

        user_account = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_F

        with SessionFactory.using_database(self.database_key) as session:
            session.add(user_account)
            session.add(agency)
            session.commit()
            session.flush()

            errors = BulkUploadInterface.upload_directory(
                session=session,
                directory=self.prosecution_directory,
                agency_id=agency.id,
                system=schema.System.PROSECUTION,
                user_account=user_account,
            )
            self.assertEqual(len(errors), 0)

            reports = ReportInterface.get_reports_by_agency_id(
                session=session, agency_id=agency.id, include_datapoints=True
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

            errors = BulkUploadInterface.upload_directory(
                session=session,
                directory=self.prosecution_directory,
                agency_id=PROSECUTION_AGENCY_ID,
                system=schema.System.PROSECUTION,
                user_account=user_account,
            )
            self.assertEqual(len(errors), 0)

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=PROSECUTION_AGENCY_ID,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_prosecution(reports_by_instance=reports_by_instance)

    def test_prosecution_excel(self) -> None:
        """Bulk upload prosecution metrics from excel spreadsheet."""

        user_account = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_F

        with SessionFactory.using_database(self.database_key) as session:
            session.add(user_account)
            session.add(agency)
            session.commit()
            session.flush()

            errors = BulkUploadInterface.upload_excel(
                session=session,
                xls=pd.ExcelFile(self.prosecution_excel),
                agency_id=agency.id,
                system=schema.System.PROSECUTION,
                user_account=user_account,
            )
            self.assertEqual(len(errors), 0)

            reports = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=agency.id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self._test_prosecution(reports_by_instance=reports_by_instance)

    def _test_prosecution(self, reports_by_instance: Dict[str, schema.Report]) -> None:
        """Spot check an annual and monthly report."""
        annual_report = reports_by_instance["2021 Annual Metrics"]
        metrics = sorted(
            ReportInterface.get_metrics_by_report(report=annual_report),
            key=lambda x: x.key,
        )
        self.assertEqual(len(metrics), 3)
        self.assertEqual(metrics[0].value, 500)
        self.assertEqual(metrics[1].value, 100)
        self.assertEqual(
            metrics[1]
            .aggregated_dimensions[0]
            .dimension_to_value[ProsecutionAndDefenseStaffType.ATTORNEY],
            50,
        )
        self.assertEqual(metrics[2].value, 4)

        monthly_report = reports_by_instance["01 2021 Metrics"]
        metrics = sorted(
            ReportInterface.get_metrics_by_report(report=monthly_report),
            key=lambda x: x.key,
        )
        self.assertEqual(len(metrics), 4)
        self.assertEqual(metrics[0].value, 100)
        self.assertEqual(
            metrics[0]
            .aggregated_dimensions[0]
            .dimension_to_value[CaseSeverityType.FELONY],
            50,
        )
        self.assertEqual(metrics[2].value, 100)
        self.assertEqual(len(metrics[2].aggregated_dimensions), 3)
        self.assertEqual(
            metrics[2]
            .aggregated_dimensions[0]
            .dimension_to_value[CaseSeverityType.FELONY],
            50,
        )
        self.assertEqual(
            metrics[2]
            .aggregated_dimensions[1]
            .dimension_to_value[GenderRestricted.FEMALE],
            25,
        )
        self.assertEqual(
            metrics[2]
            .aggregated_dimensions[2]
            .dimension_to_value[RaceAndEthnicity.BLACK],
            50,
        )
