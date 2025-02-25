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
"""Implements tests for Justice Counts Control Panel WorkbookStandardizer functionality."""
import os

import pytest

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.bulk_upload.workbook_standardizer import (
    WorkbookStandardizer,
)
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
class TestJusticeCountsWorkbookStandardizer(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel WorkbookStandardizer functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        prison_agency = self.test_schema_objects.test_agency_G
        prison_super_agency = self.test_schema_objects.test_prison_super_agency
        prison_child_agency_A = self.test_schema_objects.test_prison_child_agency_A
        prison_child_agency_B = self.test_schema_objects.test_prison_child_agency_B

        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    prison_agency,
                    prison_super_agency,
                    prison_child_agency_A,
                    prison_child_agency_B,
                ]
            )
            session.commit()
            session.flush()
            self.prison_agency_id = prison_agency.id
            self.prison_super_agency_id = prison_super_agency.id
            prison_child_agency_A.super_agency_id = self.prison_super_agency_id
            prison_child_agency_B.super_agency_id = self.prison_super_agency_id

    def test_invalid_csv_error(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS, agency=prison_agency, session=session
            )
            workbook_standardizer = WorkbookStandardizer(metadata=metadata)
            file_name = "test_prison_csv.csv"
            file_path = create_csv_file(
                system=schema.System.PRISONS,
                metric="admissions",
                file_name=file_name,
            )

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_standardizer.standardize_workbook(
                    file=file.read(), file_name=file_name
                )
            self.assertEqual(len(metadata.metric_key_to_errors), 1)
            self.assertEqual(len(metadata.metric_key_to_errors[None]), 1)
            self.assertEqual(
                metadata.metric_key_to_errors[None][0].title,
                "Invalid File Name for CSV",
            )
            os.remove("test_prison_csv.xlsx")

    def test_invalid_sheet_names(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )
            metadata = BulkUploadMetadata(
                system=schema.System.PRISONS, agency=prison_agency, session=session
            )
            workbook_standardizer = WorkbookStandardizer(metadata=metadata)
            file_name = "test_prison.xlsx"
            file_path, _ = create_excel_file(
                system=schema.System.PRISONS,
                file_name=file_name,
                add_invalid_sheet_name=True,
            )

            with open(
                file_path,
                mode="rb",
            ) as file:
                workbook_standardizer.standardize_workbook(
                    file=file.read(), file_name=file_name
                )
            self.assertEqual(len(metadata.metric_key_to_errors), 1)
            self.assertEqual(len(metadata.metric_key_to_errors[None]), 1)
            self.assertEqual(
                metadata.metric_key_to_errors[None][0].title,
                "Invalid Sheet Name",
            )

    def test_should_sheet_have_month_column(self) -> None:
        """Tests if 'month' column is required when a specific metric key is provided."""
        with SessionFactory.using_database(self.database_key) as session:
            prison_super_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_super_agency_id
            )
            superagency_metadata = BulkUploadMetadata(
                system=schema.System.SUPERAGENCY,
                agency=prison_super_agency,
                session=session,
            )
            workbook_standardizer = WorkbookStandardizer(metadata=superagency_metadata)

            _, df = create_excel_file(
                system=schema.System.SUPERAGENCY,
                file_name="superagency_funding.xlsx",
            )

            result = workbook_standardizer.should_sheet_have_month_column(
                metric_key="SUPERAGENCY_FUNDING", sheet_df=df
            )
            self.assertFalse(result)

            prisons_metadata = BulkUploadMetadata(
                system=schema.System.PRISONS,
                agency=prison_super_agency,
                session=session,
            )
            workbook_standardizer = WorkbookStandardizer(metadata=prisons_metadata)

            _, df = create_excel_file(
                system=schema.System.PRISONS,
                file_name="prisons_funding.xlsx",
            )

            result = workbook_standardizer.should_sheet_have_month_column(
                metric_key="PRISONS_FUNDING", sheet_df=df
            )
            self.assertFalse(result)

            result = workbook_standardizer.should_sheet_have_month_column(
                metric_key="PRISONS_ADMISSIONS", sheet_df=df
            )
            self.assertTrue(result)

            _, df = create_combined_excel_file(
                system=schema.System.PRISONS,
                file_name="prisons_funding_single_page.xlsx",
            )

            result = workbook_standardizer.should_sheet_have_month_column(sheet_df=df)
            self.assertTrue(result)
