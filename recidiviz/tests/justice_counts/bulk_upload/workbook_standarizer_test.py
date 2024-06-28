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


import pytest

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.workbook_standardizer import (
    WorkbookStandardizer,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
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

        with SessionFactory.using_database(self.database_key) as session:
            session.add(prison_agency)
            session.commit()
            session.flush()
            self.prison_agency_id = prison_agency.id

    def test_validate_file_name(self) -> None:
        """Bulk upload prison metrics into an empty database."""
        with SessionFactory.using_database(self.database_key) as session:
            prison_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_agency_id
            )

            workbook_standardizer = WorkbookStandardizer(
                system=schema.System.PRISONS, agency=prison_agency, session=session
            )
            invalid_file_name = workbook_standardizer.validate_file_name(
                file_name="test_prison_csv.csv"
            )
            valid_file_name = workbook_standardizer.validate_file_name(
                file_name="funding.csv"
            )

            self.assertEqual(invalid_file_name, False)
            self.assertEqual(valid_file_name, True)
