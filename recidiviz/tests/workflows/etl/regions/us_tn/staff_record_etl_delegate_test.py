#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests the ability for StaffRecordEtlDelegate to parse json rows."""
import os
from unittest import TestCase

from recidiviz.workflows.etl.regions.us_tn.staff_record_etl_delegate import (
    StaffRecordETLDelegate,
)


class StaffRecordEtlDelegateTest(TestCase):
    """
    Test class for the StaffRecordEtlDelegate
    """

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        delegate = StaffRecordETLDelegate()

        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "fixtures", "staff_record.json"
        )
        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            fixture = fp.readline()

            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "100")
            self.assertEqual(
                row,
                {
                    "id": "100",
                    "stateCode": "US_XX",
                    "name": "Joey Joe-Joe Jr. III",
                    "email": "jjjj3@xx.gov",
                    "hasCaseload": True,
                    "district": "District 1",
                },
            )

            fixture = fp.readline()

            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "101")
            self.assertEqual(
                row,
                {
                    "id": "101",
                    "stateCode": "US_XX",
                    "name": "Sally S. Slithers",
                    "email": "sal.sli@xx.gov",
                    "hasCaseload": False,
                    "district": "District 2",
                },
            )

            fixture = fp.readline()

            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "102")
            self.assertEqual(
                row,
                {
                    "id": "102",
                    "stateCode": "US_XX",
                    "name": "Foghorn Leghorn",
                    "email": None,
                    "hasCaseload": True,
                    "district": "District 3",
                },
            )
