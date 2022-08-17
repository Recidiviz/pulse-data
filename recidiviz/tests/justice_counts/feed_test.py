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
"""This class implements tests for the Justice Counts FeedInterface."""

from recidiviz.justice_counts.feed import FeedInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.justice_counts.control_panel.generate_fixtures import (
    LAW_ENFORCEMENT_AGENCY_ID,
    SUPERVISION_AGENCY_ID,
)
from recidiviz.tools.justice_counts.control_panel.load_fixtures import (
    reset_justice_counts_fixtures,
)

from ...justice_counts.metricfiles.metricfile_registry import SYSTEM_TO_METRICFILES


class TestReportInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the UserAccountInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

    def test_get_law_enforcement_feed(self) -> None:
        reset_justice_counts_fixtures(self.engine)

        with SessionFactory.using_database(self.database_key) as session:
            system_to_filename_to_rows = FeedInterface.get_feed_for_agency_id(
                session, agency_id=LAW_ENFORCEMENT_AGENCY_ID
            )
            filename_to_rows = system_to_filename_to_rows["LAW_ENFORCEMENT"]
            self.assertEqual(
                set(filename_to_rows.keys()),
                {
                    mfile.canonical_filename
                    for mfile in SYSTEM_TO_METRICFILES[schema.System.LAW_ENFORCEMENT]
                },
            )
            self.assertEqual(
                [row["year"] for row in filename_to_rows["annual_budget"]],
                [2022, 2021, 2020],
            )
            self.assertEqual(
                [
                    row["force_type"]
                    for row in filename_to_rows["use_of_force_by_type"]
                    if row["year"] == 2021
                ],
                ["PHYSICAL", "RESTRAINT", "UNKNOWN", "VERBAL", "WEAPON"],
            )
            self.assertEqual(
                [
                    row["month"]
                    for row in filename_to_rows["arrests"]
                    if row["year"] == 2022
                ],
                list(reversed(range(1, 13))),
            )
            self.assertEqual(
                [
                    row["gender"]
                    for row in filename_to_rows["arrests_by_gender"]
                    if row["year"] == 2022 and row["month"] == 12
                ],
                ["FEMALE", "MALE", "NON_BINARY", "OTHER", "UNKNOWN"],
            )

    def test_get_supervision_feed(self) -> None:
        reset_justice_counts_fixtures(self.engine)

        with SessionFactory.using_database(self.database_key) as session:
            system_to_filename_to_rows = FeedInterface.get_feed_for_agency_id(
                session, agency_id=SUPERVISION_AGENCY_ID
            )

            self.assertEqual(
                set(system_to_filename_to_rows.keys()),
                {"SUPERVISION", "PROBATION", "PAROLE"},
            )

            filename_to_rows = system_to_filename_to_rows["SUPERVISION"]
            self.assertEqual(
                set(filename_to_rows.keys()),
                {
                    mfile.canonical_filename
                    for mfile in SYSTEM_TO_METRICFILES[schema.System.SUPERVISION]
                },
            )
