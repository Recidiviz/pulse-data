# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for build_intercom_source_tables()"""

import unittest

from recidiviz.source_tables.intercom_export_source_tables import (
    INTERCOM_EXPORT_TICKETS_TABLE_ID,
    INTERCOM_EXPORT_TRACKER_TABLE_ID,
    build_intercom_source_tables,
)


class IntercomExportsSourceTablesTests(unittest.TestCase):
    """Tests for build_intercom_source_tables()"""

    def test_build_intercom_source_tables(self) -> None:
        """Tests that build_intercom_source_tables() adds export_tracker and tickets tables
        to the intercom_export SourceTableCollection"""

        intercom_collection = build_intercom_source_tables()

        expected_table_ids = {
            INTERCOM_EXPORT_TRACKER_TABLE_ID,
            INTERCOM_EXPORT_TICKETS_TABLE_ID,
        }
        produced_table_ids = {
            table.address.table_id for table in intercom_collection.source_tables
        }

        self.assertEqual(expected_table_ids, produced_table_ids)
