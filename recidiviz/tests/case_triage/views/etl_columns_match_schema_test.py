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
"""Implements tests to ensure that our ETL columms match our schemas."""
from unittest import TestCase

from recidiviz.case_triage.views.view_config import ETL_TABLES


class TestETLColumnsMatchSchema(TestCase):
    def test_etl_columns_match_schema(self) -> None:
        """Tests that the ETL columns match our schema."""
        for table, bq_view_builder in ETL_TABLES.items():
            column_names = set(col.name for col in table.__table__.columns)
            self.assertTrue(
                set(bq_view_builder.columns).issubset(column_names),
                msg=f"{bq_view_builder.view_id} has extra columns",
            )
