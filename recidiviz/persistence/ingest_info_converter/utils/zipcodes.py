# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
# ============================================================================
"""Implements zipcode lookups."""
import os
import sqlite3
from typing import Optional


# TODO(#4015): Build mechanism for refreshing this file
class ZipcodeDatabaseManager:
    """A class to manage the Zipcode sqlite database and interactions with it."""

    @staticmethod
    def _fetch_field_for_zipcode(field: str, zipcode: str) -> Optional[str]:
        conn = sqlite3.connect(
            os.path.join(os.path.dirname(__file__), "zip_code_simple_db.sqlite")
        )
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {field} FROM simple_zipcode WHERE zipcode=:zipcode",
            {"zipcode": zipcode},
        )
        matching_row = cursor.fetchone()
        if not matching_row:
            return None
        return matching_row[0]

    @staticmethod
    def county_for_zipcode(zipcode: str) -> Optional[str]:
        return ZipcodeDatabaseManager._fetch_field_for_zipcode("county", zipcode)

    @staticmethod
    def state_for_zipcode(zipcode: str) -> Optional[str]:
        return ZipcodeDatabaseManager._fetch_field_for_zipcode("state", zipcode)
