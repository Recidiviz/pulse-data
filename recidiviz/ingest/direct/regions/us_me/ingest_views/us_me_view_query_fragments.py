# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

"""Shared helper fragments for the US_ME ingest view queries."""
# TODO(#10111): Reconsider filtering test clients in ingest view
VIEW_CLIENT_FILTER_CONDITION = """(
        -- Filters out clients that are test or duplicate accounts
        NOT REGEXP_CONTAINS(First_Name, r'^\\^|(?i)(duplicate)')
        AND Last_Name NOT IN ('^', '^^')
        AND NOT (
            Middle_Name IS NOT NULL 
            AND REGEXP_CONTAINS(Middle_Name, r'(?i)(testing|duplicate)')
        )
        AND First_Name NOT IN (
            'NOT A CLIENT'
        )
    )"""
