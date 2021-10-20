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
"""Utility functions for constructing Spotlight views"""


def spotlight_age_buckets(age_column: str = "age") -> str:
    """query logic to bin ages into <25, 25-29, then ten-year increments up to 70+"""

    return f"""CASE WHEN {age_column} <= 24 THEN '<25'
                WHEN {age_column} <= 29 THEN '25-29'
                WHEN {age_column} <= 39 THEN '30-39'
                WHEN {age_column} <= 49 THEN '40-49'
                WHEN {age_column} <= 59 THEN '50-59'
                WHEN {age_column} <= 69 THEN '60-69'
                WHEN {age_column} >= 70 THEN '70<'
                WHEN {age_column} IS NULL THEN 'EXTERNAL_UNKNOWN'
            END AS age_bucket"""
