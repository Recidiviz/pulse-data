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
"""Reusable constants referenced in multiple spots in our codebase for US_TN."""
import datetime

# Facilities participating in the 2026 classification (RCAF/DCAF) pilot, mapped
# to the date each facility switched over to the new classification scheme.
# CAFs at these facilities on or after their switchover date are managed by the
# front-end form (loopback) data and are handled by the RCAFandDCAFAssessments
# ingest view, so the legacy CAFScoreAssessment ingest view excludes them.
# TODO(#78869): Add additional facilities as we roll out.
CLASSIFICATION_2026_PILOT_FACILITY_START_DATES: dict[str, datetime.date] = {
    "BCCX": datetime.date(2026, 3, 1),
    "TCIX": datetime.date(2026, 3, 1),
    "RMSI": datetime.date(2026, 3, 1),
    "DJRC": datetime.date(2026, 6, 30),
    "SPND": datetime.date(2026, 6, 30),
    "TTCC": datetime.date(2026, 6, 30),
}


def classification_2026_pilot_facility_start_dates_sql() -> str:
    """Returns a SQL subquery (usable as `({subquery}) alias`) that yields one
    row per classification 2026 pilot facility, with columns `facility` and
    `pilot_start_date`."""
    rows = ", ".join(
        f"STRUCT('{facility_id}' AS facility, "
        f"DATE '{start_date.isoformat()}' AS pilot_start_date)"
        for facility_id, start_date in CLASSIFICATION_2026_PILOT_FACILITY_START_DATES.items()
    )
    return f"SELECT * FROM UNNEST([{rows}])"
