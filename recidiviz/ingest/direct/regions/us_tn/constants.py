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

# Facilities participating in the 2026 classification (RCAF/DCAF) pilot. CAFs at
# these facilities are managed by the front-end form (loopback) data and are
# handled by the RCAFandDCAFAssessments ingest view, so the legacy
# CAFScoreAssessment ingest view excludes them.
# TODO(#78869): Add additional facilities as we roll out.
CLASSIFICATION_2026_PILOT_FACILITIES = [
    "BCCX",
    "TCIX",
    "RMSI",
    "DJRC",
    "SPND",
    "TTCC",
]
