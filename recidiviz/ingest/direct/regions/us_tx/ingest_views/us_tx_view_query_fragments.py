# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

"""Shared helper fragments for the US_TX ingest view queries."""

PERIOD_EXCLUSIONS_FRAGMENT = """
    (
            "Death reported",
            "Deported OOC",
            "Death",
            "Discharge",
            "Erroneous Release Returned to ID",
            "Full pardon",
            "Sentence Reversed",
            "Removed from Review Process",
            "UNKNOWN OFFENDER STATUS",
            "Returned ICC offender to sending state",
            "Other Agency Detainer",
            "Supervised out of state"
        )
"""
