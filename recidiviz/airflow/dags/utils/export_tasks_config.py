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
"""Config file for export tasks in the DAG."""

# The states where the Case Triage tool has launched. Determines which state
# pipelines the Case Triage export relies on.
CASE_TRIAGE_STATES = [
    "US_ID",
]

# The exports that do not rely on pipeline output
PIPELINE_AGNOSTIC_EXPORTS = [
    "COVID_DASHBOARD",
    "INGEST_METADATA",
    "VALIDATION_METADATA",
    "JUSTICE_COUNTS",
]
