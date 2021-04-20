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
# =============================================================================
"""Namespace references."""
from enum import Enum


class BigQueryViewNamespace(Enum):
    COUNTY = "county"
    JUSTICE_COUNTS = "justice_counts"
    STATE = "state"
    VALIDATION = "validation"
    CASE_TRIAGE = "case_triage"
    INGEST_METADATA = "ingest_metadata"
    DIRECT_INGEST = "direct_ingest"
    VALIDATION_METADATA = "validation_metadata"
