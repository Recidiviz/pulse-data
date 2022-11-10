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
""" Contains Justice Counts constants """
import enum

from recidiviz.persistence.database.schema.justice_counts.schema import System

REPORTING_FREQUENCY_CONTEXT_KEY = "REPORTING_FREQUENCY"


class DatapointGetRequestEntryPoint(enum.Enum):
    REPORT_PAGE = "REPORT_PAGE"
    METRICS_TAB = "METRICS_TAB"


SUPERVISION_SYSTEMS = [
    System.POST_RELEASE,
    System.PAROLE,
    System.PROBATION,
    System.SUPERVISION,
]
