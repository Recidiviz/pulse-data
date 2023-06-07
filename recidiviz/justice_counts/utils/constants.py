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
import re

REPORTING_FREQUENCY_CONTEXT_KEY = "REPORTING_FREQUENCY"

DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS = "DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS"

AUTOMATIC_UPLOAD_ID = -1

# Maps the actual name of the child agency to
# a shorthand used in a the spreadsheet during
# Bulk Upload. We need this because some agencies
# only want to provide shorthands in their spreadsheets.
CHILD_AGENCY_NAME_TO_UPLOAD_NAME = {
    "toledo police department": "toledo",
    "newark division of police": "newark",
    "cleveland police department": "cleveland",
    "columbus police department": "columbus",
}


class DatapointGetRequestEntryPoint(enum.Enum):
    REPORT_PAGE = "REPORT_PAGE"
    METRICS_TAB = "METRICS_TAB"


# Used to infer agency_id from bucket name during Automatic Upload
AUTOMATIC_UPLOAD_BUCKET_REGEX = re.compile(
    r"(?P<project>recidiviz-(?:.*))-justice-counts-ingest-agency-"
    r"(?P<agency_id>[0-9]+)"
)
