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
from typing import Dict

REPORTING_FREQUENCY_CONTEXT_KEY = "REPORTING_FREQUENCY"

DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS = "DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS"

AUTOMATIC_UPLOAD_ID = -1


class DatapointGetRequestEntryPoint(enum.Enum):
    REPORT_PAGE = "REPORT_PAGE"
    METRICS_TAB = "METRICS_TAB"


# BUCKET_ID_TO_AGENCY_ID will map GCS bucket id to an agency id.
# Each agency will have one bucket with various folders for each system.
# It is secure to save this dictionary in source code because each
# bucket will authenticate the user and only be accessible by the
# agency through a service account and recidiviz admin.
BUCKET_ID_TO_AGENCY_ID: Dict[str, int] = {
    "justice-counts-sftp-test": 164,  # NCDPS on staging
    "clackamas-county-jail-staging": 161,  # Clackamas County Jail on staging
    "douglas-county-district-attorney-staging": 163,  # Douglas County District Attorney's Office on staging
    "justice-counts-supervision-three": 168,  # Supervision 3 on Staging
}
