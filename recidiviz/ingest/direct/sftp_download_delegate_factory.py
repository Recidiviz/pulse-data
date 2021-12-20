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
"""Contains factory class for creating SftpDownloadDelegate objects"""

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.base_sftp_download_delegate import BaseSftpDownloadDelegate
from recidiviz.ingest.direct.regions.us_id.us_id_sftp_download_delegate import (
    UsIdSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_sftp_download_delegate import (
    UsPaSftpDownloadDelegate,
)


class SftpDownloadDelegateFactory:
    @classmethod
    def build(cls, *, region_code: str) -> BaseSftpDownloadDelegate:
        region_code = region_code.upper()
        if region_code == StateCode.US_ID.value:
            return UsIdSftpDownloadDelegate()
        if region_code == StateCode.US_PA.value:
            return UsPaSftpDownloadDelegate()
        raise ValueError(f"Unexpected region code provided: {region_code}")
