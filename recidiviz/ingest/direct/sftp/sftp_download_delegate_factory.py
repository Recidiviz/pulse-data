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
from recidiviz.ingest.direct.regions.us_ar.us_ar_sftp_download_delegate import (
    UsArSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_ca.us_ca_sftp_download_delegate import (
    UsCaSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_ix.us_ix_sftp_download_delegate import (
    UsIxSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_me.us_me_sftp_download_delegate import (
    UsMeSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_mi.us_mi_sftp_download_delegate import (
    UsMiSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_sftp_download_delegate import (
    UsMoSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_sftp_download_delegate import (
    UsPaSftpDownloadDelegate,
)
from recidiviz.ingest.direct.regions.us_tx.us_tx_sftp_download_delegate import (
    UsTxSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)


class SftpDownloadDelegateFactory:
    @classmethod
    def build(cls, *, region_code: str) -> BaseSftpDownloadDelegate:
        region_code = region_code.upper()
        if region_code == StateCode.US_IX.value:
            return UsIxSftpDownloadDelegate()
        if region_code == StateCode.US_PA.value:
            return UsPaSftpDownloadDelegate()
        if region_code == StateCode.US_ME.value:
            return UsMeSftpDownloadDelegate()
        if region_code == StateCode.US_MI.value:
            return UsMiSftpDownloadDelegate()
        if region_code == StateCode.US_MO.value:
            return UsMoSftpDownloadDelegate()
        if region_code == StateCode.US_AR.value:
            return UsArSftpDownloadDelegate()
        if region_code == StateCode.US_CA.value:
            return UsCaSftpDownloadDelegate()
        if region_code == StateCode.US_TX.value:
            return UsTxSftpDownloadDelegate()
        raise ValueError(f"Unexpected region code provided: {region_code}")
