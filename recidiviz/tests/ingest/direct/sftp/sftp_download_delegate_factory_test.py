# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Unit tests for the SFTP download delegates."""

import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)


class SftpDownloadDelegateTest(unittest.TestCase):
    """Tests for the SFTP download delegates."""

    def test_every_sftp_download_delegate_uses_absolute_paths(self) -> None:
        for state_code in StateCode:
            try:
                delegate = SftpDownloadDelegateFactory.build(
                    region_code=state_code.value
                )
            except ValueError:
                continue
            self.assertTrue(delegate.root_directory([]).startswith("/"))
