# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Helpers for direct ingest tests."""
import os

from gcsfs import GCSFileSystem
from mock import Mock

from recidiviz.tests.ingest import fixtures


def create_mock_gcsfs() -> GCSFileSystem:
    def mock_open(file_path):
        path, file_name = os.path.split(file_path)

        mock_fp = Mock()
        fixture_contents = fixtures.as_string(path, file_name)
        mock_fp.read.return_value = bytes(fixture_contents, 'utf-8')
        mock_fp_context_manager = Mock()
        mock_fp_context_manager.__enter__ = Mock(return_value=mock_fp)
        mock_fp_context_manager.__exit__ = Mock(return_value=False)
        return mock_fp_context_manager

    mock_fs = Mock()
    mock_fs.open = mock_open
    return mock_fs
