# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for ingest_file_decryption."""
from unittest import TestCase

from mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_file_decryption import (
    get_decryption_password_if_encrypted,
)


class IngestFileDecryptionTest(TestCase):
    """Tests for ingest_file_decryption."""

    @patch("recidiviz.ingest.direct.ingest_file_decryption.get_secret")
    def test_get_decryption_password_if_encrypted(
        self, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = "hunter2"
        self.assertEqual(
            "hunter2",
            get_decryption_password_if_encrypted(StateCode.US_NV, "recidiviz-staging"),
        )
        mock_get_secret.assert_called_once_with(
            "us_nv_ingest_files_decryption_password", project_id="recidiviz-staging"
        )

    def test_get_decryption_password_if_encrypted_unencrypted_state(self) -> None:
        # A 7z from a non-encrypted state is not an error; it just gets no password.
        self.assertIsNone(
            get_decryption_password_if_encrypted(StateCode.US_XX, "recidiviz-staging")
        )

    @patch("recidiviz.ingest.direct.ingest_file_decryption.get_secret")
    def test_get_decryption_password_if_encrypted_missing_secret(
        self, mock_get_secret: MagicMock
    ) -> None:
        # A missing secret for an encrypted state raises RuntimeError (not ValueError) so
        # the caller retries once the secret is added rather than dropping the file.
        mock_get_secret.return_value = None
        with self.assertRaisesRegex(
            RuntimeError,
            r"^Could not find decryption password secret "
            r"\[us_nv_ingest_files_decryption_password\] for state \[US_NV\]\. .*$",
        ):
            get_decryption_password_if_encrypted(StateCode.US_NV, "recidiviz-staging")
