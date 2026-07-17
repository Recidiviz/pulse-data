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
"""Tests for eOMIS credential resolution."""
import os
import unittest
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.eomis.credentials import (
    EomisCredentials,
    eomis_env_var_name,
    eomis_secret_id,
    resolve_eomis_credentials,
)

_SECRETS = {
    "eomis_us_ar_username": "secret-user",
    "eomis_us_ar_password": "secret-pass",
}


class TestResolveEomisCredentials(unittest.TestCase):
    """Tests the override -> environment -> Secret Manager resolution order."""

    def setUp(self) -> None:
        self.get_secret_patcher = patch(
            "recidiviz.eomis.credentials.get_secret", side_effect=_SECRETS.get
        )
        self.mock_get_secret = self.get_secret_patcher.start()
        # patch.dict restores the original environment on stop, so popping the
        # credential vars here cannot leak into (or from) the developer's shell.
        self.env_patcher = patch.dict("os.environ", {}, clear=False)
        self.env_patcher.start()
        os.environ.pop("EOMIS_AR_USERNAME", None)
        os.environ.pop("EOMIS_AR_PASSWORD", None)

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.env_patcher.stop()

    def test_naming_follows_per_state_conventions(self) -> None:
        self.assertEqual(
            eomis_env_var_name(StateCode.US_AR, "username"), "EOMIS_AR_USERNAME"
        )
        self.assertEqual(
            eomis_env_var_name(StateCode.US_AR, "password"), "EOMIS_AR_PASSWORD"
        )
        self.assertEqual(
            eomis_secret_id(StateCode.US_AR, "username"), "eomis_us_ar_username"
        )
        self.assertEqual(
            eomis_secret_id(StateCode.US_AR, "password"), "eomis_us_ar_password"
        )

    def test_explicit_overrides_win_over_env_and_secrets(self) -> None:
        with patch.dict(
            "os.environ",
            {"EOMIS_AR_USERNAME": "env-user", "EOMIS_AR_PASSWORD": "env-pass"},
        ):
            credentials = resolve_eomis_credentials(
                StateCode.US_AR,
                username_override="cli-user",
                password_override="cli-pass",
            )
        self.assertEqual(
            credentials,
            EomisCredentials(username="cli-user", password="cli-pass"),
        )
        self.mock_get_secret.assert_not_called()

    def test_env_vars_win_over_secrets(self) -> None:
        with patch.dict(
            "os.environ",
            {"EOMIS_AR_USERNAME": "env-user", "EOMIS_AR_PASSWORD": "env-pass"},
        ):
            credentials = resolve_eomis_credentials(
                StateCode.US_AR, username_override=None, password_override=None
            )
        self.assertEqual(
            credentials,
            EomisCredentials(username="env-user", password="env-pass"),
        )
        self.mock_get_secret.assert_not_called()

    def test_secret_manager_is_the_final_fallback(self) -> None:
        credentials = resolve_eomis_credentials(
            StateCode.US_AR, username_override=None, password_override=None
        )
        self.assertEqual(
            credentials,
            EomisCredentials(username="secret-user", password="secret-pass"),
        )

    def test_each_field_resolves_independently(self) -> None:
        with patch.dict("os.environ", {"EOMIS_AR_USERNAME": "env-user"}):
            credentials = resolve_eomis_credentials(
                StateCode.US_AR, username_override=None, password_override=None
            )
        self.assertEqual(
            credentials,
            EomisCredentials(username="env-user", password="secret-pass"),
        )

    def test_unresolvable_field_raises_without_echoing_values(self) -> None:
        with patch(
            "recidiviz.eomis.credentials.get_secret", return_value=None
        ), patch.dict("os.environ", {"EOMIS_AR_USERNAME": "env-user"}):
            with self.assertRaisesRegex(
                ValueError,
                r"^Unable to resolve the eOMIS password for \[US_AR\]: set "
                r"\[EOMIS_AR_PASSWORD\] or configure the \[eomis_us_ar_password\] "
                r"secret\.$",
            ):
                resolve_eomis_credentials(
                    StateCode.US_AR, username_override=None, password_override=None
                )

    def test_repr_never_contains_credential_values(self) -> None:
        credentials = EomisCredentials(username="user-value", password="pass-value")
        self.assertNotIn("user-value", repr(credentials))
        self.assertNotIn("pass-value", repr(credentials))
        self.assertNotIn("user-value", str(credentials))
        self.assertNotIn("pass-value", str(credentials))


if __name__ == "__main__":
    unittest.main()
