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
"""Tests for GitManager."""
import os
import subprocess
import unittest
from pathlib import Path
from unittest import mock

from recidiviz.tools.utils.git_manager import (
    _GITHUB_CREDENTIAL_HELPER_CONFIG_KEY,
    _GITHUB_TOKEN_CREDENTIAL_HELPER,
    GitManager,
)


class TestGitManagerCloneRepo(unittest.TestCase):
    """Tests for GitManager.clone_repo."""

    @mock.patch.dict(os.environ, {}, clear=False)
    @mock.patch.object(GitManager, "__attrs_post_init__")
    @mock.patch("recidiviz.tools.utils.git_manager.run_command")
    def test_clone_repo_keeps_token_out_of_command(
        self, mock_run_command: mock.MagicMock, _mock_post_init: mock.MagicMock
    ) -> None:
        GitManager.clone_repo(
            github_token="placeholder",
            repo_name="Recidiviz/looker",
            repo_root=Path("/tmp/does-not-exist/looker"),
        )

        mock_run_command.assert_called_once_with(
            "git clone -c credential.helper= -c 'credential.https://github.com.helper=!f() { echo "
            'username=x-access-token; echo "password=$RECIDIVIZ_GIT_MANAGER_GITHUB_TOKEN";'
            " }; f' https://github.com/Recidiviz/looker.git /tmp/does-not-exist/looker"
        )
        self.assertEqual(
            "placeholder", os.environ["RECIDIVIZ_GIT_MANAGER_GITHUB_TOKEN"]
        )

    def _run_credential_fill(self, host: str) -> str:
        """Runs real git so the tests cover the helper's shell quoting, the
        reset of any credential helpers configured on the machine running them,
        and the host scoping of the helper."""
        result = subprocess.run(
            [
                "git",
                "-c",
                "credential.helper=",
                "-c",
                f"{_GITHUB_CREDENTIAL_HELPER_CONFIG_KEY}={_GITHUB_TOKEN_CREDENTIAL_HELPER}",
                "credential",
                "fill",
            ],
            input=f"protocol=https\nhost={host}\n\n",
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "RECIDIVIZ_GIT_MANAGER_GITHUB_TOKEN": "placeholder",
                "GIT_TERMINAL_PROMPT": "0",
            },
        )
        return result.stdout

    def test_credential_helper_answers_with_token_for_github(self) -> None:
        self.assertEqual(
            "protocol=https\n"
            "host=github.com\n"
            "username=x-access-token\n"
            "password=placeholder\n",
            self._run_credential_fill("github.com"),
        )

    def test_credential_helper_does_not_answer_for_other_hosts(self) -> None:
        self.assertNotIn("password=", self._run_credential_fill("example.com"))
