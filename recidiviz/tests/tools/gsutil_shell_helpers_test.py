# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests various gsutil helper functions (mv, cp, ls)."""
import unittest
from unittest.mock import Mock, patch

from recidiviz.tools.gsutil_shell_helpers import (
    GSUTIL_DEFAULT_TIMEOUT_SEC,
    gcloud_storage_rm,
    gsutil_cp,
    gsutil_ls,
    gsutil_mv,
)
from recidiviz.tools.utils.script_helpers import RunCommandUnsuccessful


class TestGsutilErrorHandling(unittest.TestCase):
    """This class tests the different error handling cases that arise when using gsutil funcs.

    The error cases are:
        - RuntimeError, generally
        - RuntimeError, due to empty data that may be expected
        - RunCommandUnsuccessful, raised specifically from run_command on a fail.
    """

    empty_responses = [
        "CommandException: One or more URLs matched no objects.",
        "CommandException: No URLs matched",
    ]

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_empty_response_handling(self, mock_run: Mock) -> None:
        """Tests that gsutil funcs either raise a RuntimeError or return based on 'allow_empty'"""
        for resp_msg in self.empty_responses:
            mock_run.side_effect = RuntimeError(resp_msg)

            # Handling for copy with an empty runtime error
            with self.assertRaisesRegex(RuntimeError, resp_msg):
                gsutil_cp("FROM", "TO", False)
            gsutil_cp("FROM", "TO", True)

            # Handling for move with an empty runtime error
            with self.assertRaisesRegex(RuntimeError, resp_msg):
                gsutil_mv("FROM", "TO", False)
            gsutil_mv("FROM", "TO", True)

            # The empty handling for ls should return an empty list
            with self.assertRaisesRegex(RuntimeError, resp_msg):
                gsutil_ls("SOME_PATH", allow_empty=False)
            self.assertEqual([], gsutil_ls("SOME_PATH", allow_empty=True))

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_command_unsuccessful_handling(self, mock_run: Mock) -> None:
        mock_run.side_effect = RunCommandUnsuccessful("Hey that didn't work")
        for allow_empty in [True, False]:
            with self.assertRaises(RunCommandUnsuccessful):
                gsutil_cp("FROM", "TO", allow_empty)
            with self.assertRaises(RunCommandUnsuccessful):
                gsutil_mv("FROM", "TO", allow_empty)
            with self.assertRaises(RunCommandUnsuccessful):
                gsutil_ls("SOME_PATH", allow_empty=allow_empty)

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_generic_runtime_error_handling(self, mock_run: Mock) -> None:
        mock_run.side_effect = RuntimeError("Hey that didn't work")
        for allow_empty in [True, False]:
            with self.assertRaises(RuntimeError):
                gsutil_cp("FROM", "TO", allow_empty)
            with self.assertRaises(RuntimeError):
                gsutil_mv("FROM", "TO", allow_empty)
            with self.assertRaises(RuntimeError):
                gsutil_ls("SOME_PATH", allow_empty=allow_empty)

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_gcloud_storage_rm(self, mock_run: Mock) -> None:
        gcloud_storage_rm("gs://some-bucket", force_delete_contents=True)

        mock_run.assert_called_with(
            'gcloud storage rm "gs://some-bucket" --recursive',
            assert_success=True,
            timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC,
        )


if __name__ == "__main__":
    unittest.main()
