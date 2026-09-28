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
"""Tests various Cloud Storage CLI helper functions (mv, cp, ls)."""
import re
import unittest
from unittest.mock import MagicMock, Mock, patch

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

    # Captured verbatim from both CLIs against a real bucket on 2026-09-16. The two
    # tools word this differently, and they differ differently per verb: for `ls`
    # they share a substring, and for `cp` and `mv` they share nothing at all. Both
    # sets stay covered, because gsutil is only removed from the gcloud CLI package
    # after March 2027.
    empty_responses = [
        # gsutil
        "CommandException: One or more URLs matched no objects.",
        "CommandException: No URLs matched: gs://a-bucket/nope/*",
        # gcloud storage
        "ERROR: (gcloud.storage.ls) One or more URLs matched no objects.",
        (
            "ERROR: (gcloud.storage.cp) The following URLs matched no objects or "
            "files:\ngs://a-bucket/nope/*"
        ),
        (
            "ERROR: (gcloud.storage.mv) The following URLs matched no objects or "
            "files:\ngs://a-bucket/nope/*"
        ),
    ]

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_empty_response_handling(self, mock_run: Mock) -> None:
        """Tests that the helpers either raise a RuntimeError or return based on 'allow_empty'"""
        for resp_msg in self.empty_responses:
            mock_run.side_effect = RuntimeError(resp_msg)
            # The gcloud wording contains parentheses, which are regex metacharacters,
            # so the expected message has to be escaped or it silently fails to match.
            expected = re.escape(resp_msg)

            # Handling for copy with an empty runtime error
            with self.assertRaisesRegex(RuntimeError, expected):
                gsutil_cp("FROM", "TO", False)
            gsutil_cp("FROM", "TO", True)

            # Handling for move with an empty runtime error
            with self.assertRaisesRegex(RuntimeError, expected):
                gsutil_mv("FROM", "TO", False)
            gsutil_mv("FROM", "TO", True)

            # The empty handling for ls should return an empty list
            with self.assertRaisesRegex(RuntimeError, expected):
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

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_gsutil_ls_files_only(self, mock_run: MagicMock) -> None:
        mock_run.return_value = "gs://some-bucket/file\ngs://some-bucket/directory/"
        self.assertEqual(
            gsutil_ls("gs://some-bucket", files_only=True),
            ["gs://some-bucket/file"],
        )

    def test_gsutil_ls_files_only_and_directories_only(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "files_only and directories_only cannot both be True"
        ):
            gsutil_ls("gs://some-bucket", files_only=True, directories_only=True)

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_commands_use_gcloud_storage(self, mock_run: MagicMock) -> None:
        """Every helper shells out to `gcloud storage`, not to gsutil."""
        mock_run.return_value = ""

        gsutil_ls("gs://some-bucket/prefix/**")
        self.assertEqual(
            'gcloud storage ls "gs://some-bucket/prefix/**"', mock_run.call_args[0][0]
        )

        gsutil_cp("gs://some-bucket/a", "gs://some-bucket/b")
        mock_run.assert_called_with(
            'gcloud storage cp "gs://some-bucket/a" "gs://some-bucket/b"',
            assert_success=True,
            timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC,
        )

        gsutil_mv("gs://some-bucket/a", "gs://some-bucket/b")
        mock_run.assert_called_with(
            'gcloud storage mv "gs://some-bucket/a" "gs://some-bucket/b"',
            assert_success=True,
            timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC,
        )

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_directories_only_lists_the_prefix_not_a_wildcard(
        self, mock_run: MagicMock
    ) -> None:
        """`gcloud storage ls` has no -d flag, so the wildcard must not be sent.

        Given "gs://b/raw/2026/09/*", `gsutil ls -d` returned one line per matched
        directory, but `gcloud storage ls` descends one level into each match. Listing
        the bare prefix reproduces the old output, so that is what is sent.
        """
        mock_run.return_value = (
            "gs://some-bucket/raw/2026/09/14/\n"
            "gs://some-bucket/raw/2026/09/15/\n"
            "gs://some-bucket/raw/2026/09/16/\n"
        )

        result = gsutil_ls("gs://some-bucket/raw/2026/09/*", directories_only=True)

        mock_run.assert_called_with(
            'gcloud storage ls "gs://some-bucket/raw/2026/09/"',
            assert_success=True,
            timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC,
        )
        self.assertEqual(
            [
                "gs://some-bucket/raw/2026/09/14/",
                "gs://some-bucket/raw/2026/09/15/",
                "gs://some-bucket/raw/2026/09/16/",
            ],
            result,
        )

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_directories_only_never_returns_a_file(self, mock_run: MagicMock) -> None:
        """Regression test for the silent wrong-answer case.

        This feeds in the output shape that a naive swap produced: header lines that
        end in "/:", the files inside each matched directory, and blank separators.
        Callers such as get_last_stored_date_for_states take [-1] of this list, so
        returning a file here would report a wrong "last stored date" with no error.

        Nothing in that shape ends in "/", so the result is empty and the caller
        raises IndexError. Failing loudly is the point.
        """
        mock_run.return_value = (
            "gs://some-bucket/raw/2026/09/14/:\n"
            "gs://some-bucket/raw/2026/09/14/aaa_file.csv\n"
            "\n"
            "gs://some-bucket/raw/2026/09/16/:\n"
            "gs://some-bucket/raw/2026/09/16/ccc_file.csv\n"
        )

        result = gsutil_ls("gs://some-bucket/raw/2026/09/*", directories_only=True)

        self.assertEqual([], result)
        for path in result:
            self.assertTrue(path.endswith("/"))

    @patch("recidiviz.tools.gsutil_shell_helpers.run_command")
    def test_ls_drops_directory_header_lines(self, mock_run: MagicMock) -> None:
        """Header lines are formatting, never a result. An object cannot end in "/"."""
        mock_run.return_value = (
            "gs://some-bucket/prefix/:\n"
            "gs://some-bucket/prefix/file.csv\n"
            "\n"
            "gs://some-bucket/other/\n"
        )

        self.assertEqual(
            ["gs://some-bucket/prefix/file.csv", "gs://some-bucket/other/"],
            gsutil_ls("gs://some-bucket/*"),
        )

    def test_directories_only_rejects_double_wildcard(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Double-wildcard searches are not compatible"
        ):
            gsutil_ls("gs://some-bucket/**", directories_only=True)


if __name__ == "__main__":
    unittest.main()
