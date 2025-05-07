# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

"""
Unit tests for the export_notebooks_pre_commit module.
"""
import unittest
from unittest.mock import MagicMock, Mock, patch

from recidiviz.research.export_notebooks_pre_commit import (
    export_notebook,
    get_deleted_notebooks,
    get_staged_files,
    handle_deleted_notebooks,
    handle_renamed_notebooks,
)


class TestExportNotebooksPreCommit(unittest.TestCase):
    """
    Test suite for the export_notebooks_pre_commit module.
    """

    @patch("subprocess.check_output")
    def test_get_staged_files(self, mock_check_output: Mock) -> None:
        mock_check_output.return_value = "file1.ipynb\nfile2.py\n"
        result = get_staged_files()
        self.assertEqual(result, ["file1.ipynb", "file2.py"])
        mock_check_output.assert_called_once_with("git diff ", shell=True, text=True)

    @patch("subprocess.check_output")
    def test_get_deleted_notebooks(self, mock_check_output: Mock) -> None:
        mock_check_output.return_value = "deleted_file.ipynb\n"
        result = get_deleted_notebooks()
        self.assertEqual(result, ["deleted_file.ipynb"])
        mock_check_output.assert_called_once_with(
            "git diff  --cached --name-only --diff-filter=D", shell=True, text=True
        )

    @patch("os.path.exists")
    @patch("os.remove")
    @patch("subprocess.run")
    def test_handle_deleted_notebooks(
        self,
        mock_subprocess_run: MagicMock,
        mock_remove: MagicMock,
        mock_path_exists: MagicMock,
    ) -> None:
        mock_path_exists.return_value = True
        handle_deleted_notebooks(["notebooks/test.ipynb"])
        mock_remove.assert_called_once_with("notebooks/nbautoexport-scripts/test.py")
        mock_subprocess_run.assert_called_once_with(
            ["git", "rm", "--cached", "notebooks/nbautoexport-scripts/test.py"],
            check=True,
        )

    @patch("os.path.exists")
    @patch("os.makedirs")
    @patch("os.rename")
    @patch("subprocess.run")
    def test_handle_renamed_notebooks(
        self,
        mock_subprocess_run: MagicMock,
        mock_rename: MagicMock,
        mock_makedirs: MagicMock,
        mock_path_exists: MagicMock,
    ) -> None:
        mock_path_exists.return_value = True
        handle_renamed_notebooks(
            [("notebooks/old_test.ipynb", "notebooks/new_test.ipynb")]
        )
        mock_makedirs.assert_called_once_with(
            "notebooks/nbautoexport-scripts", exist_ok=True
        )
        mock_rename.assert_called_once_with(
            "notebooks/nbautoexport-scripts/old_test.py",
            "notebooks/nbautoexport-scripts/new_test.py",
        )
        mock_subprocess_run.assert_any_call(
            ["git", "add", "notebooks/nbautoexport-scripts/new_test.py"], check=True
        )
        mock_subprocess_run.assert_any_call(
            ["git", "rm", "--cached", "notebooks/nbautoexport-scripts/old_test.py"],
            check=True,
        )

    @patch("os.path.exists")
    @patch("os.makedirs")
    @patch("subprocess.run")
    @patch("os.rename")
    def test_export_notebook(
        self,
        mock_rename: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_makedirs: MagicMock,
        mock_path_exists: MagicMock,
    ) -> None:
        mock_path_exists.return_value = True
        with patch(
            "recidiviz.research.export_notebooks_pre_commit.get_staged_files"
        ) as mock_get_staged_files, patch(
            "recidiviz.research.export_notebooks_pre_commit.get_deleted_notebooks"
        ) as mock_get_deleted_notebooks, patch(
            "recidiviz.research.export_notebooks_pre_commit.get_renamed_notebooks"
        ) as mock_get_renamed_notebooks:
            mock_get_staged_files.return_value = ["notebooks/test.ipynb"]
            mock_get_deleted_notebooks.return_value = []
            mock_get_renamed_notebooks.return_value = []

            export_notebook()

            mock_makedirs.assert_called_once_with(
                "notebooks/nbautoexport-scripts", exist_ok=True
            )
            mock_subprocess_run.assert_any_call(
                [
                    "jupyter",
                    "nbconvert",
                    "--to",
                    "script",
                    "--output-dir",
                    "notebooks/nbautoexport-scripts",
                    "notebooks/test.ipynb",
                ],
                check=True,
            )
            mock_rename.assert_called_once_with(
                "notebooks/nbautoexport-scripts/test.txt",
                "notebooks/nbautoexport-scripts/test.py",
            )
            mock_subprocess_run.assert_any_call(
                ["git", "add", "notebooks/nbautoexport-scripts/test.py"], check=True
            )


if __name__ == "__main__":
    unittest.main()
