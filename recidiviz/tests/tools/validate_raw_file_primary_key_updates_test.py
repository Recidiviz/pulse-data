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

"""Tests for validate_raw_file_primary_key_updates script."""
import subprocess
import unittest
from typing import Any, Callable
from unittest.mock import Mock, patch

from recidiviz.tools.validate_raw_file_primary_key_updates import (
    validate_raw_file_primary_key_updates,
)


class TestValidateRawFilePrimaryKeyUpdates(unittest.TestCase):
    """Tests for raw file primary key validation."""

    def setUp(self) -> None:
        self.sample_yaml_with_keys = """
file_tag: test_file
data_classification: source
primary_key_cols:
  - key1
  - key2
columns:
  - name: key1
  - name: key2
        """.strip()

        self.sample_yaml_different_keys = """
file_tag: test_file
data_classification: source
primary_key_cols:
  - key1
  - key3
columns:
  - name: key1
  - name: key3
        """.strip()

        self.sample_yaml_no_keys = """
file_tag: test_file
data_classification: source
primary_key_cols: []
columns:
  - name: col1
        """.strip()

    def _create_mock_git_side_effect(self, git_responses: dict) -> Callable:
        """Helper to create a mock side effect function for subprocess.run calls."""

        def mock_run_side_effect(cmd: list[str], **_kwargs: Any) -> Mock:
            mock_result = Mock()
            mock_result.returncode = 0
            cmd_tuple = tuple(cmd)
            if cmd_tuple in git_responses:
                mock_result.stdout.decode.return_value = git_responses[cmd_tuple]
            elif cmd[1] == "show" and "main:" in cmd[2]:
                # Simulate file not existing at base commit
                raise subprocess.CalledProcessError(1, cmd)
            else:
                mock_result.stdout.decode.return_value = ""
            return mock_result

        return mock_run_side_effect

    @patch("subprocess.run")
    def test_no_modified_raw_data_files(self, mock_run: Mock) -> None:
        """Test when no raw data files are modified, returns False."""
        # Mock git diff to return no raw data files
        mock_run.return_value.stdout.decode.return_value = """
some/other/file.py
docs/readme.md
        """.strip()

        result = validate_raw_file_primary_key_updates("main...HEAD")

        self.assertFalse(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_raw_data_files_modified_no_pk_changes(
        self, mock_run: Mock, mock_file_exempt: Mock
    ) -> None:
        """Test when raw data files are modified but no primary key changes."""
        mock_file_exempt.return_value = False
        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml
            """.strip(),
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_with_keys,
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        self.assertFalse(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_primary_key_changes_with_allow_flag(
        self, mock_run: Mock, mock_file_exempt: Mock
    ) -> None:
        """Test when primary keys change but [ALLOW_PK_UPDATES] flag is present."""
        mock_file_exempt.return_value = False
        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml
            """.strip(),
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_different_keys,
            (
                "git",
                "log",
                "--format=%h %B",
                "--grep=[ALLOW_PK_UPDATES]",
                "main...HEAD",
            ): "[ALLOW_PK_UPDATES] Update primary keys",
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        self.assertFalse(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_primary_key_changes_without_allow_flag(
        self, mock_run: Mock, mock_file_exempt: Mock
    ) -> None:
        """Test when primary keys change but no [ALLOW_PK_UPDATES] flag."""
        mock_file_exempt.return_value = False
        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml
            """.strip(),
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_different_keys,
            (
                "git",
                "log",
                "--format=%h %B",
                "--grep=[ALLOW_PK_UPDATES]",
                "main...HEAD",
            ): "",
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        self.assertTrue(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_new_file_with_primary_keys(
        self, mock_run: Mock, mock_file_exempt: Mock
    ) -> None:
        """Test when a new file is added with primary keys but no allow flag"""
        mock_file_exempt.return_value = False
        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/new_file.yaml
            """.strip(),
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_ca/raw_data/new_file.yaml",
            ): "",
            (
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_ca/raw_data/new_file.yaml",
            ): self.sample_yaml_with_keys,
            ("git", "log", "--format=%h %B", "main...HEAD"): "",
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        self.assertFalse(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_deleted_file(self, mock_run: Mock, mock_file_exempt: Mock) -> None:
        """Test when a file is deleted, should not trigger validation failure."""
        mock_file_exempt.return_value = False
        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/deleted_file.yaml
            """.strip(),
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_ca/raw_data/deleted_file.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_ca/raw_data/deleted_file.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "log",
                "--format=%h %B",
                "--grep=[ALLOW_PK_UPDATES]",
                "main...HEAD",
            ): "",
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        self.assertFalse(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_keys_removed_without_allow_flag(
        self, mock_run: Mock, mock_file_exempt: Mock
    ) -> None:
        """Test when primary keys are removed without allow flag."""
        mock_file_exempt.return_value = False
        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml
            """.strip(),
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml",
            ): self.sample_yaml_no_keys,
            (
                "git",
                "log",
                "--format=%h %B",
                "--grep=[ALLOW_PK_UPDATES]",
                "main...HEAD",
            ): "",
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        self.assertTrue(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_multiple_files_mixed_changes(
        self, mock_run: Mock, mock_file_exempt: Mock
    ) -> None:
        """Test multiple files with some having PK changes."""
        mock_file_exempt.return_value = False
        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/file1.yaml
recidiviz/ingest/direct/regions/us_co/raw_data/file2.yaml
            """.strip(),
            # File 1: has PK changes
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_ca/raw_data/file1.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_ca/raw_data/file1.yaml",
            ): self.sample_yaml_different_keys,
            # File 2: no PK changes
            (
                "git",
                "show",
                "main:recidiviz/ingest/direct/regions/us_co/raw_data/file2.yaml",
            ): self.sample_yaml_with_keys,
            (
                "git",
                "show",
                "HEAD:recidiviz/ingest/direct/regions/us_co/raw_data/file2.yaml",
            ): self.sample_yaml_with_keys,
            ("git", "log", "--format=%B", "main...HEAD"): "",
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        # Should fail because file1 has PK changes without allow flag
        self.assertTrue(result)

    @patch(
        "recidiviz.tools.validate_raw_file_primary_key_updates.file_tag_exempt_from_automatic_raw_data_pruning"
    )
    @patch("subprocess.run")
    def test_exempt_file_skipped(self, mock_run: Mock, mock_file_exempt: Mock) -> None:
        """Test that files exempt from pruning are skipped from validation."""
        # Mock file to be exempt from automatic pruning
        mock_file_exempt.return_value = True

        git_responses = {
            (
                "git",
                "diff",
                "--name-only",
                "main...HEAD",
            ): """
recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml
            """.strip(),
        }

        mock_run.side_effect = self._create_mock_git_side_effect(git_responses)
        result = validate_raw_file_primary_key_updates("main...HEAD")
        # Should return False because the exempt file is skipped and no non-exempt files are modified
        self.assertFalse(result)
