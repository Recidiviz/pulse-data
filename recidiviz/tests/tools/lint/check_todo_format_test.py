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
"""Tests for check_todo_format."""

import os
import tempfile
import unittest

from recidiviz.tools.lint.check_todo_format import check_file, is_excluded, main


class IsExcludedTest(unittest.TestCase):
    """Tests for is_excluded()."""

    def test_excluded_files(self) -> None:
        self.assertTrue(is_excluded(".github/workflows/find-linked-todos.yml"))
        self.assertTrue(is_excluded(".github/workflows/find-closed-todos.yml"))
        self.assertTrue(
            is_excluded("recidiviz/tools/github/find_closing_issue_todos.py")
        )
        self.assertTrue(is_excluded("recidiviz/repo/issue.py"))
        self.assertTrue(is_excluded("recidiviz/tests/repo/issue_test.py"))
        self.assertTrue(is_excluded("recidiviz/repo/issue_references.py"))
        self.assertTrue(is_excluded("recidiviz/tests/repo/issue_references_test.py"))
        self.assertTrue(is_excluded("bandit-baseline.json"))
        self.assertTrue(
            is_excluded(
                "recidiviz/tools/deploy/atmos/components/terraform/vendor/foo.tf"
            )
        )
        self.assertTrue(is_excluded("recidiviz/tools/lint/run_pylint.sh"))
        self.assertTrue(is_excluded(".pylintrc"))
        self.assertTrue(is_excluded("CLAUDE.md"))
        self.assertTrue(is_excluded("some/path/templates/foo.txt"))
        self.assertTrue(is_excluded("some/nbautoexports/foo.py"))
        self.assertTrue(is_excluded("some/migrated_notebooks/foo.py"))
        self.assertTrue(is_excluded("recidiviz/tools/lint/check_todo_format.py"))
        self.assertTrue(
            is_excluded("recidiviz/tests/tools/lint/check_todo_format_test.py")
        )

    def test_non_excluded_files(self) -> None:
        self.assertFalse(is_excluded("recidiviz/common/date.py"))
        self.assertFalse(is_excluded("recidiviz/pipelines/metrics/foo.py"))
        self.assertFalse(is_excluded("recidiviz/ingest/direct/regions/us_xx/foo.py"))


class CheckFileTest(unittest.TestCase):
    """Tests for check_file()."""

    def _write_temp_file(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(content)
        self.addCleanup(os.unlink, path)
        return path

    def test_valid_github_issue_todo(self) -> None:
        path = self._write_temp_file("# TODO(#123): Fix this\n")
        self.assertEqual(check_file(path), [])

    def test_valid_github_issue_with_repo_todo(self) -> None:
        path = self._write_temp_file("# TODO(Recidiviz/pulse-dashboard#456): Remap\n")
        self.assertEqual(check_file(path), [])

    def test_valid_linear_issue_todo(self) -> None:
        path = self._write_temp_file("# TODO(OBT-12345): Migrate this\n")
        self.assertEqual(check_file(path), [])

    def test_valid_url_todo(self) -> None:
        path = self._write_temp_file(
            "# TODO(https://issues.apache.org/jira/browse/BEAM-12641): Upgrade\n"
        )
        self.assertEqual(check_file(path), [])

    def test_invalid_todo_bare_text(self) -> None:
        path = self._write_temp_file("# TODO(XXXX): Bad format\n")
        result = check_file(path)
        self.assertEqual(len(result), 1)
        self.assertIn("TODO(XXXX)", result[0])

    def test_invalid_todo_bare_number(self) -> None:
        path = self._write_temp_file("# TODO(123): Missing hash\n")
        result = check_file(path)
        self.assertEqual(len(result), 1)

    def test_mixed_valid_and_invalid(self) -> None:
        path = self._write_temp_file(
            "# TODO(#123): Good\n# TODO(bad): Bad\n# TODO(OBT-999): Good\n"
        )
        result = check_file(path)
        self.assertEqual(len(result), 1)
        self.assertIn("TODO(bad)", result[0])

    def test_no_todos(self) -> None:
        path = self._write_temp_file("x = 1\ny = 2\n")
        self.assertEqual(check_file(path), [])

    def test_todo_in_word_ignored(self) -> None:
        path = self._write_temp_file("# AUTODO something\n")
        self.assertEqual(check_file(path), [])


class MainTest(unittest.TestCase):
    """Tests for main()."""

    def _write_temp_file(self, content: str, suffix: str = ".py") -> str:
        fd, path = tempfile.mkstemp(suffix=suffix)
        with os.fdopen(fd, "w") as f:
            f.write(content)
        self.addCleanup(os.unlink, path)
        return path

    def test_returns_zero_when_all_valid(self) -> None:
        path = self._write_temp_file("# TODO(#123): Good\n")
        self.assertEqual(main([path]), 0)

    def test_returns_one_when_invalid(self) -> None:
        path = self._write_temp_file("# TODO(bad): Bad\n")
        self.assertEqual(main([path]), 1)

    def test_excludes_files(self) -> None:
        path = self._write_temp_file(
            "# TODO(bad): Bad\n", suffix="_issue_references.py"
        )
        self.assertEqual(main([path]), 0)

    def test_empty_file_list(self) -> None:
        self.assertEqual(main([]), 0)
