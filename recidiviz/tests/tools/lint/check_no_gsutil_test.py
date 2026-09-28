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
"""Tests for check_no_gsutil."""

import os
import tempfile
import unittest

from recidiviz.tools.lint.check_no_gsutil import (
    check_file,
    is_excluded,
    line_has_invocation,
    main,
)


class IsExcludedTest(unittest.TestCase):
    """Tests for is_excluded()."""

    def test_excluded_files(self) -> None:
        self.assertTrue(
            is_excluded("recidiviz/research/migrated_notebooks/jkunz/foo.ipynb")
        )
        self.assertTrue(is_excluded("some/nbautoexports/foo.py"))
        self.assertTrue(is_excluded("bandit_configs/bandit-baseline.json"))
        self.assertTrue(is_excluded("recidiviz/tools/lint/check_no_gsutil.py"))
        self.assertTrue(
            is_excluded("recidiviz/tests/tools/lint/check_no_gsutil_test.py")
        )

    def test_non_excluded_files(self) -> None:
        self.assertFalse(is_excluded("recidiviz/tools/deploy/deploy_helpers.sh"))
        self.assertFalse(is_excluded("recidiviz/tools/gsutil_shell_helpers.py"))
        self.assertFalse(is_excluded(".github/workflows/ci.yml"))
        self.assertFalse(is_excluded(".claude/settings.json"))


class LineHasInvocationTest(unittest.TestCase):
    """Tests for line_has_invocation()."""

    def test_flags_shell_invocations(self) -> None:
        for line in [
            "gsutil cp a.txt gs://b/",
            "  gsutil -m rsync -r -c -d src gs://b/",
            'TAG="$(gsutil cat gs://b/state.json | jq -r .x)"',
            "cat merged.json | gsutil cp - gs://b/x.json",
            "true && gsutil ls gs://b/",
            "step: gsutil rm -r gs://b/old/",
        ]:
            self.assertTrue(line_has_invocation(line), line)

    def test_flags_python_strings_and_argv(self) -> None:
        for line in [
            'cmd = f"gsutil cp {csv_path} {dest}"',
            "command = \"gsutil ls -d 'gs://b/*'\"",
            '["gsutil", "cp", csv_path, dest], check=False',
            "subprocess.run(['gsutil', 'mv', src, dst])",
            '["gsutil", "-m", "cp", src, dst]',
            'cmd = f\'gsutil cp "{src}" "{dst}"\'',
        ]:
            self.assertTrue(line_has_invocation(line), line)

    def test_ignores_command_in_trailing_comment(self) -> None:
        for line in [
            "result = run()  # was gsutil cp a b",
            'gcloud storage cp a gs://b/  # replaced "gsutil cp a gs://b/"',
        ]:
            self.assertFalse(line_has_invocation(line), line)
        # A hash with no leading space is not a comment marker.
        self.assertTrue(line_has_invocation('echo "$#"; gsutil ls gs://b/'))

    def test_ignores_single_quoted_command_in_prose(self) -> None:
        for line in [
            '    """Returns list of paths returned by \'gsutil ls <gs_path>\'.',
            "    Copies a file via 'gsutil cp'. See the docs.",
        ]:
            self.assertFalse(line_has_invocation(line), line)

    def test_ignores_identifiers(self) -> None:
        for line in [
            "from recidiviz.tools.gsutil_shell_helpers import gsutil_ls",
            "paths = gsutil_ls(uri, directories_only=True)",
            "GSUTIL_DEFAULT_TIMEOUT_SEC = 60 * 20",
            "recidiviz/tests/tools/gsutil_shell_helpers_test.py",
        ]:
            self.assertFalse(line_has_invocation(line), line)

    def test_ignores_prose(self) -> None:
        for line in [
            "Google removes gsutil from the CLI package after March 2027.",
            "The old tool, gsutil, is no longer installed.",
            "gsutil is deprecated but ships on some images.",
        ]:
            self.assertFalse(line_has_invocation(line), line)

    def test_ignores_comments_and_inline_code(self) -> None:
        for line in [
            "# gsutil cp is what this used to run",
            "    # $ gsutil cp gs://a gs://b",
            '`gsutil ls -d "gs://bucket/prefix/*"` printed one line per match.',
            "See `gsutil rsync -c` for the old behaviour.",
        ]:
            self.assertFalse(line_has_invocation(line), line)

    def test_ignores_claude_allowlist_entry(self) -> None:
        self.assertFalse(line_has_invocation('      "Bash(gsutil ls:*)",'))

    def test_ignores_gcloud_storage(self) -> None:
        for line in [
            "gcloud storage cp a.txt gs://b/",
            "command = f'gcloud storage ls \"{gs_path}\"'",
        ]:
            self.assertFalse(line_has_invocation(line), line)


class CheckFileTest(unittest.TestCase):
    """Tests for check_file() and main()."""

    def _write_temp_file(self, content: str, suffix: str = ".sh") -> str:
        fd, path = tempfile.mkstemp(suffix=suffix)
        with os.fdopen(fd, "w") as f:
            f.write(content)
        self.addCleanup(os.unlink, path)
        return path

    def test_reports_line_numbers(self) -> None:
        path = self._write_temp_file(
            "#!/bin/bash\nset -e\ngsutil cp a gs://b/\ngcloud storage ls gs://b/\n"
        )
        result = check_file(path)
        self.assertEqual(1, len(result))
        self.assertIn(":3:gsutil cp a gs://b/", result[0])

    def test_clean_file(self) -> None:
        path = self._write_temp_file("gcloud storage cp a gs://b/\n")
        self.assertEqual([], check_file(path))

    def test_main_exit_codes(self) -> None:
        bad = self._write_temp_file("gsutil ls gs://b/\n")
        good = self._write_temp_file("gcloud storage ls gs://b/\n")
        self.assertEqual(1, main([bad, good]))
        self.assertEqual(0, main([good]))

    def test_main_skips_excluded_paths(self) -> None:
        directory = tempfile.mkdtemp()
        excluded_dir = os.path.join(directory, "migrated_notebooks")
        os.makedirs(excluded_dir)
        path = os.path.join(excluded_dir, "old.py")
        with open(path, "w", encoding="utf-8") as f:
            f.write("gsutil ls gs://b/\n")
        self.addCleanup(os.unlink, path)
        self.assertEqual(0, main([path]))
