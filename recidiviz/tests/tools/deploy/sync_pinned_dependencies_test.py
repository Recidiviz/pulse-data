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
"""Tests for recidiviz.tools.deploy.sync_pinned_dependencies."""
import unittest

from recidiviz.tools.deploy.sync_pinned_dependencies import rewrite_requirements_lines

DISPLAY_PATH = "recidiviz/path/to/requirements.txt"


class TestRewriteRequirementsLines(unittest.TestCase):
    """Tests rewrite_requirements_lines against small in-memory fixtures."""

    def test_updates_stale_pin_preserves_comments_and_casing(self) -> None:
        lines = [
            "# A comment that must survive untouched.\n",
            "PyGithub==2.9.1\n",
            "\n",
            "bar==2.0.0\n",
        ]

        new_lines, changed = rewrite_requirements_lines(
            lines=lines,
            uv_versions={"pygithub": "2.10.0", "bar": "2.0.0"},
            display_path=DISPLAY_PATH,
        )

        self.assertTrue(changed)
        self.assertEqual(
            [
                "# A comment that must survive untouched.\n",
                "PyGithub==2.10.0\n",
                "\n",
                "bar==2.0.0\n",
            ],
            new_lines,
        )

    def test_noop_when_already_synced(self) -> None:
        lines = ["requests==2.34.2\n"]

        new_lines, changed = rewrite_requirements_lines(
            lines=lines,
            uv_versions={"requests": "2.34.2"},
            display_path=DISPLAY_PATH,
        )

        self.assertFalse(changed)
        self.assertEqual(lines, new_lines)

    def test_leaves_unpinned_and_non_uv_lock_entries_alone(self) -> None:
        lines = [
            "foo>=1.0.0\n",
            "not-in-uv-lock==9.9.9\n",
        ]

        new_lines, changed = rewrite_requirements_lines(
            lines=lines,
            uv_versions={"foo": "1.5.0"},
            display_path=DISPLAY_PATH,
        )

        self.assertFalse(changed)
        self.assertEqual(lines, new_lines)

    def test_preserves_extras_and_markers(self) -> None:
        lines = [
            "foo[redis]==1.0.0\n",
            'bar==1.0.0; python_version < "3.12"\n',
            'baz[a,b]==1.0.0; sys_platform == "linux"\n',
        ]

        new_lines, changed = rewrite_requirements_lines(
            lines=lines,
            uv_versions={"foo": "1.5.0", "bar": "2.5.0", "baz": "3.5.0"},
            display_path=DISPLAY_PATH,
        )

        self.assertTrue(changed)
        self.assertEqual(
            [
                "foo[redis]==1.5.0\n",
                'bar==2.5.0; python_version < "3.12"\n',
                'baz[a,b]==3.5.0; sys_platform == "linux"\n',
            ],
            new_lines,
        )

    def test_leaves_direct_url_reference_alone(self) -> None:
        lines = ["foo @ https://example.com/foo-1.0.0.tar.gz\n"]

        new_lines, changed = rewrite_requirements_lines(
            lines=lines,
            uv_versions={"foo": "1.5.0"},
            display_path=DISPLAY_PATH,
        )

        self.assertFalse(changed)
        self.assertEqual(lines, new_lines)

    def test_preserves_missing_trailing_newline(self) -> None:
        lines = ["foo==1.0.0"]

        new_lines, changed = rewrite_requirements_lines(
            lines=lines,
            uv_versions={"foo": "1.5.0"},
            display_path=DISPLAY_PATH,
        )

        self.assertTrue(changed)
        self.assertEqual(["foo==1.5.0"], new_lines)
