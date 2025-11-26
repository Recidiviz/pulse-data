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
"""Tests for GitHubCheckStatusResult class."""
import unittest
from unittest.mock import Mock

from github.CheckRun import CheckRun

from recidiviz.tools.deploy.verify_github_check_statuses import GitHubCheckStatusResult


def _create_mock_check_run(
    name: str,
    status: str = "completed",
    conclusion: str | None = "success",
) -> CheckRun:
    """Factory function to create mock CheckRun objects."""
    mock_check_run = Mock()
    mock_check_run.name = name
    mock_check_run.status = status
    mock_check_run.conclusion = conclusion
    mock_check_run.html_url = None
    mock_check_run.started_at = None
    return mock_check_run


class GitHubCheckStatusResultTest(unittest.TestCase):
    """Tests for GitHubCheckStatusResult class."""

    def setUp(self) -> None:
        """Set up common test data."""
        self.required_check_names = {"Required Check 1", "Required Check 2"}

    def test_creates_result_with_all_required_checks_present(self) -> None:
        """Test that result can be created when all required checks are present."""
        all_checks = [
            _create_mock_check_run("Required Check 1"),
            _create_mock_check_run("Optional Check"),
            _create_mock_check_run("Required Check 2"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(result.commit_ref, "abc123")
        self.assertEqual(len(result.all_check_runs), 3)

    def test_raises_error_when_required_check_missing(self) -> None:
        """Test that ValueError is raised when a required check is missing."""
        all_checks = [
            _create_mock_check_run("Required Check 1"),
            _create_mock_check_run("Optional Check"),
            # Missing "Required Check 2"
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Found required Github checks which were not run for commit abc123: "
            r"\{'Required Check 2'\}",
        ):
            GitHubCheckStatusResult(
                commit_ref="abc123",
                all_check_runs=all_checks,
                required_check_names=self.required_check_names,
            )

    def test_required_check_runs_filters_by_name(self) -> None:
        """Test that required_check_runs property filters by required names."""
        all_checks = [
            _create_mock_check_run("Required Check 1"),
            _create_mock_check_run("Optional Check"),
            _create_mock_check_run("Required Check 2"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.required_check_runs), 2)
        required_names = {check.name for check in result.required_check_runs}
        self.assertEqual(required_names, {"Required Check 1", "Required Check 2"})

    def test_required_check_runs_empty_when_no_required(self) -> None:
        """Test that required_check_runs is empty when no checks are required."""
        all_checks = [
            _create_mock_check_run("Optional Check 1"),
            _create_mock_check_run("Optional Check 2"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=set(),
        )

        self.assertEqual(len(result.required_check_runs), 0)

    def test_incomplete_required_check_runs_filters_in_progress(self) -> None:
        """Test that incomplete_required_check_runs filters checks with status != 'completed'."""
        all_checks = [
            _create_mock_check_run("Required Check 1", status="in_progress"),
            _create_mock_check_run("Required Check 2", status="completed"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.incomplete_required_check_runs), 1)
        self.assertEqual(
            result.incomplete_required_check_runs[0].name, "Required Check 1"
        )

    def test_incomplete_required_check_runs_empty_when_all_complete(self) -> None:
        """Test that incomplete_required_check_runs is empty when all checks are completed."""
        all_checks = [
            _create_mock_check_run("Required Check 1", status="completed"),
            _create_mock_check_run("Required Check 2", status="completed"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.incomplete_required_check_runs), 0)

    def test_incomplete_required_check_runs_includes_queued(self) -> None:
        """Test that incomplete_required_check_runs includes queued checks."""
        all_checks = [
            _create_mock_check_run(
                "Required Check 1", status="queued", conclusion=None
            ),
            _create_mock_check_run("Required Check 2", status="completed"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.incomplete_required_check_runs), 1)
        self.assertEqual(
            result.incomplete_required_check_runs[0].name, "Required Check 1"
        )

    def test_failing_required_check_runs_includes_failures(self) -> None:
        """Test that failing_required_check_runs includes failed checks."""
        all_checks = [
            _create_mock_check_run("Required Check 1", conclusion="failure"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.failing_required_check_runs), 1)
        self.assertEqual(result.failing_required_check_runs[0].name, "Required Check 1")

    def test_failing_required_check_runs_excludes_incomplete(self) -> None:
        """Test that failing_required_check_runs excludes incomplete checks."""
        all_checks = [
            _create_mock_check_run("Required Check 1", status="in_progress"),
            _create_mock_check_run("Required Check 2", conclusion="failure"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.failing_required_check_runs), 1)
        self.assertEqual(result.failing_required_check_runs[0].name, "Required Check 2")

    def test_failing_required_check_runs_excludes_success(self) -> None:
        """Test that failing_required_check_runs excludes successful checks."""
        all_checks = [
            _create_mock_check_run("Required Check 1", conclusion="success"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.failing_required_check_runs), 0)

    def test_failing_required_check_runs_includes_timed_out(self) -> None:
        """Test that failing_required_check_runs includes timed out checks."""
        all_checks = [
            _create_mock_check_run("Required Check 1", conclusion="timed_out"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.failing_required_check_runs), 1)
        self.assertEqual(result.failing_required_check_runs[0].name, "Required Check 1")

    def test_all_checks_passed_true_when_all_success(self) -> None:
        """Test that all_checks_passed is True when all checks passed."""
        all_checks = [
            _create_mock_check_run("Required Check 1", conclusion="success"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
            _create_mock_check_run("Optional Check", conclusion="success"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertTrue(result.all_checks_passed)

    def test_all_checks_passed_false_when_non_required_fails(self) -> None:
        """Test that all_checks_passed is False when non-required check failed."""
        all_checks = [
            _create_mock_check_run("Required Check 1", conclusion="success"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
            _create_mock_check_run("Optional Check", conclusion="failure"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertFalse(result.all_checks_passed)

    def test_all_checks_passed_false_when_incomplete(self) -> None:
        """Test that all_checks_passed is False when any check is incomplete."""
        all_checks = [
            _create_mock_check_run("Required Check 1", status="in_progress"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertFalse(result.all_checks_passed)

    def test_all_checks_passed_handles_skipped(self) -> None:
        """Test that all_checks_passed considers 'skipped' as success."""
        all_checks = [
            _create_mock_check_run("Required Check 1", conclusion="skipped"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertTrue(result.all_checks_passed)

    def test_mixed_check_states(self) -> None:
        """Test combination of completed/incomplete/failed checks."""
        all_checks = [
            _create_mock_check_run("Required Check 1", status="in_progress"),
            _create_mock_check_run("Required Check 2", conclusion="failure"),
            _create_mock_check_run("Optional Check 1", conclusion="success"),
            _create_mock_check_run(
                "Optional Check 2", status="queued", conclusion=None
            ),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.incomplete_required_check_runs), 1)
        self.assertEqual(len(result.failing_required_check_runs), 1)
        self.assertFalse(result.all_checks_passed)

    def test_only_non_required_checks(self) -> None:
        """Test when required_check_names is empty set."""
        all_checks = [
            _create_mock_check_run("Optional Check 1", conclusion="success"),
            _create_mock_check_run("Optional Check 2", conclusion="failure"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=set(),
        )

        self.assertEqual(len(result.required_check_runs), 0)
        self.assertEqual(len(result.incomplete_required_check_runs), 0)
        self.assertEqual(len(result.failing_required_check_runs), 0)
        self.assertFalse(result.all_checks_passed)  # Optional check failed

    def test_all_checks_are_required(self) -> None:
        """Test when all checks are in required_check_names."""
        all_checks = [
            _create_mock_check_run("Required Check 1", conclusion="success"),
            _create_mock_check_run("Required Check 2", conclusion="success"),
        ]

        result = GitHubCheckStatusResult(
            commit_ref="abc123",
            all_check_runs=all_checks,
            required_check_names=self.required_check_names,
        )

        self.assertEqual(len(result.required_check_runs), 2)
        self.assertEqual(len(result.all_check_runs), 2)
        self.assertTrue(result.all_checks_passed)
