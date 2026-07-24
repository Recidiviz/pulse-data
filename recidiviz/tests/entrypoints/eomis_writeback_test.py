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
"""Tests for the eOMIS writeback entrypoint."""
import argparse
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.entrypoints.eomis_writeback import (
    EomisWritebackEntrypoint,
    base_url_for_project,
    build_flow,
)
from recidiviz.eomis.flow import ResultStatus, WriteResult
from recidiviz.eomis.us_ar.program_referral_flow import (
    PROD_BASE_URL,
    TEST_BASE_URL,
    ArProgramReferralCandidate,
    default_view,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


def _candidate(action: str) -> ArProgramReferralCandidate:
    return ArProgramReferralCandidate(
        offender_id="0000001",
        action=action,
        reason="test",
        referral_date="01/02/2024",
        referral_status=None,
    )


def _result(status: ResultStatus) -> WriteResult:
    return WriteResult(_candidate("create"), status, "detail")


class TestArgumentParsing(unittest.TestCase):
    """Tests entrypoint argument parsing."""

    def setUp(self) -> None:
        self.parser = EomisWritebackEntrypoint.get_parser()

    def test_dry_run_is_the_default(self) -> None:
        args = self.parser.parse_args(["--flow", "ar_ged"])
        self.assertFalse(args.commit)
        self.assertFalse(args.allow_prod_write)
        self.assertIsNone(args.max_writes)
        self.assertIsNone(args.bq_view)

    def test_all_args(self) -> None:
        args = self.parser.parse_args(
            [
                "--flow=ar_ged",
                "--commit",
                "--max-writes=5",
                "--bq-view=some-project.some_dataset.some_view",
                "--allow-prod-write",
            ]
        )
        self.assertEqual(args.flow, "ar_ged")
        self.assertTrue(args.commit)
        self.assertEqual(args.max_writes, 5)
        self.assertEqual(args.bq_view, "some-project.some_dataset.some_view")
        self.assertTrue(args.allow_prod_write)

    def test_flow_is_required_and_validated(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args([])
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--flow", "not_a_flow"])


class TestFlowConstruction(unittest.TestCase):
    """Tests flow building and instance selection."""

    def test_builds_the_ar_flow_configured_like_the_cli(self) -> None:
        with patch(
            "recidiviz.entrypoints.eomis_writeback.ArProgramReferralFlow"
        ) as mock_flow_cls, patch(
            "recidiviz.entrypoints.eomis_writeback.metadata.project_id",
            return_value=GCP_PROJECT_STAGING,
        ):
            build_flow("ar_ged", bq_view=None)
        mock_flow_cls.assert_called_once_with(
            bq_view=default_view(GCP_PROJECT_STAGING),
            project_id=GCP_PROJECT_STAGING,
            limit=None,
            comment="",
            add_comment="",
        )

    def test_bq_view_override(self) -> None:
        with patch(
            "recidiviz.entrypoints.eomis_writeback.ArProgramReferralFlow"
        ) as mock_flow_cls, patch(
            "recidiviz.entrypoints.eomis_writeback.metadata.project_id",
            return_value=GCP_PROJECT_STAGING,
        ):
            build_flow("ar_ged", bq_view="p.d.v")
        self.assertEqual(mock_flow_cls.call_args.kwargs["bq_view"], "p.d.v")

    def test_unknown_flow_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"^Unexpected flow name: \[nope\]$"):
            build_flow("nope", bq_view=None)

    def test_staging_targets_the_test_instance(self) -> None:
        self.assertEqual(
            base_url_for_project("ar_ged", GCP_PROJECT_STAGING), TEST_BASE_URL
        )

    def test_production_targets_the_prod_instance(self) -> None:
        self.assertEqual(
            base_url_for_project("ar_ged", GCP_PROJECT_PRODUCTION), PROD_BASE_URL
        )


@patch("recidiviz.entrypoints.eomis_writeback.run_writeback")
@patch("recidiviz.entrypoints.eomis_writeback.EomisClient")
@patch("recidiviz.entrypoints.eomis_writeback.resolve_eomis_credentials")
@patch("recidiviz.entrypoints.eomis_writeback.build_flow")
@patch("recidiviz.entrypoints.eomis_writeback.metadata.project_id")
class TestRunEntrypoint(unittest.TestCase):
    """Tests the run semantics: prod-write guard, dry-run default, and the
    non-zero exit when any candidate errored."""

    def _parse(self, argv: list[str]) -> argparse.Namespace:
        return EomisWritebackEntrypoint.get_parser().parse_args(argv)

    def test_commit_against_prod_requires_allow_prod_write(
        self,
        mock_project_id: MagicMock,
        mock_build_flow: MagicMock,
        _mock_credentials: MagicMock,
        _mock_client: MagicMock,
        _mock_run: MagicMock,
    ) -> None:
        mock_project_id.return_value = GCP_PROJECT_PRODUCTION
        with self.assertRaisesRegex(
            ValueError, r"^production writes require --allow-prod-write$"
        ):
            EomisWritebackEntrypoint.run_entrypoint(
                args=self._parse(["--flow=ar_ged", "--commit"])
            )
        mock_build_flow.assert_not_called()

    def test_dry_run_against_prod_project_is_allowed(
        self,
        mock_project_id: MagicMock,
        mock_build_flow: MagicMock,
        _mock_credentials: MagicMock,
        _mock_client: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        mock_project_id.return_value = GCP_PROJECT_PRODUCTION
        mock_build_flow.return_value.load_candidates.return_value = [
            _candidate("create")
        ]
        mock_run.return_value = [_result(ResultStatus.DRY_RUN)]

        EomisWritebackEntrypoint.run_entrypoint(args=self._parse(["--flow=ar_ged"]))

        self.assertFalse(mock_run.call_args.kwargs["commit"])

    def test_skip_only_run_makes_no_client_and_succeeds(
        self,
        mock_project_id: MagicMock,
        mock_build_flow: MagicMock,
        mock_credentials: MagicMock,
        mock_client: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        mock_project_id.return_value = GCP_PROJECT_STAGING
        mock_build_flow.return_value.load_candidates.return_value = [_candidate("skip")]

        EomisWritebackEntrypoint.run_entrypoint(args=self._parse(["--flow=ar_ged"]))

        mock_credentials.assert_not_called()
        mock_client.assert_not_called()
        mock_run.assert_not_called()

    def test_any_errored_candidate_raises(
        self,
        mock_project_id: MagicMock,
        mock_build_flow: MagicMock,
        _mock_credentials: MagicMock,
        _mock_client: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        mock_project_id.return_value = GCP_PROJECT_STAGING
        mock_build_flow.return_value.load_candidates.return_value = [
            _candidate("create"),
            _candidate("update"),
        ]
        mock_run.return_value = [
            _result(ResultStatus.SUCCESS),
            _result(ResultStatus.ERROR),
        ]

        with self.assertRaisesRegex(RuntimeError, r"\[1\] of \[2\] candidates errored"):
            EomisWritebackEntrypoint.run_entrypoint(
                args=self._parse(["--flow=ar_ged", "--commit"])
            )

    def test_max_writes_defaults_to_the_flow_circuit_breaker(
        self,
        mock_project_id: MagicMock,
        mock_build_flow: MagicMock,
        _mock_credentials: MagicMock,
        _mock_client: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        mock_project_id.return_value = GCP_PROJECT_STAGING
        mock_build_flow.return_value.load_candidates.return_value = [
            _candidate("create")
        ]
        mock_build_flow.return_value.max_writes_per_run = 25
        mock_run.return_value = [_result(ResultStatus.DRY_RUN)]

        EomisWritebackEntrypoint.run_entrypoint(args=self._parse(["--flow=ar_ged"]))
        self.assertEqual(mock_run.call_args.kwargs["max_writes"], 25)

        EomisWritebackEntrypoint.run_entrypoint(
            args=self._parse(["--flow=ar_ged", "--max-writes=3"])
        )
        self.assertEqual(mock_run.call_args.kwargs["max_writes"], 3)


if __name__ == "__main__":
    unittest.main()
