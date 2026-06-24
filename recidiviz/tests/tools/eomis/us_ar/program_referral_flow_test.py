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
"""Tests for the AR GED Program Referral writeback flow.

The classification tests are the executable form of the V1 write criteria:
each test name states one rule from the spec.
"""
import csv
import tempfile
import unittest
from typing import Any
from unittest.mock import Mock

from recidiviz.tools.eomis.flow import SKIP_ACTION
from recidiviz.tools.eomis.us_ar.program_referral_flow import (
    CREATE_ACTION,
    PRIORITY_HIGH,
    RELEASE_AND_CREATE_ACTION,
    STATUS_COMPLETED,
    STATUS_REFERRED_NOT_SCREENED,
    UPDATE_ACTION,
    ExistingReferral,
    build_add_payload,
    build_id_candidates,
    build_update_payload,
    classify_csv_row,
    classify_view_row,
    is_completed_status,
    is_flippable_status,
    load_csv_candidates,
    referral_seq,
    skip_offenders_already_completed_this_incarceration,
    success_detail,
)


def _view_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "OFFENDERID": "0000001",
        "certificate_award_date": "2024-01-02",
        "referral_application_date": None,
        "referral_status": None,
        "completed_in_prior_incarceration_flag": False,
        "entered_in_prior_incarceration_flag": False,
    }
    row.update(overrides)
    return row


class TestClassifyViewRow(unittest.TestCase):
    """Tests the V1 write criteria over reconciliation-view rows."""

    def test_prior_incarceration_completion_is_skipped(self) -> None:
        candidate = classify_view_row(
            _view_row(completed_in_prior_incarceration_flag=True)
        )
        self.assertEqual(candidate.action, SKIP_ACTION)
        self.assertEqual(candidate.reason, "prior completion")

    def test_missing_referral_is_created_dated_to_certificate(self) -> None:
        candidate = classify_view_row(_view_row())
        self.assertEqual(candidate.action, CREATE_ACTION)
        self.assertEqual(candidate.referral_date, "01/02/2024")

    def test_open_referral_is_marked_complete(self) -> None:
        candidate = classify_view_row(
            _view_row(
                referral_status="Referred, not Screened",
                referral_application_date="2023-11-30",
            )
        )
        self.assertEqual(candidate.action, UPDATE_ACTION)
        self.assertEqual(candidate.referral_date, "11/30/2023")

    def test_already_complete_referral_is_skipped(self) -> None:
        candidate = classify_view_row(
            _view_row(
                referral_status="Completed successfully",
                referral_application_date="2023-11-30",
            )
        )
        self.assertEqual(candidate.action, SKIP_ACTION)
        self.assertEqual(candidate.reason, "already complete")

    def test_unsupported_status_is_skipped_for_manual_review(self) -> None:
        candidate = classify_view_row(
            _view_row(
                referral_status="Withdrawn",
                referral_application_date="2023-11-30",
            )
        )
        self.assertEqual(candidate.action, SKIP_ACTION)
        self.assertEqual(candidate.reason, "unsupported referral status")

    def test_prior_incarceration_referral_is_released_then_recreated(self) -> None:
        candidate = classify_view_row(
            _view_row(
                referral_status="Referred, not Screened",
                referral_application_date="2019-05-01",
                entered_in_prior_incarceration_flag=True,
            )
        )
        self.assertEqual(candidate.action, RELEASE_AND_CREATE_ACTION)
        self.assertEqual(candidate.referral_date, "01/02/2024")
        self.assertEqual(candidate.prior_referral_date, "05/01/2019")

    def test_completed_prior_incarceration_referral_needs_manual_review(self) -> None:
        candidate = classify_view_row(
            _view_row(
                referral_status="Completed successfully",
                referral_application_date="2019-05-01",
                entered_in_prior_incarceration_flag=True,
            )
        )
        self.assertEqual(candidate.action, SKIP_ACTION)
        self.assertIn("needs manual review", candidate.reason)

    def test_prior_referral_sharing_certificate_date_needs_manual_review(self) -> None:
        candidate = classify_view_row(
            _view_row(
                referral_status="Referred, not Screened",
                referral_application_date="2024-01-02",
                entered_in_prior_incarceration_flag=True,
            )
        )
        self.assertEqual(candidate.action, SKIP_ACTION)
        self.assertIn("shares certificate date", candidate.reason)


class TestStatusHelpers(unittest.TestCase):
    def test_status_matching_is_case_insensitive(self) -> None:
        self.assertTrue(is_completed_status("COMPLETED SUCCESSFULLY"))
        self.assertTrue(is_flippable_status("Active Participant"))
        self.assertFalse(is_completed_status("Referred, not Screened"))
        self.assertFalse(is_flippable_status("Withdrawn"))


class TestPayloads(unittest.TestCase):
    """Tests the proven two-step create/update form bodies."""

    def test_add_payload_creates_as_referred_not_screened(self) -> None:
        payload = build_add_payload(
            referral_date="01/02/2024",
            status_date="01/02/2024",
            comment="<p>note</p>",
            uuid="abc",
        )
        self.assertIn(
            ("ProgramApplicationStatus", STATUS_REFERRED_NOT_SCREENED), payload
        )
        # eOMIS assigns the sequence number; "***" is the add-form sentinel.
        self.assertIn(("ProgramReferralSeqNbr", "***"), payload)
        self.assertIn(("ProgramReferralPriority", PRIORITY_HIGH), payload)
        self.assertIn(("dateLastUpdate", ""), payload)
        self.assertIn(("PgmReferralComments", "<p>note</p>"), payload)

    def test_update_payload_echoes_last_update_stamps(self) -> None:
        referral = ExistingReferral(
            seq="002",
            referral_date="01/02/2024",
            status_label="Referred, not Screened",
            detail_url="/eomis/detail?ProgramReferralSeqNbr=002",
            date_last_update="01/03/2024",
            time_last_update="10:11:12",
            staff_last_update="0999998",
            priority="9",
        )
        payload = build_update_payload(
            referral=referral,
            status=STATUS_COMPLETED,
            status_date="06/11/2026",
            comment="<p>done</p>",
            uuid="abc",
        )
        self.assertIn(("dateLastUpdate", "01/03/2024"), payload)
        self.assertIn(("timeLastUpdate", "10:11:12"), payload)
        self.assertIn(("staffLastUpdate", "0999998"), payload)
        self.assertIn(("ProgramReferralSeqNbr", "002"), payload)
        self.assertIn(("ProgramApplicationStatus", STATUS_COMPLETED), payload)
        # The update must not change the priority a human set on the referral.
        self.assertIn(("ProgramReferralPriority", "9"), payload)


class TestSuccessDetail(unittest.TestCase):
    def test_recognizes_known_success_markers(self) -> None:
        for marker in (
            "added successfully",
            "Record updated",
            "Release Dates have been recomputed",
        ):
            response = Mock(text=f"<html>{marker}</html>", status_code=200)
            self.assertEqual(success_detail(response), marker)

    def test_unknown_response_is_not_reported_as_success(self) -> None:
        response = Mock(text="<html>Unexpected error page</html>", status_code=200)
        self.assertEqual(success_detail(response), "POST 200; no known success marker")


class TestReferralSeq(unittest.TestCase):
    def test_prefers_detail_url_then_form_value_then_unique_id(self) -> None:
        self.assertEqual(
            referral_seq(
                {"_detail_url": "/detail?ProgramReferralSeqNbr=007&x=1"},
                {"ProgramReferralSeqNbr": "008"},
            ),
            "007",
        )
        self.assertEqual(referral_seq({}, {"ProgramReferralSeqNbr": "008"}), "008")
        self.assertEqual(referral_seq({"_detailuniqueid": "row009"}, {}), "009")
        self.assertEqual(referral_seq({}, {}), "")


class TestManualSources(unittest.TestCase):
    """Tests the csv/ids candidate sources used for attended runs."""

    def test_id_candidates_require_explicit_action_and_date(self) -> None:
        with self.assertRaisesRegex(ValueError, "--action create or --action update"):
            build_id_candidates(["0000001"], "auto", None)
        with self.assertRaisesRegex(ValueError, "--referral-date"):
            build_id_candidates(["0000001"], CREATE_ACTION, None)
        [candidate] = build_id_candidates(["0000001"], CREATE_ACTION, "2024-01-02")
        self.assertEqual(candidate.action, CREATE_ACTION)
        self.assertEqual(candidate.referral_date, "01/02/2024")

    def test_csv_row_with_explicit_action_bypasses_classification(self) -> None:
        candidate = classify_csv_row(
            {
                "OFFENDERID": "0000001",
                "action": "UPDATE",
                "referral_date": "2024-01-02",
            }
        )
        self.assertEqual(candidate.action, UPDATE_ACTION)
        self.assertEqual(candidate.reason, "csv action")

    def test_csv_row_without_action_uses_view_classification(self) -> None:
        candidate = classify_csv_row(_view_row())
        self.assertEqual(candidate.action, CREATE_ACTION)

    def test_csv_row_with_unknown_action_fails_before_any_write(self) -> None:
        with self.assertRaisesRegex(ValueError, r"unknown action \[delete\]"):
            classify_csv_row({"OFFENDERID": "0000001", "action": "delete"})

    def test_csv_file_must_have_offender_id_column(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", newline="", encoding="utf-8"
        ) as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=["other_column"])
            writer.writeheader()
            writer.writerow({"other_column": "x"})
            csv_file.flush()
            with self.assertRaisesRegex(ValueError, "OFFENDERID column"):
                load_csv_candidates(csv_file.name, None)

    def test_csv_release_and_create_requires_prior_referral_date(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", newline="", encoding="utf-8"
        ) as csv_file:
            writer = csv.DictWriter(
                csv_file, fieldnames=["OFFENDERID", "action", "referral_date"]
            )
            writer.writeheader()
            writer.writerow(
                {
                    "OFFENDERID": "0000001",
                    "action": RELEASE_AND_CREATE_ACTION,
                    "referral_date": "2024-01-02",
                }
            )
            csv_file.flush()
            with self.assertRaisesRegex(ValueError, "prior_referral_date"):
                load_csv_candidates(csv_file.name, None)


class TestSuppressAlreadyCompletedOffenders(unittest.TestCase):
    """A person appears on one view row per referral. A current completed referral
    on one row must suppress writes derived from that person's other rows."""

    def test_current_completion_suppresses_release_and_create_from_stale_row(
        self,
    ) -> None:
        rows = [
            _view_row(
                referral_status="Completed successfully",
                referral_application_date="2024-01-02",
            ),
            _view_row(
                referral_status="Released from Custody",
                referral_application_date="2019-05-01",
                entered_in_prior_incarceration_flag=True,
            ),
        ]
        candidates = [classify_view_row(row) for row in rows]
        self.assertEqual(
            [candidate.action for candidate in candidates],
            [SKIP_ACTION, RELEASE_AND_CREATE_ACTION],
        )

        result = skip_offenders_already_completed_this_incarceration(rows, candidates)

        self.assertTrue(all(candidate.action == SKIP_ACTION for candidate in result))
        self.assertIn("already completed a GED referral", result[1].reason)

    def test_writes_for_other_offenders_are_left_alone(self) -> None:
        rows = [
            _view_row(
                OFFENDERID="0000001",
                referral_status="Completed successfully",
                referral_application_date="2024-01-02",
            ),
            _view_row(OFFENDERID="0000002"),
        ]
        candidates = [classify_view_row(row) for row in rows]

        result = skip_offenders_already_completed_this_incarceration(rows, candidates)

        self.assertEqual(result[1].action, CREATE_ACTION)


if __name__ == "__main__":
    unittest.main()
