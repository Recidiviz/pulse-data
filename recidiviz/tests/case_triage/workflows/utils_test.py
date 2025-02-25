#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for workflows api utils."""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from recidiviz.case_triage.workflows.utils import (
    allowed_twilio_dev_recipient,
    get_sms_request_firestore_path,
)


class TestWorkflowsRouteUtils(TestCase):
    """Test suite for workflows route utils"""

    @patch("recidiviz.case_triage.workflows.utils.FirestoreClientImpl")
    def test_number_allowed(self, mock_firestore_impl: MagicMock) -> None:
        mock_firestore_impl.return_value.get_document.return_value.get.return_value.to_dict.return_value = {
            "allowedTwilioRecipients": {"8884445555": True}
        }

        self.assertTrue(allowed_twilio_dev_recipient("8884445555"))

    @patch("recidiviz.case_triage.workflows.utils.FirestoreClientImpl")
    def test_number_explicitly_disallowed(self, mock_firestore_impl: MagicMock) -> None:
        mock_firestore_impl.return_value.get_document.return_value.get.return_value.to_dict.return_value = {
            "allowedTwilioRecipients": {"5558675309": False}
        }

        self.assertFalse(allowed_twilio_dev_recipient("5558675309"))

    @patch("recidiviz.case_triage.workflows.utils.FirestoreClientImpl")
    def test_number_not_listed(self, mock_firestore_impl: MagicMock) -> None:
        mock_firestore_impl.return_value.get_document.return_value.get.return_value.to_dict.return_value = {
            "allowedTwilioRecipients": {}
        }

        self.assertFalse(allowed_twilio_dev_recipient("8775994444"))

    @patch("recidiviz.case_triage.workflows.utils.FirestoreClientImpl")
    def test_key_missing_from_config(self, mock_firestore_impl: MagicMock) -> None:
        mock_firestore_impl.return_value.get_document.return_value.get.return_value.to_dict.return_value = (
            {}
        )

        self.assertFalse(allowed_twilio_dev_recipient("8775994444"))

    @patch("recidiviz.case_triage.workflows.utils.FirestoreClientImpl")
    def test_firestore_failure_returns_false(
        self, mock_firestore_impl: MagicMock
    ) -> None:
        mock_firestore_impl.return_value.get_document.side_effect = Exception

        self.assertFalse(allowed_twilio_dev_recipient("8775994444"))

    @freeze_time("2023-01-01 01:23:45")
    def test_get_sms_request_firestore_path(self) -> None:
        self.assertEqual(
            get_sms_request_firestore_path(state="US_CA", recipient_external_id="1234"),
            "clientUpdatesV2/us_ca_1234/milestonesMessages/milestones_01_2023",
        )
