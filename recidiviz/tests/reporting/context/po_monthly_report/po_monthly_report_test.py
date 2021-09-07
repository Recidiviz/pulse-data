# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for po_monthly_report/context.py."""

import json
import os
import textwrap
from copy import deepcopy
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import (
    DEFAULT_MESSAGE_BODY_KEY,
    ReportType,
)
from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext
from recidiviz.reporting.recipient import Recipient

FIXTURE_FILE = "po_monthly_report_data_fixture.json"

TEST_SECRETS = {"po_report_cdn_static_IP": "123.456.7.8"}


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="RECIDIVIZ_TEST"))
@patch("recidiviz.utils.secrets.get_secret", MagicMock(side_effect=TEST_SECRETS.get))
class PoMonthlyReportContextTests(TestCase):
    """Tests for po_monthly_report/context.py."""

    def setUp(self) -> None:
        with open(
            os.path.join(os.path.dirname(__file__), FIXTURE_FILE), encoding="utf-8"
        ) as fixture_file:
            self.recipient = Recipient.from_report_json(json.loads(fixture_file.read()))
            self.recipient.data["batch_id"] = "20201105123033"

    def test_get_report_type(self) -> None:
        context = PoMonthlyReportContext(StateCode.US_ID, self.recipient)
        self.assertEqual(ReportType.POMonthlyReport, context.get_report_type())

    def test_congratulations_text_only_improved_from_last_month(self) -> None:
        """Test that the congratulations text looks correct if only the goals were met for the last month."""
        recipient_data = {
            "pos_discharges_state_average": "6",
            "pos_discharges_district_average": "6",
            "earned_discharges": "0",
            "supervision_downgrades": "0",
            "technical_revocations": "3",
            "crime_revocations": "4",
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()
        self.assertEqual(
            "You improved from last month for 1 metric.", actual["congratulations_text"]
        )

    def test_congratulations_text_only_outperformed_region_averages(self) -> None:
        """Test that the congratulations text looks correct if only the region averages were outperformed."""
        recipient_data = {
            "pos_discharges": "0",
            "earned_discharges": "2",
            "supervision_downgrades": "0",
            "technical_revocations": "3",
            "crime_revocations": "4",
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()
        self.assertEqual(
            "You out-performed other officers like you for 1 metric.",
            actual["congratulations_text"],
        )

    def test_congratulations_text_empty(self) -> None:
        """Test that the congratulations text is an empty string and the display is none if neither
        metric goals were met
        """
        recipient_data = {
            "pos_discharges": "0",
            "earned_discharges": "0",
            "supervision_downgrades": "0",
            "technical_revocations": "3",
            "crime_revocations": "4",
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()
        self.assertEqual("", actual["congratulations_text"])
        self.assertEqual("none", actual["display_congratulations"])

    def test_message_body_override(self) -> None:
        """Test that the message body is overriden by the message_body_override"""
        recipient_data = {
            "pos_discharges": "0",
            "earned_discharges": "0",
            "supervision_downgrades": "0",
            "technical_revocations": "0",
            "crime_revocations": "0",
            "message_body_override": "THIS IS A TEST",
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()
        self.assertEqual("THIS IS A TEST", actual["message_body"])

    # pylint:disable=trailing-whitespace
    def test_attachment_content(self) -> None:
        """Given client details for every section, it returns a formatted string to be used as the email attachment."""
        recipient_data = {
            "pos_discharges_clients": [
                {
                    "person_external_id": 123,
                    "full_name": "ROSS, BOB",
                    "successful_completion_date": "2020-12-01",
                }
            ],
            "earned_discharges_clients": [
                {
                    "person_external_id": 321,
                    "full_name": "POLLOCK, JACKSON",
                    "earned_discharge_date": "2020-12-05",
                }
            ],
            "supervision_downgrades_clients": [
                {
                    "person_external_id": 246,
                    "full_name": "GOYA, FRANCISCO",
                    "latest_supervision_downgrade_date": "2020-12-07",
                    "previous_supervision_level": "MEDIUM",
                    "supervision_level": "MINIMUM",
                }
            ],
            "revocations_clients": [
                {
                    "person_external_id": 456,
                    "full_name": "MUNCH, EDVARD",
                    "revocation_violation_type": "NEW_CRIME",
                    "revocation_report_date": "2020-12-06",
                },
                {
                    "person_external_id": 111,
                    "full_name": "MIRO, JOAN",
                    "revocation_violation_type": "TECHNICAL",
                    "revocation_report_date": "2020-12-10",
                },
            ],
            "absconsions_clients": [
                {
                    "person_external_id": 789,
                    "full_name": "DALI, SALVADOR",
                    "absconsion_report_date": "2020-12-11",
                }
            ],
            "assessments_out_of_date_clients": [
                {"person_external_id": 987, "full_name": "KAHLO, FRIDA"}
            ],
            "facetoface_out_of_date_clients": [
                {"person_external_id": 654, "full_name": "DEGAS, EDGAR"}
            ],
        }

        recipient = self.recipient.create_derived_recipient(recipient_data)

        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()
        expected = textwrap.dedent(
            """\
            MONTHLY RECIDIVIZ REPORT
            Prepared on 11/05/2020, for Christopher
            
            // Successful Case Completion //
            [123]     Ross, Bob     Supervision completed on 12/01/2020    
            
            // Early Discharge //
            [321]     Pollock, Jackson     Discharge granted on 12/05/2020    
            
            // Supervision Downgrades //
            [246]     Goya, Francisco     Supervision level downgraded on 12/07/2020    
            
            // Revocations //
            [111]     Miro, Joan        Technical Only     Revocation recommendation staffed on 12/10/2020    
            [456]     Munch, Edvard     New Crime          Revocation recommendation staffed on 12/06/2020    

            // Absconsions //
            [789]     Dali, Salvador     Absconsion reported on 12/11/2020    
            
            // Out of Date Risk Assessments //
            [987]     Kahlo, Frida    
            
            // Out of Date Face to Face Contacts //
            [654]     Degas, Edgar    
            
            Please send questions or data issues to feedback@recidiviz.org

            Please note: people on probation in custody who technically remain on your caseload are currently counted in your Key Supervision Task percentages, including contacts and risk assessments."""
        )
        self.maxDiff = None
        self.assertEqual(expected, actual["attachment_content"])

    def test_attachment_content_missing_sections(self) -> None:
        """Given client details for just one section, it returns a formatted string to
        be used as the email attachment."""
        recipient_data = {
            "revocations_clients": [
                {
                    "person_external_id": 456,
                    "full_name": "MUNCH, EDVARD",
                    "revocation_violation_type": "NEW_CRIME",
                    "revocation_report_date": "2020-12-06",
                },
                {
                    "person_external_id": 111,
                    "full_name": "MIRO, JOAN",
                    "revocation_violation_type": "TECHNICAL",
                    "revocation_report_date": "2020-12-10",
                },
            ]
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()
        expected = textwrap.dedent(
            """\
            MONTHLY RECIDIVIZ REPORT
            Prepared on 11/05/2020, for Christopher
            
            // Revocations //
            [111]     Miro, Joan        Technical Only     Revocation recommendation staffed on 12/10/2020    
            [456]     Munch, Edvard     New Crime          Revocation recommendation staffed on 12/06/2020    
                
            Please send questions or data issues to feedback@recidiviz.org

            Please note: people on probation in custody who technically remain on your caseload are currently counted in your Key Supervision Task percentages, including contacts and risk assessments."""
        )
        self.assertEqual(expected, actual["attachment_content"])

    def test_prepare_for_generation(self) -> None:
        context = PoMonthlyReportContext(StateCode.US_ID, self.recipient)
        actual = context.get_prepared_data()
        red = "#A43939"
        gray = "#7D9897"
        default_color = "#00413E"

        expected = deepcopy(self.recipient.data)
        expected[
            "static_image_path"
        ] = "http://123.456.7.8/US_ID/po_monthly_report/static"
        expected["message_body"] = context.properties[DEFAULT_MESSAGE_BODY_KEY]
        expected["review_month"] = "May"
        expected["greeting"] = "Hey there, Christopher!"

        # No client data returns None for attachment_content
        expected["attachment_content"] = None

        # [improved] More pos_discharges than district and state average
        # [improved] Higher district average than state average
        expected["pos_discharges"] = "5"
        expected["pos_discharges_color"] = default_color
        expected["pos_discharges_change"] = "2 more than last month. You're on a roll!"
        expected["pos_discharges_change_color"] = gray
        expected["pos_discharges_district_average"] = "1.148"
        expected["pos_discharges_district_average_color"] = default_color
        expected["pos_discharges_state_average"] = "0.95"

        # [improved] More early discharges than district average
        # Lower district average compared to state average
        expected["earned_discharges"] = "1"
        expected["earned_discharges_color"] = red
        expected["earned_discharges_change"] = "2 fewer than last month."
        expected["earned_discharges_change_color"] = red
        expected["earned_discharges_district_average"] = "0.86"
        expected["earned_discharges_district_average_color"] = red
        expected["earned_discharges_state_average"] = "1.657"

        # [improved] More supervision downgrades than district average
        # Higher district average compared to state average
        expected["supervision_downgrades"] = "5"
        expected["supervision_downgrades_color"] = default_color
        expected[
            "supervision_downgrades_change"
        ] = "2 more than last month. You're on a roll!"
        expected["supervision_downgrades_change_color"] = gray
        expected["supervision_downgrades_district_average"] = "2.346"
        expected["supervision_downgrades_district_average_color"] = default_color
        expected["supervision_downgrades_state_average"] = "1.765"

        # [improved] Less technical revocations
        # [improved] Lower district average than state average
        expected["technical_revocations"] = "0"
        expected["technical_revocations_color"] = default_color
        expected["technical_revocations_district_average"] = "2.022"
        expected["technical_revocations_district_average_color"] = default_color
        expected["technical_revocations_state_average"] = "2.095"

        # [improved] Less crime revocations than district and state averages
        # [improved] Lower district average than state average
        expected["crime_revocations"] = "2"
        expected["crime_revocations_color"] = default_color
        expected["crime_revocations_district_average"] = "3.353"
        expected["crime_revocations_district_average_color"] = default_color
        expected["crime_revocations_state_average"] = "3.542"

        # Higher absconsions than district or state average
        # higher district average than state average
        expected["absconsions"] = "2"
        expected["absconsions_color"] = red
        expected["absconsions_district_average"] = "0.22"
        expected["absconsions_district_average_color"] = red
        expected["absconsions_state_average"] = "0.14"

        expected["total_revocations"] = "2"
        expected["assessments_percent"] = "73"
        expected["overdue_assessments_goal_percent"] = "81"
        expected["assessments_goal_enabled"] = True
        expected["assessments_goal_met"] = False
        expected["facetoface_percent"] = "N/A"
        expected["overdue_facetoface_goal_percent"] = "N/A"
        expected["facetoface_goal_enabled"] = False
        expected["facetoface_goal_met"] = False

        expected["pos_discharges_label"] = "Successful&nbsp;Case Completions"
        expected["earned_discharges_label"] = "Early Discharge"
        expected["supervision_downgrades_label"] = "Supervision Downgrades"
        expected["total_revocations_label"] = "Revocations"
        expected["absconsions_label"] = "Absconsions"

        expected["display_congratulations"] = "inherit"
        expected["congratulations_text"] = (
            "You improved from last month across 4 metrics and out-performed other "
            "officers like you across 5 metrics."
        )
        expected[
            "learn_more_link"
        ] = "https://docs.google.com/document/d/1kgG5LiIrFQaBupHYfoIwo59TCmYH5f_aIpRzGrtOkhU/edit#heading=h.r6s5tyc7ut6c"

        for key, value in expected.items():
            if key not in actual:
                print(f"Missing key: {key}")
            self.assertTrue(key in actual)

        for key, value in actual.items():
            self.assertEqual(expected[key], value, f"key = {key}")

        # Testing an instance where some metrics are not populated
        expected[
            "static_image_path"
        ] = "http://123.456.7.8/US_PA/po_monthly_report/static"
        del expected["earned_discharges_color"]
        del expected["earned_discharges_change"]
        del expected["earned_discharges_change_color"]
        del expected["earned_discharges_district_average_color"]
        del expected["earned_discharges_label"]

        expected["earned_discharges"] = 0
        expected["earned_discharges_last_month"] = 0
        expected["earned_discharges_district_average"] = 0.0
        expected["earned_discharges_state_average"] = 0.0

        expected["congratulations_text"] = (
            "You improved from last month across 4 metrics and out-performed other "
            "officers like you across 4 metrics."
        )

        recipient = self.recipient.create_derived_recipient(
            {
                "earned_discharges": 0,
                "earned_discharges_last_month": 0,
                "earned_discharges_district_average": 0.0,
                "earned_discharges_state_average": 0.0,
            }
        )
        context = PoMonthlyReportContext(StateCode.US_PA, recipient)
        actual = context.get_prepared_data()

        for key, value in expected.items():
            if key not in actual:
                print(f"Missing key: {key}")
            self.assertTrue(key in actual)

        for key, value in actual.items():
            self.assertEqual(expected[key], value, f"key = {key}")

    def test_compliance_goals_enabled(self) -> None:
        """Test that compliance goals are enabled if below baseline threshold"""
        recipient_data = {
            "assessments_percent": 90,
            "facetoface_percent": 85,
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()

        self.assertTrue(actual["assessments_goal_enabled"])
        self.assertTrue(actual["facetoface_goal_enabled"])

        self.assertFalse(actual["assessments_goal_met"])
        self.assertFalse(actual["facetoface_goal_met"])

    def test_compliance_goals_disabled(self) -> None:
        """Test that compliance goals are disabled if above baseline threshold"""
        recipient_data = {
            "assessments_percent": 95,
            "facetoface_percent": 90,
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()

        self.assertFalse(actual["assessments_goal_enabled"])
        self.assertFalse(actual["facetoface_goal_enabled"])

        self.assertTrue(actual["assessments_goal_met"])
        self.assertTrue(actual["facetoface_goal_met"])

    def test_compliance_goals_unavailable(self) -> None:
        """Test that compliance goals are disabled if metrics are unavailable"""
        recipient_data = {
            "assessments_percent": "NaN",
            "facetoface_percent": "NaN",
        }
        recipient = self.recipient.create_derived_recipient(recipient_data)
        context = PoMonthlyReportContext(StateCode.US_ID, recipient)
        actual = context.get_prepared_data()

        self.assertFalse(actual["assessments_goal_enabled"])
        self.assertFalse(actual["facetoface_goal_enabled"])

        self.assertFalse(actual["assessments_goal_met"])
        self.assertFalse(actual["facetoface_goal_met"])
