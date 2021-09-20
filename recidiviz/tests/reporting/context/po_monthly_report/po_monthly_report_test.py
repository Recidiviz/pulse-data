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
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import (
    EARNED_DISCHARGES,
    POS_DISCHARGES,
    SUPERVISION_DOWNGRADES,
    ReportType,
)
from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext
from recidiviz.reporting.recipient import Recipient
from recidiviz.tests.reporting.context.po_monthly_report.po_monthly_report_expected_data import (
    expected_us_id,
    expected_us_pa,
)

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
        """Test that the message body is overridden by the message_body_override"""
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
        """Smoke test for the entire template context with fixture data."""
        # these dicts are big, need to see the whole thing if something fails
        self.maxDiff = None

        self.assertEqual(
            expected_us_id,
            PoMonthlyReportContext(StateCode.US_ID, self.recipient).get_prepared_data(),
        )

        pa_recipient = self.recipient.create_derived_recipient(
            {"state_code": StateCode.US_PA.value}
        )
        self.assertEqual(
            expected_us_pa,
            PoMonthlyReportContext(StateCode.US_PA, pa_recipient).get_prepared_data(),
        )

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

    def test_completions(self) -> None:
        """Test that completions context is populated according to input data."""

        happy_path_data = PoMonthlyReportContext(
            StateCode.US_ID, self.recipient
        ).get_prepared_data()[POS_DISCHARGES]

        self.assertIn(
            "273", happy_path_data["main_text"].format(happy_path_data["total"])
        )
        self.assertEqual(
            happy_path_data["supplemental_text"],
            "5 from your caseload",
        )
        self.assertEqual(
            happy_path_data["action_table"],
            [("Hansen, Linet (105)", "June 7"), ("Cortes, Rebekah (142)", "June 18")],
        )

        no_caseload_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient({POS_DISCHARGES: "0"}),
        ).get_prepared_data()[POS_DISCHARGES]
        self.assertEqual(
            no_caseload_data["supplemental_text"],
            "38 from your district",
        )

        no_local_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient(
                {POS_DISCHARGES: "0", f"{POS_DISCHARGES}_district_total": 0}
            ),
        ).get_prepared_data()[POS_DISCHARGES]
        self.assertIsNone(no_local_data["supplemental_text"])

        no_action_items_data = no_local_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient(
                {"upcoming_release_date_clients": []}
            ),
        ).get_prepared_data()[POS_DISCHARGES]
        self.assertIsNone(no_action_items_data["action_table"])

    def test_downgrades(self) -> None:
        """Test that downgrades context is populated according to input data."""

        happy_path_data = PoMonthlyReportContext(
            StateCode.US_ID, self.recipient
        ).get_prepared_data()[SUPERVISION_DOWNGRADES]

        self.assertIn(
            "314", happy_path_data["main_text"].format(happy_path_data["total"])
        )
        self.assertEqual(
            happy_path_data["supplemental_text"],
            "5 from your caseload",
        )
        self.assertEqual(
            happy_path_data["action_table"],
            [
                ("Tonye Thompson (189472)", "Medium &rarr; Low"),
                ("Linet Hansen (47228)", "Medium &rarr; Low"),
                ("Rebekah Cortes (132878)", "High &rarr; Medium"),
                ("Taryn Berry (147872)", "High &rarr; Low"),
            ],
        )

        no_caseload_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient({SUPERVISION_DOWNGRADES: "0"}),
        ).get_prepared_data()[SUPERVISION_DOWNGRADES]
        self.assertEqual(
            no_caseload_data["supplemental_text"],
            "51 from your district",
        )

        no_local_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient(
                {
                    SUPERVISION_DOWNGRADES: "0",
                    f"{SUPERVISION_DOWNGRADES}_district_total": 0,
                }
            ),
        ).get_prepared_data()[SUPERVISION_DOWNGRADES]
        self.assertIsNone(no_local_data["supplemental_text"])

        no_action_items_data = no_local_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient({"mismatches": []}),
        ).get_prepared_data()[SUPERVISION_DOWNGRADES]
        self.assertIsNone(no_action_items_data["action_table"])

    def test_early_discharges(self) -> None:
        """Test that early discharge context is populated according to input data."""

        happy_path_data = PoMonthlyReportContext(
            StateCode.US_ID, self.recipient
        ).get_prepared_data()[EARNED_DISCHARGES]

        self.assertIn(
            "106", happy_path_data["main_text"].format(happy_path_data["total"])
        )
        self.assertEqual(
            happy_path_data["supplemental_text"],
            "1 from your caseload",
        )
        self.assertIsNone(happy_path_data["action_table"])

        no_caseload_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient({EARNED_DISCHARGES: "0"}),
        ).get_prepared_data()[EARNED_DISCHARGES]
        self.assertEqual(
            no_caseload_data["supplemental_text"],
            "18 from your district",
        )

        no_local_data = PoMonthlyReportContext(
            StateCode.US_ID,
            self.recipient.create_derived_recipient(
                {EARNED_DISCHARGES: "0", f"{EARNED_DISCHARGES}_district_total": 0}
            ),
        ).get_prepared_data()[EARNED_DISCHARGES]
        self.assertIsNone(no_local_data["supplemental_text"])
