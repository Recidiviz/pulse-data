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

#pylint: disable=import-outside-toplevel

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from recidiviz.tests.ingest.fixtures import as_string_from_relative_path
from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext

# The path is defined relative to recidiviz/tests/ingest, hence the specific path structure below:
_PROPERTIES = as_string_from_relative_path(
    "../../reporting/context/po_monthly_report/properties.json")


class PoMonthlyReportContextTests(TestCase):
    """Tests for po_monthly_report/context.py."""

    @classmethod
    def setUpClass(cls):
        cls.PoMonthlyReportContext = PoMonthlyReportContext

    def test_get_report_type(self):
        context = self.PoMonthlyReportContext('us_va', {})
        self.assertEqual('po_monthly_report', context.get_report_type())


    @patch('recidiviz.reporting.email_reporting_utils.load_string_from_storage')
    @patch('recidiviz.utils.secrets.get_secret')
    @patch('recidiviz.utils.metadata.project_id')
    def test_prepare_for_generation(self, mock_project_id, mock_secret, mock_load_string):
        project_id = 'RECIDIVIZ_TEST'
        cdn_static_ip = '123.456.7.8'
        mock_load_string.return_value = _PROPERTIES
        mock_project_id.return_value = project_id

        test_secrets = {
            'po_report_cdn_static_IP': cdn_static_ip
        }
        mock_secret.side_effect = test_secrets.get

        recipient_data = {"state_code": "US_VA",
                          "email_address": "letter@kenny.ca",
                          "review_month": "5",
                          "pos_discharges": "5",
                          "pos_discharges_district_average": "1.1",
                          "pos_discharges_state_average": "0.9",
                          "earned_discharges": "1",
                          "earned_discharges_change": "-2",
                          "earned_discharges_district_average": "0.8",
                          "earned_discharges_state_average": "1.6",
                          "technical_revocations": "1",
                          "technical_revocations_change": "0",
                          "absconsions": "2",
                          "absconsions_change": "1",
                          "crime_revocations": "2",
                          "crime_revocations_change": "-1",
                          "assessments": "15",
                          "assessment_percent": "73.3",
                          "assessment_percent_last_month": "10.1",
                          "facetoface": "30",
                          "facetoface_percent": "45.3",
                          "facetoface_percent_last_month": "0.2"}

        context = self.PoMonthlyReportContext('US_VA', recipient_data)
        actual = context.get_prepared_data()

        expected = deepcopy(recipient_data)
        expected["static_image_path"] = "http://123.456.7.8/US_VA/po_monthly_report/static"
        expected["review_month"] = "May"

        expected["earned_discharges_change"] = "2 fewer than last month"
        expected["earned_discharges_change_color"] = "#c77d7d"

        expected["technical_revocations_change"] = "Same as last month"
        expected["technical_revocations_change_color"] = "#7d9d9c"

        expected["absconsions_change"] = "1 more than last month"
        expected["absconsions_change_color"] = "#c77d7d"

        expected["crime_revocations_change"] = "1 fewer than last month"
        expected["crime_revocations_change_color"] = "#7d9d9c"

        expected["assessment_percent"] = "73"
        expected["assessment_percent_last_month"] = "10"

        expected["facetoface_percent"] = "45"
        expected["facetoface_percent_last_month"] = "0"

        expected["pos_discharges_icon"] = "ic_case-completions_great.png"
        expected["earned_discharges_icon"] = "ic_early-discharge_good.png"

        expected["earned_discharges_tip_title"] = "Nice work!"
        expected["earned_discharges_tip_text"] = \
            "Early discharge is one way to reward individuals succeeding on supervision. Could anyone else be eligible?"

        expected["pos_discharges_label"] = "Successful&nbsp;Case Completions"
        expected["pos_discharges_description"] = "More completions than the state and district average"
        expected["earned_discharges_label"] = "Early Discharge Filed"
        expected["technical_revocations_label"] = "Technical-Only Revocation"
        expected["absconsions_label"] = "Absconders"
        expected["crime_revocations_label"] = "Revocations for New Criminal Charges"
        expected["assessments_label"] = "Risk Assessments"

        for key, value in actual.items():
            self.assertEqual(value, expected[key], f'key = {key}')
