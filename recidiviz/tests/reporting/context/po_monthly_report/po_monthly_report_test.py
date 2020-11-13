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

import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch
import json

from recidiviz.tests.ingest.fixtures import as_string_from_relative_path
from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext

# The path is defined relative to recidiviz/tests/ingest, hence the specific path structure below:
_PROPERTIES = as_string_from_relative_path(
    "../../reporting/context/po_monthly_report/properties.json")

FIXTURE_FILE = 'po_monthly_report_data_fixture.json'


class PoMonthlyReportContextTests(TestCase):
    """Tests for po_monthly_report/context.py."""

    def setUp(self) -> None:
        with open(os.path.join(os.path.dirname(__file__), FIXTURE_FILE)) as fixture_file:
            self.recipient_data = json.loads(fixture_file.read())
        project_id = 'RECIDIVIZ_TEST'
        cdn_static_ip = '123.456.7.8'
        test_secrets = {
            'po_report_cdn_static_IP': cdn_static_ip
        }

        self.get_secret_patcher = patch('recidiviz.utils.secrets.get_secret')
        self.project_id_patcher = patch('recidiviz.utils.metadata.project_id')

        self.get_secret_patcher.start().side_effect = test_secrets.get
        self.project_id_patcher.start().return_value = project_id

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.project_id_patcher.stop()

    def test_get_report_type(self) -> None:
        context = PoMonthlyReportContext('us_va', self.recipient_data)
        self.assertEqual('po_monthly_report', context.get_report_type())

    def test_congratulations_text_only_improved_from_last_month(self) -> None:
        """Test that the congratulations text looks correct if only the goals were met for the last month."""
        recipient_data = deepcopy(self.recipient_data)
        recipient_data["pos_discharges_state_average"] = "6"
        recipient_data["pos_discharges_district_average"] = "6"
        recipient_data["earned_discharges"] = "0"
        recipient_data["technical_revocations"] = "3"
        recipient_data["crime_revocations"] = "4"
        context = PoMonthlyReportContext('US_VA', recipient_data)
        actual = context.get_prepared_data()
        self.assertEqual("You improved from last month for 1 metric.", actual["congratulations_text"])

    def test_congratulations_text_only_outperformed_region_averages(self) -> None:
        """Test that the congratulations text looks correct if only the region averages were outperformed."""
        recipient_data = deepcopy(self.recipient_data)
        recipient_data["pos_discharges"] = "0"
        recipient_data["earned_discharges"] = "2"
        recipient_data["technical_revocations"] = "3"
        recipient_data["crime_revocations"] = "4"
        context = PoMonthlyReportContext('US_VA', recipient_data)
        actual = context.get_prepared_data()
        self.assertEqual("You out-performed other officers like you for 1 metric.", actual["congratulations_text"])

    def test_congratulations_text_empty(self) -> None:
        """Test that the congratulations text is an empty string and the display is none if neither
            metric goals were met
        """
        recipient_data = deepcopy(self.recipient_data)
        recipient_data["pos_discharges"] = "0"
        recipient_data["earned_discharges"] = "0"
        recipient_data["technical_revocations"] = "3"
        recipient_data["crime_revocations"] = "4"
        context = PoMonthlyReportContext('US_VA', recipient_data)
        actual = context.get_prepared_data()
        self.assertEqual("", actual["congratulations_text"])
        self.assertEqual("none", actual["display_congratulations"])

    def test_prepare_for_generation(self) -> None:
        context = PoMonthlyReportContext('US_VA', self.recipient_data)
        actual = context.get_prepared_data()
        red = "#A43939"
        gray = "#7D9897"
        default_color = "#00413E"

        expected = deepcopy(self.recipient_data)
        expected["static_image_path"] = "http://123.456.7.8/US_VA/po_monthly_report/static"
        expected["review_month"] = "May"
        expected["greeting"] = "Hey there, Christopher!"

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
        expected["assessment_percent"] = "73"
        expected["facetoface_percent"] = "45"

        expected["pos_discharges_label"] = "Successful&nbsp;Case Completions"
        expected["earned_discharges_label"] = "Early Discharge"
        expected["total_revocations_label"] = "Revocations"
        expected["absconsions_label"] = "Absconsions"
        expected["assessments_label"] = "Risk Assessments"
        expected["facetoface_label"] = "Face-to-Face Contacts"

        expected["display_congratulations"] = "inherit"
        expected["congratulations_text"] = "You improved from last month across 3 metrics and out-performed other " \
                                           "officers like you across 4 metrics."

        for key, value in expected.items():
            if key not in actual:
                print(f"Missing key: {key}")
            self.assertTrue(key in actual)

        for key, value in actual.items():
            self.assertEqual(expected[key], value, f'key = {key}')
