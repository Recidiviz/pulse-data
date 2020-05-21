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

"""Report context for the PO Monthly Report.

The PO Monthly Report is a report for parole and probation officers with feedback on measures they have taken to
improve individual outcomes. It aims to promote and increase the usage of measures such as early discharges, and
decrease the usage of measures such as revocations.
"""

import copy
import json
from typing import List

from context_utils import singular_or_plural, month_number_to_name, round_float_value_to_int
import email_reporting_utils as utils
from report_context import ReportContext


class PoMonthlyReportContext(ReportContext):
    """Report context for the PO Monthly Report."""

    def __init__(self, state_code: str, recipient_data: dict):
        super().__init__(state_code, recipient_data)

    def get_report_type(self) -> str:
        return "po_monthly_report"

    def has_chart(self) -> bool:
        return False

    def prepare_for_generation(self) -> dict:
        """Executes PO Monthly Report data preparation."""
        self.properties = json.loads(utils.load_string_from_storage(
            utils.get_data_storage_bucket_name(),
            utils.get_properties_filename(self.state_code, self.get_report_type())
        ))

        self.prepared_data = copy.deepcopy(self.recipient_data)

        self.prepared_data["static_image_path"] = utils.get_static_image_path(self.state_code, self.get_report_type())
        self._convert_month_to_name("review_month")

        self._color_change_text("pos_discharges_change", "pos_discharges_change_color",
                                self.properties["increase"], self.properties["decrease"], True)
        self._color_change_text("earned_discharges_change", "earned_discharges_change_color",
                                self.properties["increase"], self.properties["decrease"], True)

        self._color_change_text("technical_revocations_change", "technical_revocations_change_color",
                                self.properties["increase"], self.properties["decrease"], False)
        self._color_change_text("absconsions_change", "absconsions_change_color",
                                self.properties["increase"], self.properties["decrease"], False)
        self._color_change_text("crime_revocations_change", "crime_revocations_change_color",
                                self.properties["increase"], self.properties["decrease"], False)

        self._color_change_text("assessment_percent_change", "assessment_percent_change_color",
                                self.properties["percent_increase"], self.properties["percent_decrease"], True)
        self._color_change_text("facetoface_percent_change", "facetoface_percent_change_color",
                                self.properties["percent_increase"], self.properties["percent_decrease"], True)
        self._color_change_text("home_visits_percent_change", "home_visits_percent_change_color",
                                self.properties["percent_increase"], self.properties["percent_decrease"], True)

        self._choose_comparison_icon("pos_discharges_icon",
                                     "pos_discharges",
                                     "pos_discharges_district_average",
                                     "pos_discharges_state_average",
                                     "pos_discharges_icon_great",
                                     "pos_discharges_icon_good",
                                     "pos_discharges_icon_low")

        self._choose_comparison_icon("earned_discharges_icon",
                                     "earned_discharges",
                                     "earned_discharges_district_average",
                                     "earned_discharges_state_average",
                                     "earned_discharges_icon_great",
                                     "earned_discharges_icon_good",
                                     "earned_discharges_icon_low")

        self._earned_discharges_tip()
        self._singular_or_plural_labels()
        self._round_float_values(['assessment_percent', 'home_visits_percent', 'facetoface_percent'])

        return self.prepared_data

    def _singular_or_plural_labels(self):
        """Ensures that each of the given labels will be made singular or plural, based on the value it is labelling."""
        singular_or_plural(self.prepared_data, "pos_discharges", "pos_discharges_label",
                           "Positive&nbsp;Discharge", "Positive&nbsp;Discharges")

        singular_or_plural(self.prepared_data, "earned_discharges", "earned_discharges_label",
                           "Earned Discharge Filed", "Earned Discharges Filed")

        singular_or_plural(self.prepared_data, "technical_revocations", "technical_revocations_label",
                           "Technical Revocation", "Technical Revocations")

        singular_or_plural(self.prepared_data, "absconsions", "absconsions_label", "Absconsion", "Absconsions")

        singular_or_plural(self.prepared_data, "crime_revocations", "crime_revocations_label",
                           "New Crime Revocation", "New Crime Revocations")

        singular_or_plural(self.prepared_data, "assessments", "assessments_label",
                           "Risk Assessment", "Risk Assessments")

        singular_or_plural(self.prepared_data, "home_visits", "home_visits_label", "Home Visit", "Home Visits")

    def _convert_month_to_name(self, month_key: str):
        """Converts the number at the given key, representing a calendar month, into the name of that month."""
        month_number = self.recipient_data[month_key]
        month_name = month_number_to_name(month_number)
        self.prepared_data[month_key] = month_name

    def _round_float_values(self, float_keys: List[str]):
        """Rounds all of the values with the given keys to their nearest integer values for display."""
        for float_key in float_keys:
            self.prepared_data[float_key] = round_float_value_to_int(self.recipient_data[float_key])

    def _choose_comparison_icon(self,
                                icon_to_choose: str,
                                you_key: str, district_key: str, state_key: str,
                                great_icon: str, good_icon: str, low_icon: str):
        """Decides which icon to show in the one of the report sections that compares an officer to district and
        state averages, for positive values where higher numbers are preferred, e.g. early discharges.

            Great - value > district and state averages
            Good - value >= district or state averages
            Low - value < district and state averages
        """

        you = float(self.recipient_data[you_key])
        district = float(self.recipient_data[district_key])
        state = float(self.recipient_data[state_key])

        if you > district and you > state:
            self.prepared_data[icon_to_choose] = self.properties[great_icon]
        elif you >= district or you >= state:
            self.prepared_data[icon_to_choose] = self.properties[good_icon]
        else:
            self.prepared_data[icon_to_choose] = self.properties[low_icon]

    def _earned_discharges_tip(self):
        """Sets the title and text of the earned discharges tip."""
        earned_discharges = int(self.recipient_data["earned_discharges"])

        if earned_discharges <= 0:
            self.prepared_data["earned_discharges_tip_title"] = self.properties["earned_discharges_tip_title_0"]
            self.prepared_data["earned_discharges_tip_text"] = self.properties["earned_discharges_tip_text_0"]
        elif earned_discharges == 1:
            self.prepared_data["earned_discharges_tip_title"] = self.properties["earned_discharges_tip_title_1"]
            self.prepared_data["earned_discharges_tip_text"] = self.properties["earned_discharges_tip_text_1"]
        elif earned_discharges == 2:
            self.prepared_data["earned_discharges_tip_title"] = self.properties["earned_discharges_tip_title_2"]
            self.prepared_data["earned_discharges_tip_text"] = self.properties["earned_discharges_tip_text_2"]
        elif earned_discharges >= 3:
            self.prepared_data["earned_discharges_tip_title"] = self.properties["earned_discharges_tip_title_3"]
            self.prepared_data["earned_discharges_tip_text"] = self.properties["earned_discharges_tip_text_3"]

    def _color_change_text(self,
                           base_key: str,
                           color_key: str,
                           increase_text: str,
                           decrease_text: str,
                           blue_on_increase: bool):
        """Sets text and color properties for the given keys based on the value of the base key and whether we want
        the coloring to apply on increase or on decrease. Also ensures that all displayed values are rounded to the
        nearest integer."""
        str_value = self.recipient_data[base_key]
        float_value = float(str_value)
        int_value = int(round(float_value))

        if blue_on_increase:
            if float_value >= 0:
                self.prepared_data[color_key] = self.properties["blue"]
                self.prepared_data[base_key] = str(int_value) + increase_text
            else:
                self.prepared_data[color_key] = self.properties["red"]
                self.prepared_data[base_key] = str(abs(int_value)) + decrease_text
        else:
            if float_value <= 0:
                self.prepared_data[color_key] = self.properties["blue"]
                self.prepared_data[base_key] = str(abs(int_value)) + decrease_text
            else:
                self.prepared_data[color_key] = self.properties["red"]
                self.prepared_data[base_key] = str(int_value) + increase_text
