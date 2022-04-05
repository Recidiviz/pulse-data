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

import os
import copy
import json
from typing import List

from recidiviz.reporting.context.context_utils import singular_or_plural, month_number_to_name, \
    round_float_value_to_int, round_float_value_to_number_of_digits, format_greeting
import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.reporting.context.report_context import ReportContext

_AVERAGE_VALUES_SIGNIFICANT_DIGITS = 3

_METRICS_IMPROVE_ON_INCREASE = [
    "pos_discharges",
    "pos_discharges_district_average",
    "pos_discharges_state_average",
    "earned_discharges",
    "earned_discharges_district_average",
    "earned_discharges_state_average",
]

_AVERAGE_METRICS_FOR_DISPLAY = [
    "pos_discharges_district_average",
    "pos_discharges_state_average",
    "earned_discharges_district_average",
    "earned_discharges_state_average",
    "technical_revocations_district_average",
    "technical_revocations_state_average",
    "crime_revocations_district_average",
    "crime_revocations_state_average",
    "absconsions_district_average",
    "absconsions_state_average",
]

_BASE_METRICS_FOR_DISPLAY = ["pos_discharges", "earned_discharges", "technical_revocations", "crime_revocations",
                             "absconsions"]

_ALL_METRICS_FOR_DISPLAY = _BASE_METRICS_FOR_DISPLAY + _AVERAGE_METRICS_FOR_DISPLAY

_ALL_LAST_MONTH_METRICS = [
    "pos_discharges_last_month",
    "earned_discharges_last_month",
    "technical_revocations_last_month",
    "crime_revocations_last_month",
    "absconsions_last_month",
]

_ALL_REQUIRED_RECIPIENT_DATA_FIELDS = _ALL_METRICS_FOR_DISPLAY + _ALL_LAST_MONTH_METRICS + [
    "officer_external_id",
    "state_code",
    "district",
    "email_address",
    "officer_given_name",
    "review_month",
    "assessments",
    "assessment_percent",
    "facetoface",
    "facetoface_percent"
]


class PoMonthlyReportContext(ReportContext):
    """Report context for the PO Monthly Report."""

    def __init__(self, state_code: str, recipient_data: dict):
        self._validate_recipient_data_has_expected_fields(recipient_data)
        super().__init__(state_code, recipient_data)
        with open(self.get_properties_filepath()) as properties_file:
            self.properties = json.loads(properties_file.read())

    @staticmethod
    def _validate_recipient_data_has_expected_fields(recipient_data: dict) -> None:
        for expected_key in _ALL_REQUIRED_RECIPIENT_DATA_FIELDS:
            if expected_key not in recipient_data.keys():
                raise KeyError(f"Expected key [{expected_key}] not found in recipient_data.")

    def get_report_type(self) -> str:
        return "po_monthly_report"

    def get_properties_filepath(self) -> str:
        """Returns path to the properties.json, assumes it is in the same directory as the context."""
        return os.path.join(os.path.dirname(__file__), 'properties.json')

    def get_html_template_filepath(self) -> str:
        """Returns path to the template.html file, assumes it is in the same directory as the context."""
        return os.path.join(os.path.dirname(__file__), 'template.html')

    def prepare_for_generation(self) -> dict:
        """Executes PO Monthly Report data preparation."""
        self.prepared_data = copy.deepcopy(self.recipient_data)

        self.prepared_data["static_image_path"] = utils.get_static_image_path(self.state_code, self.get_report_type())

        self.prepared_data["greeting"] = format_greeting(self.recipient_data["officer_given_name"])

        self._convert_month_to_name("review_month")

        self._set_base_metric_color(_BASE_METRICS_FOR_DISPLAY)
        self._set_averages_metric_color(_AVERAGE_METRICS_FOR_DISPLAY)

        self._set_month_to_month_change_metrics(["pos_discharges", "earned_discharges"])

        self._set_congratulations_section()
        self._set_total_revocations()
        self._singular_or_plural_section_headers()
        self._round_float_values_to_ints(['assessment_percent', 'facetoface_percent'])
        self._round_float_values_to_number_of_digits(_AVERAGE_METRICS_FOR_DISPLAY,
                                                     number_of_digits=_AVERAGE_VALUES_SIGNIFICANT_DIGITS)

        return self.prepared_data

    @staticmethod
    def _metric_improved(metric_key: str, metric_value: float, comparison_value: float) -> bool:
        """Returns True if the value improved when compared to the comparison_value"""
        change_value = float(metric_value - comparison_value)
        if metric_key in _METRICS_IMPROVE_ON_INCREASE:
            return change_value > 0
        return change_value < 0

    def _get_num_metrics_improved_from_last_month(self) -> int:
        """Returns the number of metrics that performed better than last month"""
        num_metrics_met_goal = 0
        for metric_key in _BASE_METRICS_FOR_DISPLAY:
            metric_value = float(self.recipient_data[metric_key])
            last_month_value = float(self.recipient_data[f"{metric_key}_last_month"])
            if self._metric_improved(metric_key, metric_value, last_month_value):
                num_metrics_met_goal += 1

        return num_metrics_met_goal

    def _improved_over_state_average(self, metric_key: str) -> bool:
        state_average = float(self.recipient_data[f"{metric_key}_state_average"])
        metric_value = float(self.recipient_data[metric_key])
        return self._metric_improved(metric_key, metric_value, state_average)

    def _improved_over_district_average(self, metric_key: str) -> bool:
        district_average = float(self.recipient_data[f"{metric_key}_district_average"])
        metric_value = float(self.recipient_data[metric_key])
        return self._metric_improved(metric_key, metric_value, district_average)

    def _get_num_metrics_outperformed_region_averages(self) -> int:
        """Returns the number of metrics that performed better than the either district or state averages"""
        num_metrics_outperformed_region_averages = 0
        for metric_key in _BASE_METRICS_FOR_DISPLAY:
            if self._improved_over_district_average(metric_key) or self._improved_over_state_average(metric_key):
                num_metrics_outperformed_region_averages += 1

        return num_metrics_outperformed_region_averages

    def _set_total_revocations(self) -> None:
        self.prepared_data['total_revocations'] = str(int(self.recipient_data['technical_revocations']) + \
                                                      int(self.recipient_data['crime_revocations']))

    def _get_metric_text_singular_or_plural(self, metric_value: int) -> str:
        metric_text = 'metric_text_singular' if metric_value == 1 else 'metric_text_plural'
        return self.properties[metric_text].format(metric=metric_value)

    def _set_congratulations_section(self) -> None:
        """Set the text and the display property for the Congratulations section. Sets the display property to
            'inherit' if the metrics met goals, or 'none' if no goals were met and it should be hidden."""
        num_metrics_met_goal = self._get_num_metrics_improved_from_last_month()
        num_metrics_outperformed_region_averages = self._get_num_metrics_outperformed_region_averages()
        is_meeting_goals = num_metrics_met_goal > 0 or num_metrics_outperformed_region_averages > 0

        self.prepared_data['display_congratulations'] = 'inherit' if is_meeting_goals else 'none'

        met_goal_text = self.properties['met_goal_text'].format(
            metric_text=self._get_metric_text_singular_or_plural(num_metrics_met_goal)
        )
        outperformed_region_averages_text = self.properties['outperformed_region_averages_text'].format(
            metric_text=self._get_metric_text_singular_or_plural(num_metrics_outperformed_region_averages)
        )

        if num_metrics_met_goal > 0 and num_metrics_outperformed_region_averages > 0:
            congratulations_text = f"You {met_goal_text} and {outperformed_region_averages_text}."

        elif num_metrics_met_goal > 0:
            congratulations_text = f"You {met_goal_text}."

        elif num_metrics_outperformed_region_averages > 0:
            congratulations_text = f"You {outperformed_region_averages_text}."

        else:
            congratulations_text = ""

        self.prepared_data['congratulations_text'] = congratulations_text

    def _set_averages_metric_color(self, metrics: List[str]) -> None:
        """Sets the color for the district averages displayed in each section. Color will be the red if it didn't
            improve compared to the state average, otherwise it will be the default color.
        """
        for metric_key in metrics:
            if metric_key.endswith("state_average"):
                # Only set the color for district averages
                continue

            color_key = f'{metric_key}_color'
            metric_value = float(self.recipient_data[metric_key])
            state_average = float(self.recipient_data[metric_key.replace("district_average", "state_average")])
            metric_improved = self._metric_improved(metric_key, metric_value, state_average)

            if metric_improved:
                self.prepared_data[color_key] = self.properties['default_color']
            else:
                self.prepared_data[color_key] = self.properties['red']

    def _set_base_metric_color(self, metrics: List[str]) -> None:
        """Sets the color for the base metrics displayed in each section. Color will be the red if it didn't
            improve compared to either the district or state averages, otherwise it will be the default color.
        """
        for metric_key in metrics:
            color_key = f'{metric_key}_color'

            if self._improved_over_district_average(metric_key) and self._improved_over_state_average(metric_key):
                self.prepared_data[color_key] = self.properties['default_color']
            else:
                self.prepared_data[color_key] = self.properties['red']

    def _set_month_to_month_change_metrics(self, metrics: List[str]) -> None:
        """Sets the value and color for metrics that display the month-to-month change value. If the metric improved
            from last month's value, the color will be gray, otherwise it will be red.
        """
        for metric_key in metrics:
            metric_improves_on_increase = metric_key in _METRICS_IMPROVE_ON_INCREASE
            base_key = f'{metric_key}_change'
            color_key = f'{metric_key}_change_color'
            metric_value = float(self.recipient_data[metric_key])
            last_month_value = float(self.recipient_data[f"{metric_key}_last_month"])
            change_value = float(metric_value - last_month_value)

            self._set_color_and_text_for_change_metrics(change_value,
                                                        base_key,
                                                        color_key,
                                                        metric_improves_on_increase)

    def _singular_or_plural_section_headers(self) -> None:
        """Ensures that each of the given labels will be made singular or plural, based on the value it is labelling."""
        singular_or_plural(self.prepared_data, "pos_discharges", "pos_discharges_label",
                           "Successful&nbsp;Case Completion", "Successful&nbsp;Case Completions")

        singular_or_plural(self.prepared_data, "earned_discharges", "earned_discharges_label", "Early Discharge",
                           "Early Discharges")

        singular_or_plural(self.prepared_data, "total_revocations", "total_revocations_label", "Revocation",
                           "Revocations")

        singular_or_plural(self.prepared_data, "absconsions", "absconsions_label", "Absconsion", "Absconsions")

        singular_or_plural(self.prepared_data, "assessments", "assessments_label", "Risk Assessment",
                           "Risk Assessments")

        singular_or_plural(self.prepared_data, "facetoface", "facetoface_label", "Face-to-Face Contact",
                           "Face-to-Face Contacts")

    def _convert_month_to_name(self, month_key: str) -> None:
        """Converts the number at the given key, representing a calendar month, into the name of that month."""
        month_number = self.recipient_data[month_key]
        month_name = month_number_to_name(month_number)
        self.prepared_data[month_key] = month_name

    def _round_float_values_to_ints(self, float_keys: List[str]) -> None:
        """Rounds all of the values with the given keys to their nearest integer values for display."""
        for float_key in float_keys:
            self.prepared_data[float_key] = round_float_value_to_int(self.recipient_data[float_key])

    def _round_float_values_to_number_of_digits(self, float_keys: List[str], number_of_digits: int) -> None:
        """Rounds all of the values with the given keys to the given number of digits."""
        for float_key in float_keys:
            self.prepared_data[float_key] = round_float_value_to_number_of_digits(
                self.recipient_data[float_key], number_of_digits)

    def _set_color_and_text_for_change_metrics(self,
                                               change_value: float,
                                               base_key: str,
                                               color_key: str,
                                               metric_improves_on_increase: bool) -> None:
        """Sets text and color properties for the given value and key based on the value and whether we want
        the coloring to apply on increase or on decrease. Also ensures that all displayed values are rounded to the
        nearest integer."""
        int_value = int(round(change_value))
        gray = self.properties["gray"]
        red = self.properties["red"]

        increase_text = self.properties["increase"]
        decrease_text = self.properties["decrease"]
        encouragement_text = self.properties["encouragement"]

        if metric_improves_on_increase:
            if change_value >= 0:
                self.prepared_data[color_key] = gray
                self.prepared_data[base_key] = f"{str(int_value)} {increase_text} {encouragement_text}"
            else:
                self.prepared_data[color_key] = red
                self.prepared_data[base_key] = f"{str(abs(int_value))} {decrease_text}"
        else:
            if change_value <= 0:
                self.prepared_data[color_key] = gray
                self.prepared_data[base_key] = f"{str(abs(int_value))} {decrease_text} {encouragement_text}"
            else:
                self.prepared_data[color_key] = red
                self.prepared_data[base_key] = f"{str(int_value)} {increase_text}"

        # If it's the same (a float which rounds up or down to the integer 0 is effectively "the same"),
        # then we override the text and color
        if change_value == 0 or int_value == 0:
            self.prepared_data[color_key] = gray
            self.prepared_data[base_key] = self.properties["same"]
