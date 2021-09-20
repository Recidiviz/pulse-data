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

To generate a sample output for the PO Monthly Report email template, just run:

python -m recidiviz.reporting.context.po_monthly_report.context
"""
import copy
import os
from datetime import date
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader, Template

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.context_utils import (
    align_columns,
    format_date,
    format_greeting,
    format_name,
    format_violation_type,
    month_number_to_name,
    round_float_value_to_int,
    round_float_value_to_number_of_digits,
    singular_or_plural,
)
from recidiviz.reporting.context.po_monthly_report.constants import (
    CRIME_REVOCATIONS,
    DEFAULT_MESSAGE_BODY_KEY,
    EARNED_DISCHARGES,
    POS_DISCHARGES,
    SUPERVISION_DOWNGRADES,
    TECHNICAL_REVOCATIONS,
    TOTAL_REVOCATIONS,
    ReportType,
)
from recidiviz.reporting.context.po_monthly_report.state_utils.po_monthly_report_metrics_delegate_factory import (
    PoMonthlyReportMetricsDelegateFactory,
)
from recidiviz.reporting.context.po_monthly_report.types import DecarceralMetricContext
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.recipient import Recipient
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_AVERAGE_VALUES_SIGNIFICANT_DIGITS = 3


class PoMonthlyReportContext(ReportContext):
    """Report context for the PO Monthly Report."""

    def __init__(self, state_code: StateCode, recipient: Recipient):
        self.metrics_delegate = PoMonthlyReportMetricsDelegateFactory.build(
            state_code=state_code
        )
        super().__init__(state_code, recipient)
        self.recipient_data = recipient.data

        self.jinja_env = Environment(
            loader=FileSystemLoader(self._get_context_templates_folder())
        )

        self.state_name = state_code.get_state().name

    @property
    def attachment_template(self) -> Template:
        return self.jinja_env.get_template("po_monthly_report/attachment.txt.jinja2")

    def get_required_recipient_data_fields(self) -> List[str]:
        return self.metrics_delegate.required_recipient_data_fields

    def get_report_type(self) -> ReportType:
        return ReportType.POMonthlyReport

    def get_properties_filepath(self) -> str:
        """Returns path to the properties.json, assumes it is in the same directory as the context."""
        return os.path.join(os.path.dirname(__file__), "properties.json")

    @property
    def html_template(self) -> Template:
        return self.jinja_env.get_template("po_monthly_report/email.html.jinja2")

    def prepare_for_generation(self) -> dict:
        """Executes PO Monthly Report data preparation."""
        self.prepared_data = copy.deepcopy(self.recipient_data)

        self.prepared_data["static_image_path"] = utils.get_static_image_path(
            self.state_code, self.get_report_type()
        )
        self.prepared_data["greeting"] = format_greeting(
            self.recipient_data["officer_given_name"]
        )
        self.prepared_data["learn_more_link"] = self.properties["learn_more_link"]

        if message_override := self.recipient_data.get("message_body_override"):
            self.prepared_data["message_body"] = message_override
        else:
            self.prepared_data["message_body"] = self.properties[
                DEFAULT_MESSAGE_BODY_KEY
            ]

        self._convert_month_to_name("review_month")

        self._set_base_metric_color(self.metrics_delegate.client_outcome_metrics)
        self._set_averages_metric_color(
            self.metrics_delegate.average_metrics_for_display
        )

        self._set_congratulations_section()
        self._set_total_revocations()
        self._singular_or_plural_section_headers()
        self._round_float_values_to_ints(
            self.metrics_delegate.float_metrics_to_round_to_int
        )
        self._round_float_values_to_number_of_digits(
            self.metrics_delegate.average_metrics_for_display,
            number_of_digits=_AVERAGE_VALUES_SIGNIFICANT_DIGITS,
        )
        self._set_compliance_goals()
        self._prepare_attachment_content()

        for metric in self.metrics_delegate.decarceral_actions_metrics:
            self.prepared_data[metric] = getattr(self, f"_get_{metric}")()

        return self.prepared_data

    def _prepare_attachment_content(self) -> None:
        if not self._should_generate_attachment():
            self.prepared_data["attachment_content"] = None
            return

        self.prepared_data["attachment_content"] = self.attachment_template.render(
            self._prepare_attachment_data()
        )

    def _metric_improved(
        self, metric_key: str, metric_value: float, comparison_value: float
    ) -> bool:
        """Returns True if the value improved when compared to the comparison_value"""
        change_value = float(metric_value - comparison_value)
        if metric_key in self.metrics_delegate.metrics_improve_on_increase:
            return change_value > 0
        return change_value < 0

    def _get_num_metrics_improved_from_last_month(self) -> int:
        """Returns the number of metrics that performed better than last month"""
        num_metrics_met_goal = 0
        for metric_key in self.metrics_delegate.base_metrics_for_display:
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
        for metric_key in self.metrics_delegate.base_metrics_for_display:
            if self._improved_over_district_average(
                metric_key
            ) or self._improved_over_state_average(metric_key):
                num_metrics_outperformed_region_averages += 1

        return num_metrics_outperformed_region_averages

    def _set_total_revocations(self) -> None:
        self.prepared_data[TOTAL_REVOCATIONS] = str(
            int(self.recipient_data[TECHNICAL_REVOCATIONS])
            + int(self.recipient_data[CRIME_REVOCATIONS])
        )

    def _get_metric_text_singular_or_plural(self, metric_value: int) -> str:
        metric_text = (
            "metric_text_singular" if metric_value == 1 else "metric_text_plural"
        )
        return self.properties[metric_text].format(metric=metric_value)

    def _set_congratulations_section(self) -> None:
        """Set the text and the display property for the Congratulations section. Sets the display property to
        'inherit' if the metrics met goals, or 'none' if no goals were met and it should be hidden."""
        num_metrics_met_goal = self._get_num_metrics_improved_from_last_month()
        num_metrics_outperformed_region_averages = (
            self._get_num_metrics_outperformed_region_averages()
        )
        is_meeting_goals = (
            num_metrics_met_goal > 0 or num_metrics_outperformed_region_averages > 0
        )

        self.prepared_data["display_congratulations"] = (
            "inherit" if is_meeting_goals else "none"
        )

        met_goal_text = self.properties["met_goal_text"].format(
            metric_text=self._get_metric_text_singular_or_plural(num_metrics_met_goal)
        )
        outperformed_region_averages_text = self.properties[
            "outperformed_region_averages_text"
        ].format(
            metric_text=self._get_metric_text_singular_or_plural(
                num_metrics_outperformed_region_averages
            )
        )

        if num_metrics_met_goal > 0 and num_metrics_outperformed_region_averages > 0:
            congratulations_text = (
                f"You {met_goal_text} and {outperformed_region_averages_text}."
            )

        elif num_metrics_met_goal > 0:
            congratulations_text = f"You {met_goal_text}."

        elif num_metrics_outperformed_region_averages > 0:
            congratulations_text = f"You {outperformed_region_averages_text}."

        else:
            congratulations_text = ""

        self.prepared_data["congratulations_text"] = congratulations_text

    def _set_averages_metric_color(self, metrics: List[str]) -> None:
        """Sets the color for the district averages displayed in each section. Color will be the red if it didn't
        improve compared to the state average, otherwise it will be the default color.
        """
        for metric_key in metrics:
            if metric_key.endswith("state_average"):
                # Only set the color for district averages
                continue

            color_key = f"{metric_key}_color"
            metric_value = float(self.recipient_data[metric_key])
            state_average = float(
                self.recipient_data[
                    metric_key.replace("district_average", "state_average")
                ]
            )
            metric_improved = self._metric_improved(
                metric_key, metric_value, state_average
            )

            if metric_improved:
                self.prepared_data[color_key] = self.properties["default_color"]
            else:
                self.prepared_data[color_key] = self.properties["red"]

    def _set_base_metric_color(self, metrics: List[str]) -> None:
        """Sets the color for the base metrics displayed in each section. Color will be the red if it didn't
        improve compared to either the district or state averages, otherwise it will be the default color.
        """
        for metric_key in metrics:
            color_key = f"{metric_key}_color"

            if self._improved_over_district_average(
                metric_key
            ) and self._improved_over_state_average(metric_key):
                self.prepared_data[color_key] = self.properties["default_color"]
            else:
                self.prepared_data[color_key] = self.properties["red"]

    def _singular_or_plural_section_headers(self) -> None:
        """Ensures that each of the given labels will be made singular or plural, based on the value it is labelling."""
        allowed_metrics = self.metrics_delegate.singular_or_plural_metrics

        singular_or_plural(
            self.prepared_data,
            allowed_metrics,
            "total_revocations",
            "total_revocations_label",
            "Revocation",
            "Revocations",
        )

        singular_or_plural(
            self.prepared_data,
            allowed_metrics,
            "absconsions",
            "absconsions_label",
            "Absconsion",
            "Absconsions",
        )

    def _convert_month_to_name(self, month_key: str) -> None:
        """Converts the number at the given key, representing a calendar month, into the name of that month."""
        month_number = self.recipient_data[month_key]
        month_name = month_number_to_name(month_number)
        self.prepared_data[month_key] = month_name

    def _round_float_values_to_ints(self, float_keys: List[str]) -> None:
        """Rounds all of the values with the given keys to their nearest integer values for display."""
        for float_key in float_keys:
            self.prepared_data[float_key] = round_float_value_to_int(
                self.recipient_data[float_key]
            )

    def _round_float_values_to_number_of_digits(
        self, float_keys: List[str], number_of_digits: int
    ) -> None:
        """Rounds all of the values with the given keys to the given number of digits."""
        for float_key in float_keys:
            self.prepared_data[float_key] = round_float_value_to_number_of_digits(
                self.recipient_data[float_key], number_of_digits
            )

    def _set_compliance_goals(self) -> None:
        """Examines data to determine whether compliance goal prompts should be active
        and sets data properties accordingly."""
        for metric in self.metrics_delegate.compliance_action_metrics:
            goal_key = f"{metric}_goal_enabled"
            success_key = f"{metric}_goal_met"

            threshold = self.metrics_delegate.compliance_action_metric_goal_thresholds[
                metric
            ]
            compliance_pct = self.prepared_data[f"{metric}_percent"]

            if compliance_pct == "N/A":
                goal_active = False
                goal_met = False
            else:
                pct_as_num = int(compliance_pct)
                goal_active = pct_as_num < threshold
                goal_met = not goal_active

            self.prepared_data[goal_key] = goal_active
            self.prepared_data[success_key] = goal_met

    def _should_generate_attachment_section(self, clients_key: str) -> bool:
        return clients_key in self.recipient_data and self.recipient_data[clients_key]

    def _should_generate_attachment(self) -> bool:
        return any(
            self._should_generate_attachment_section(clients_key)
            for clients_key in self.metrics_delegate.client_fields
        )

    def _prepare_attachment_clients_tables(self) -> Dict[str, List[List[str]]]:
        """
        Prepares "tables" (2 dimensional arrays) of client details to inline into the email
        Returns: a dictionary, keyed by client "type", and its corresponding table
        """
        clients_by_type: Dict[str, List[List[str]]] = {}

        for clients_key in self.metrics_delegate.client_fields:
            clients_by_type[clients_key] = []

            if not self._should_generate_attachment_section(clients_key):
                continue

            for client in self.recipient_data[clients_key]:
                base_columns = [
                    f"[{client['person_external_id']}]",
                    # TODO(#7374): Revert to bracket access when current investigation
                    # is figured out.
                    format_name(client.get("full_name", "")),
                ]
                additional_columns = []

                if clients_key == "pos_discharges_clients":
                    additional_columns = [
                        f'Supervision completed on {format_date(client["successful_completion_date"])}'
                    ]
                elif clients_key == "earned_discharges_clients":
                    additional_columns = [
                        f'Discharge granted on {format_date(client["earned_discharge_date"])}'
                    ]
                elif clients_key == "supervision_downgrades_clients":
                    additional_columns = [
                        f'Supervision level downgraded on {format_date(client["latest_supervision_downgrade_date"])}'
                    ]
                elif clients_key == "absconsions_clients":
                    additional_columns = [
                        f'Absconsion reported on {format_date(client["absconsion_report_date"])}'
                    ]
                elif clients_key == "revocations_clients":
                    additional_columns = [
                        f'{format_violation_type(client["revocation_violation_type"])}',
                        f'Revocation recommendation staffed on {format_date(client["revocation_report_date"])}',
                    ]

                clients_by_type[clients_key].append(base_columns + additional_columns)
        # sorting clients of each type by the clients' names
        for clients in clients_by_type.values():
            clients.sort(key=lambda x: x[1])
        return clients_by_type

    def _prepare_attachment_data(self) -> Dict:
        prepared_on_date = format_date(
            self.get_batch_id(), current_format="%Y%m%d%H%M%S"
        )

        return {
            "prepared_on_date": prepared_on_date,
            "officer_given_name": format_name(
                self.recipient_data["officer_given_name"]
            ),
            "clients": {
                clients_key: align_columns(clients)
                for clients_key, clients in self._prepare_attachment_clients_tables().items()
            },
        }

    def _caseload_or_district_outcome_text(self, metric: str) -> Optional[str]:
        district_total = int(self.recipient_data[f"{metric}_district_total"])
        recipient_total = int(self.recipient_data[metric])

        if recipient_total:
            return f"{recipient_total:n} from your caseload"

        if district_total:
            return f"{district_total:n} from your district"

        return None

    def _get_pos_discharges(self) -> DecarceralMetricContext:
        """Creates context data for the Successful Completions card"""
        state_total = int(self.recipient_data[f"{POS_DISCHARGES}_state_total"])
        main_text = (
            f"{{}} people completed supervision in {self.state_name} this month."
        )

        supplemental_text = self._caseload_or_district_outcome_text(POS_DISCHARGES)

        action_text = (
            "These clients are within 30 days of their "
            f"{self.metrics_delegate.completion_date_label}:"
        )

        action_clients = self.recipient_data["upcoming_release_date_clients"]

        action_table = (
            [
                (
                    f'{client["full_name"]} ({client["person_external_id"]})',
                    "{d:%B} {d.day}".format(
                        d=date.fromisoformat(client["projected_end_date"])
                    ),
                )
                for client in action_clients
            ]
            if len(action_clients)
            else None
        )

        return {
            "heading": "Successful Completions",
            "icon": "ic_case-completions-v2.png",
            "main_text": main_text,
            "total": state_total,
            "supplemental_text": supplemental_text,
            "action_text": action_text,
            "action_table": action_table,
        }

    def _get_earned_discharges(self) -> DecarceralMetricContext:
        """Creates context data for the Early Releases card"""
        state_total = int(self.recipient_data[f"{EARNED_DISCHARGES}_state_total"])
        main_text = (
            f"{{}} early discharge requests were filed across {self.state_name}."
        )

        supplemental_text = self._caseload_or_district_outcome_text(EARNED_DISCHARGES)

        return {
            "heading": "Early Releases",
            "icon": "ic_early-discharges-v2.png",
            "main_text": main_text,
            "total": state_total,
            "supplemental_text": supplemental_text,
            "action_text": None,
            "action_table": None,
        }

    def _get_supervision_downgrades(self) -> DecarceralMetricContext:
        """Creates context data for the Successful Completions card"""
        state_total = int(self.recipient_data[f"{SUPERVISION_DOWNGRADES}_state_total"])

        main_text = "{} clients had their supervision downgraded this month."

        supplemental_text = self._caseload_or_district_outcome_text(
            SUPERVISION_DOWNGRADES
        )

        action_text = (
            "These clients may be downgraded based on their latest assessment:"
        )

        action_clients = self.recipient_data["mismatches"]

        action_table = (
            [
                (
                    f'{client["name"]} ({client["person_external_id"]})',
                    f"{client['current_supervision_level']} &rarr; {client['recommended_level']}",
                )
                for client in action_clients
            ]
            if len(action_clients)
            else None
        )

        return {
            "heading": "Supervision Downgrades",
            "icon": "ic_supervision-downgrades-v2.png",
            "main_text": main_text,
            "total": state_total,
            "supplemental_text": supplemental_text,
            "action_text": action_text,
            "action_table": action_table,
        }


if __name__ == "__main__":
    context = PoMonthlyReportContext(
        StateCode.US_ID,
        Recipient.from_report_json(
            {
                utils.KEY_EMAIL_ADDRESS: "test@recidiviz.org",
                utils.KEY_STATE_CODE: "US_ID",
                utils.KEY_DISTRICT: "US_ID_D3",
                "pos_discharges": 0,
                "earned_discharges": 0,
                "supervision_downgrades": 2,
                "technical_revocations": 0,
                "crime_revocations": 0,
                "absconsions": 0,
                "pos_discharges_district_average": 0,
                "pos_discharges_state_average": 0,
                "earned_discharges_district_average": 0,
                "earned_discharges_state_average": 0,
                "supervision_downgrades_district_average": 0,
                "supervision_downgrades_state_average": 0,
                "pos_discharges_district_total": 38,
                "pos_discharges_state_total": 273,
                "earned_discharges_district_total": 0,
                "earned_discharges_state_total": 163,
                "supervision_downgrades_district_total": 56,
                "supervision_downgrades_state_total": 391,
                "technical_revocations_district_average": 0,
                "technical_revocations_state_average": 0,
                "crime_revocations_district_average": 0,
                "crime_revocations_state_average": 0,
                "absconsions_district_average": 0,
                "absconsions_state_average": 0,
                "pos_discharges_last_month": 0,
                "earned_discharges_last_month": 0,
                "supervision_downgrades_last_month": 0,
                "technical_revocations_last_month": 0,
                "crime_revocations_last_month": 0,
                "absconsions_last_month": 0,
                "pos_discharges_clients": [],
                "earned_discharges_clients": [],
                "supervision_downgrades_clients": [],
                "absconsions_clients": [],
                "assessments_out_of_date_clients": [],
                "facetoface_out_of_date_clients": [],
                "revocations_clients": [],
                "upcoming_release_date_clients": [
                    {
                        "full_name": "Hansen, Linet",
                        "person_external_id": "105",
                        "projected_end_date": "2021-05-07",
                    },
                    {
                        "full_name": "Cortes, Rebekah",
                        "person_external_id": "142",
                        "projected_end_date": "2021-05-18",
                    },
                ],
                "assessments": "7",
                "assessments_percent": 96,
                "overdue_assessments_goal": "1",
                "overdue_assessments_goal_percent": 100,
                "facetoface": "94",
                "facetoface_percent": 45.268932,
                "overdue_facetoface_goal": "9",
                "overdue_facetoface_goal_percent": 58.732651,
                "officer_external_id": 0,
                "officer_given_name": "Clementine",
                "review_month": 4,
                "mismatches": [
                    {
                        "name": "Tonye Thompson",
                        "person_external_id": "189472",
                        "last_score": 14,
                        "last_assessment_date": "10/12/20",
                        "current_supervision_level": "Medium",
                        "recommended_level": "Low",
                    },
                    {
                        "name": "Linet Hansen",
                        "person_external_id": "47228",
                        "last_assessment_date": "1/12/21",
                        "last_score": 8,
                        "current_supervision_level": "Medium",
                        "recommended_level": "Low",
                    },
                    {
                        "name": "Rebekah Cortes",
                        "person_external_id": "132878",
                        "last_assessment_date": "3/14/20",
                        "last_score": 10,
                        "current_supervision_level": "High",
                        "recommended_level": "Medium",
                    },
                    {
                        "name": "Taryn Berry",
                        "person_external_id": "147872",
                        "last_assessment_date": "3/13/20",
                        "last_score": 4,
                        "current_supervision_level": "High",
                        "recommended_level": "Low",
                    },
                ],
            }
        ),
    )

    with local_project_id_override(GCP_PROJECT_STAGING):
        prepared_data = context.prepare_for_generation()
        prepared_data["static_image_path"] = "./recidiviz/reporting/context/static"

    print(context.html_template.render(**prepared_data))
