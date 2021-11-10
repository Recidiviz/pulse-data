# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Report context for the Top Opportunity emails.

To generate a sample output for the Top Opps email template, just run:

python -m recidiviz.reporting.context.top_opportunities.context
"""

import copy
import os
from typing import List

from jinja2 import Environment, FileSystemLoader, Template

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import (
    DEFAULT_MESSAGE_BODY_KEY,
    OFFICER_GIVEN_NAME,
    ReportType,
)
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.recipient import Recipient
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class TopOpportunitiesReportContext(ReportContext):
    """Report context for the Top Opportunities email."""

    def __init__(self, batch: utils.Batch, recipient: Recipient):
        self.recipient_data = recipient.data

        self.jinja_env = Environment(
            loader=FileSystemLoader(self._get_context_templates_folder())
        )
        super().__init__(batch, recipient)

    def get_required_recipient_data_fields(self) -> List[str]:
        return [OFFICER_GIVEN_NAME, "mismatches"]

    def get_report_type(self) -> ReportType:
        return ReportType.TopOpportunities

    def get_properties_filepath(self) -> str:
        """Returns path to the properties.json, assumes it is in the same directory as the context."""
        return os.path.join(os.path.dirname(__file__), "properties.json")

    @property
    def html_template(self) -> Template:
        return self.jinja_env.get_template("top_opportunities/email.html.jinja2")

    def _prepare_for_generation(self) -> dict:
        """Executes PO Monthly Report data preparation."""
        self.prepared_data = copy.deepcopy(self.recipient_data)

        self.prepared_data["static_image_path"] = utils.get_static_image_path(
            self.get_report_type()
        )
        self.prepared_data[
            "greeting"
        ] = f'Hi {self.recipient_data["officer_given_name"]}:'

        if message_override := self.recipient_data.get("message_body_override"):
            self.prepared_data["message_body"] = message_override
        else:
            self.prepared_data["message_body"] = self.properties[
                DEFAULT_MESSAGE_BODY_KEY
            ]

        return self.prepared_data


if __name__ == "__main__":
    context = TopOpportunitiesReportContext(
        utils.Batch(
            state_code=StateCode.US_ID,
            batch_id="test",
            report_type=ReportType.TopOpportunities,
        ),
        Recipient.from_report_json(
            {
                utils.KEY_EMAIL_ADDRESS: "test@recidiviz.org",
                utils.KEY_STATE_CODE: "US_ID",
                utils.KEY_DISTRICT: "US_ID_D3",
                OFFICER_GIVEN_NAME: "Alex",
                "assessment_name": "LSI-R",
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
        prepared_data = context.get_prepared_data()
        prepared_data["static_image_path"] = "./recidiviz/reporting/context/static"

    print(context.html_template.render(**prepared_data))
