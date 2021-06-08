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

from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.recipient import Recipient


class TopOpportunitiesReportContext(ReportContext):
    """Report context for the Top Opportunities email."""

    def __init__(self, state_code: str, recipient: Recipient):
        super().__init__(state_code, recipient)
        self.recipient_data = recipient.data

        self.jinja_env = Environment(
            loader=FileSystemLoader(self._get_context_templates_folder())
        )

    def get_required_recipient_data_fields(self) -> List[str]:
        return []

    def get_report_type(self) -> str:
        return "top_opportunities"

    @property
    def html_template(self) -> Template:
        return self.jinja_env.get_template("top_opportunities/email.html.jinja2")

    def prepare_for_generation(self) -> dict:
        """Executes PO Monthly Report data preparation."""
        self.prepared_data = copy.deepcopy(self.recipient_data)
        return self.prepared_data


if __name__ == "__main__":
    jinja_env = Environment(
        loader=FileSystemLoader(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
        )
    )

    template = jinja_env.get_template("top_opportunities/email.html.jinja2")
    print(
        template.render(
            greeting="Hi Clementine,",
            message_body="We've been searching your caseload for opportunities to lighten your load. We've found a couple of cases where we think your clients are eligible for reduced supervision. Please let us know if anything seems wrong!",
            static_image_path="./recidiviz/reporting/context/static",
            mismatches={
                "high_to_medium": [
                    {
                        "name": "Nikhil Bhargava",
                        "person_external_id": "189472",
                    },
                ],
                "high_to_low": [
                    {
                        "name": "Serena Chang",
                        "person_external_id": "47228",
                    },
                    {
                        "name": "Juan Agron",
                        "person_external_id": "132878",
                    },
                    {
                        "name": "Emily Chao",
                        "person_external_id": "147872",
                    },
                ],
                "medium_to_low": [
                    {
                        "name": "Annalise Irby",
                        "person_external_id": "74827",
                    },
                    {
                        "name": "Dan Hansen",
                        "person_external_id": "32224",
                    },
                ],
            },
        )
    )
