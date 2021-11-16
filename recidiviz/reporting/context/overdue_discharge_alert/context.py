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

python -m recidiviz.reporting.context.overdue_discharge_alert.context
"""

from typing import Any, Dict, List

from jinja2 import Template

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import (
    OFFICER_GIVEN_NAME,
    Batch,
    ReportType,
)
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.recipient import Recipient
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_SPECIFIC_LANGUAGE = {
    StateCode.US_ID: {
        "expiration_date": "full term release date",
        "parole_agents": "POs",
        "partnership": "Recidiviz is working with IDOC",
    }
}


class OverdueDischargeAlertContext(ReportContext):
    """Report context for the Top Opportunities email."""

    @property
    def jinja_options(self) -> Dict[str, Any]:
        return {"trim_blocks": True}

    def get_required_recipient_data_fields(self) -> List[str]:
        return [
            OFFICER_GIVEN_NAME,
            "overdue_discharges",
            "upcoming_discharges",
        ]

    def get_report_type(self) -> ReportType:
        return ReportType.OverdueDischargeAlert

    @property
    def html_template(self) -> Template:
        return self.jinja_env.get_template("overdue_discharge_alert/email.html.jinja2")

    def _prepare_for_generation(self) -> dict:
        """Executes PO Monthly Report data preparation."""
        return {
            **self.recipient_data,
            "l10n": STATE_SPECIFIC_LANGUAGE[self.state_code],
        }


if __name__ == "__main__":
    context = OverdueDischargeAlertContext(
        Batch(
            state_code=StateCode.US_ID,
            batch_id="test",
            report_type=ReportType.OverdueDischargeAlert,
        ),
        Recipient.from_report_json(
            {
                utils.KEY_EMAIL_ADDRESS: "test@recidiviz.org",
                utils.KEY_STATE_CODE: "US_ID",
                utils.KEY_DISTRICT: "US_ID_D3",
                OFFICER_GIVEN_NAME: "Alex",
                "overdue_discharges": [
                    {
                        "full_name": "Dan",
                        "person_external_id": "123",
                        "expiration_date": "123",
                    },
                    {
                        "full_name": "Dan",
                        "person_external_id": "123",
                        "expiration_date": "123",
                    },
                ],
                "upcoming_discharges": [
                    {
                        "full_name": "Dan",
                        "person_external_id": "123",
                        "expiration_date": "123",
                    },
                    {
                        "full_name": "Dan",
                        "person_external_id": "123",
                        "expiration_date": "123",
                    },
                ],
            }
        ),
    )

    with local_project_id_override(GCP_PROJECT_STAGING):
        prepared_data = context.get_prepared_data()
        prepared_data["static_image_path"] = "./recidiviz/reporting/context/static"

    print(context.html_template.render(**prepared_data))
