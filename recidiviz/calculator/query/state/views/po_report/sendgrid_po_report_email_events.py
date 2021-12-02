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
"""Data to generate the events following an email being sent from Sendgrid.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.po_report.sendgrid_po_report_email_events
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENDGRID_PO_REPORT_EMAIL_EVENTS_VIEW_NAME = "sendgrid_po_report_email_events"

SENDGRID_PO_REPORT_EMAIL_EVENTS_DESCRIPTION = """
 Sendgrid events describing the engagement with a PO monthly report email sent.
 """

SENDGRID_PO_REPORT_EMAIL_EVENTS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH monthly_report_message_ids AS (
        SELECT message_id
        FROM `{project_id}.{sendgrid_email_dataset}.raw_sendgrid_email_data`
        WHERE ENDS_WITH(email, ".gov")
        AND subject = "Your monthly Recidiviz report"
    )
    SELECT 
        message_id,
        email,
        event,
        processed AS event_datetime
    FROM `{project_id}.{sendgrid_email_dataset}.raw_sendgrid_email_data`
    JOIN monthly_report_message_ids USING (message_id)
    ORDER BY message_id, processed
    """

SENDGRID_PO_REPORT_EMAIL_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SENDGRID_PO_REPORT_EMAIL_EVENTS_VIEW_NAME,
    should_materialize=True,
    view_query_template=SENDGRID_PO_REPORT_EMAIL_EVENTS_QUERY_TEMPLATE,
    description=SENDGRID_PO_REPORT_EMAIL_EVENTS_DESCRIPTION,
    sendgrid_email_dataset=dataset_config.SENDGRID_EMAIL_DATA_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENDGRID_PO_REPORT_EMAIL_EVENTS_VIEW_BUILDER.build_and_print()
