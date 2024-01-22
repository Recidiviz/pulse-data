# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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

"""
Cloud Run Job script that copies all metric settings from a super agency to its 
child agencies, and sends the requesting user an email confirmation upon the success
of the job.
"""

import argparse
import logging

from recidiviz.justice_counts.utils.constants import UNSUBSCRIBE_GROUP_ID
from recidiviz.reporting.sendgrid_client_wrapper import SendGridClientWrapper

# from recidiviz.tools.justice_counts.copy_over_metric_settings_to_child_agencies import (
#     copy_metric_settings,
# )
from recidiviz.utils.params import str_to_list

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--super_agency_id",
        type=int,
        help="The id of the super_agency you want to copy agency settings from.",
        required=True,
    )
    parser.add_argument(
        "--user_email",
        type=str,
        help="The email address of the user to send a job completion message to.",
        required=True,
    )
    parser.add_argument(
        "--metric_definition_key_subset",
        type=str_to_list,
        help="List of metrics definition keys that should be copied over. To find a list of metric definition keys for all systems check the script (copy_over_metric_settings_to_child_agencies.py).",
        required=False,
    )
    args = parser.parse_args()

    # TODO(#1056) - Debug/refactor to get `copy_metric_settings` working
    # with local_project_id_override(args.project_id):
    #     copy_metric_settings(
    #         dry_run=True,
    #         super_agency_id=args.super_agency_id,
    #         metric_definition_key_subset=args.metric_definition_key_subset,
    # )

    # Send confirmation email once metrics have been successfully copied over
    send_grid_client = SendGridClientWrapper(key_type="justice_counts")
    try:
        send_grid_client.send_message(
            to_email=args.user_email,
            from_email="no-reply@justice-counts.org",
            from_email_name="Justice Counts",
            subject=f"Your request to copy superagency {args.super_agency_id}'s metric settings to its child agencies has been completed.",
            html_content=f"""<p>All of the metric settings from superagency {args.super_agency_id} have been successfully copied over to all of its child agencies.</p>""",
            disable_link_click=True,
            unsubscribe_group_id=UNSUBSCRIBE_GROUP_ID,
        )
    except Exception as e:
        logging.exception("Failed to send confirmation email: %s", e)
