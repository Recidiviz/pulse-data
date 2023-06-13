# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper function for PagerDuty alerting integration"""
import logging
import os
from typing import Optional

from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection


def pagerduty_integration_email(conn_id: str) -> Optional[list[str]]:
    """Returns the email of the PagerDuty alert integration if the environment is configured to send mail."""
    if not os.getenv("SENDGRID_MAIL_FROM") or not os.getenv("SENDGRID_MAIL_SENDER"):
        logging.warning(
            "Failure emails disabled due to missing Sendgrid environment variables. Got %s %s",
            os.getenv("SENDGRID_MAIL_FROM"),
            os.getenv("SENDGRID_MAIL_SENDER"),
        )
        return None

    try:
        connection = Connection.get_connection_from_secrets(conn_id)

        return [connection.extra_dejson["email"]]
    except AirflowNotFoundException:
        logging.warning("Could not find connection with id %s", conn_id)
        return None
