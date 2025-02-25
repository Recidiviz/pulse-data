# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
""" Utils for Airflow email """
import logging
import os

# Airflow is configured to use the airflow.providers.sendgrid emailer
# These environment variables set the default values for from email / from name
# https://airflow.apache.org/docs/apache-airflow-providers-sendgrid/stable/_modules/airflow/providers/sendgrid/utils/emailer.html#send_email
SENDGRID_MAIL_FROM = "SENDGRID_MAIL_FROM"
SENDGRID_MAIL_SENDER = "SENDGRID_MAIL_SENDER"


def can_send_mail() -> bool:
    mail_from = os.getenv(SENDGRID_MAIL_FROM)
    mail_sender = os.getenv(SENDGRID_MAIL_SENDER)

    if (mail_from and not mail_sender) or (mail_sender and not mail_from):
        raise RuntimeError(
            f"Must have either none or both of {SENDGRID_MAIL_FROM} and "
            f"{SENDGRID_MAIL_SENDER} env variables set. Found {mail_from=} and "
            f"{mail_sender=}."
        )

    can_send = bool(mail_from) and bool(mail_sender)

    if not can_send:
        logging.warning(
            "Emails disabled due to missing Sendgrid environment variables. Got %s %s",
            os.getenv(SENDGRID_MAIL_FROM),
            os.getenv(SENDGRID_MAIL_SENDER),
        )

    return can_send
