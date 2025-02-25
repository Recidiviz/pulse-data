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
"""Tests for email.py"""
import os
import unittest
from unittest import mock

from recidiviz.airflow.dags.utils.email import (
    SENDGRID_MAIL_FROM,
    SENDGRID_MAIL_SENDER,
    can_send_mail,
)


class TestCanSendMail(unittest.TestCase):
    """Tests for can_send_mail()."""

    @mock.patch.dict(
        os.environ,
        {
            SENDGRID_MAIL_FROM: "alerts+airflow-production@recidiviz.org",
            SENDGRID_MAIL_SENDER: "Airflow Alerts (production)",
        },
    )
    def test_can_send_mail_env_vars_set(self) -> None:
        self.assertTrue(can_send_mail())

    def test_can_send_mail_env_vars_not_set(self) -> None:
        self.assertFalse(can_send_mail())

    @mock.patch.dict(
        os.environ,
        {
            SENDGRID_MAIL_FROM: "alerts+airflow-production@recidiviz.org",
        },
    )
    def test_can_send_mail_env_vars_only_from_email_set(self) -> None:
        with self.assertRaisesRegex(
            RuntimeError,
            "Must have either none or both of SENDGRID_MAIL_FROM and "
            "SENDGRID_MAIL_SENDER env variables set.",
        ):
            _ = can_send_mail()

    @mock.patch.dict(
        os.environ,
        {
            SENDGRID_MAIL_SENDER: "Airflow Alerts (production)",
        },
    )
    def test_can_send_mail_env_vars_only_sender_set(self) -> None:
        with self.assertRaisesRegex(
            RuntimeError,
            "Must have either none or both of SENDGRID_MAIL_FROM and "
            "SENDGRID_MAIL_SENDER env variables set.",
        ):
            _ = can_send_mail()
