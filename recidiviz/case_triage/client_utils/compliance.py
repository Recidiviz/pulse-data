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
"""Utilities for computing client-related compliance properties."""

from datetime import date, timedelta
from typing import Literal, Optional, Tuple

from recidiviz.persistence.database.schema.case_triage.schema import ETLClient

DueDateStatus = Literal["OVERDUE", "UPCOMING"]


def _get_due_date_status(
    due_date: date, upcoming_threshold_days: int
) -> Optional[DueDateStatus]:
    today = date.today()
    if due_date < today:
        return "OVERDUE"
    if due_date <= today + timedelta(days=upcoming_threshold_days):
        return "UPCOMING"

    return None


def _get_days_until_due(due_date: date) -> int:
    return (due_date - date.today()).days


def get_assessment_due_details(
    client: ETLClient,
) -> Optional[Tuple[DueDateStatus, int]]:
    """Outputs whether client's assessment is upcoming or overdue relative to the current date."""
    due_date = client.next_recommended_assessment_date
    if due_date:
        status = _get_due_date_status(due_date, 30)
        if status:
            return (status, _get_days_until_due(due_date))
    return None


def get_contact_due_details(client: ETLClient) -> Optional[Tuple[DueDateStatus, int]]:
    """Outputs whether contact with client is upcoming or overdue relative to the current date."""

    if due_date := client.next_recommended_face_to_face_date:
        if status := _get_due_date_status(due_date, 30):
            return status, _get_days_until_due(due_date)
    return None


def get_home_visit_due_details(
    client: ETLClient,
) -> Optional[Tuple[DueDateStatus, int]]:
    """Outputs whether a home visit with client is upcoming or overdue relative to the current date."""

    if due_date := client.next_recommended_home_visit_date:
        if status := _get_due_date_status(due_date, 30):
            return status, _get_days_until_due(due_date)
    return None
