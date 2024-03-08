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
"""Defines a SQLAlchemy session listener for enforcing pre-commit constraints that cannot be implemented easily with a
CheckConstraint."""

from sqlite3 import IntegrityError
from typing import Any, Optional

from sqlalchemy import event

from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestInstanceStatus,
)
from recidiviz.persistence.database.session import Session


def session_listener(session: Session) -> None:
    """Returns a session listener for the provided session which can be registered to
    respond to different events in the session.
    """

    @event.listens_for(session, "pending_to_persistent")
    def _pending_to_persistent(session: Session, instance: Any) -> None:
        """Called when a SQLAlchemy object transitions to a persistent object. If this function throws, the session
        will be rolled back and that object will not be committed."""
        if isinstance(instance, DirectIngestInstanceStatus):
            # Confirm that the timestamp of the row that is attempting to be committed is strictly after
            # the most recent row's timestamp.
            most_recent_row: Optional[DirectIngestInstanceStatus] = (
                session.query(DirectIngestInstanceStatus)
                .filter_by(
                    region_code=instance.region_code,
                    instance=instance.instance,
                )
                .order_by(DirectIngestInstanceStatus.status_timestamp.desc())
                .limit(1)
                .one_or_none()
            )

            if (
                most_recent_row
                and most_recent_row.status_timestamp > instance.status_timestamp
            ):
                raise IntegrityError(
                    "Attempting to commit a DirectIngestInstanceStatus row for "
                    f"region_code={instance.region_code} and instance={instance.instance} whose timestamp is less "
                    f"than the timestamp of the most recent row. The timestamp of the most recent row is "
                    f"{most_recent_row.status_timestamp} and the timestamp of the attempted committed row is "
                    f"{instance.status_timestamp}."
                )
