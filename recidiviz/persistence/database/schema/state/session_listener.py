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

from typing import Any

from psycopg2 import IntegrityError
from sqlalchemy import event

from recidiviz.persistence.database.schema.state.schema import (
    StateAssessment,
    StateProgramAssignment,
    StateSupervisionContact,
    StateSupervisionPeriod,
)
from recidiviz.persistence.database.session import Session


# TODO(#19786): delete once StateAgent is deleted
def session_listener(session: Session) -> None:
    """Returns a session listener for the provided session which can be registered to
    respond to different events in the session.
    """

    @event.listens_for(session, "pending_to_persistent")
    def _pending_to_persistent(
        session: Session, instance: Any  # pylint: disable=unused-argument
    ) -> None:
        """Called when a SQLAlchemy object transitions to a persistent object. If this function throws, the session
        will be rolled back and that object will not be committed."""
        if isinstance(instance, StateAssessment):
            if not instance.conducting_agent:
                if instance.conducting_staff_external_id:
                    raise IntegrityError(
                        f"Attempting to commit StateAssessment row for "
                        f"conducting_agent=None,"
                        f"conducting_staff_external_id={instance.conducting_staff_external_id},"
                        f"conducting_agent.external_id should match conducting_staff_external_id."
                    )
                return

            if (
                instance.conducting_agent.external_id
                != instance.conducting_staff_external_id
            ):
                raise IntegrityError(
                    f"Attempting to commit StateAssessment row for "
                    f"conducting_agent.external_id={instance.conducting_agent.external_id},"
                    f"conducting_staff_external_id={instance.conducting_staff_external_id}"
                )

        if isinstance(instance, StateSupervisionPeriod):
            if not instance.supervising_officer:
                if instance.supervising_officer_staff_external_id:
                    raise IntegrityError(
                        f"Attempting to commit StateSupervisionPeriod row for "
                        f"supervising_officer=None,"
                        f"supervising_officer_external_id={instance.supervising_officer_staff_external_id},"
                        f"supervising_officer.external_id should match supervising_officer_external_id."
                    )
                return

            if (
                instance.supervising_officer.external_id
                != instance.supervising_officer_staff_external_id
            ):
                raise IntegrityError(
                    f"Attempting to commit StateSupervisionPeriod row for "
                    f"supervising_officer.external_id={instance.supervising_officer.external_id},"
                    f"supervising_officer_external_id={instance.supervising_officer_staff_external_id}"
                )

        if isinstance(instance, StateSupervisionContact):
            if not instance.contacted_agent:
                if instance.contacting_staff_external_id:
                    raise IntegrityError(
                        f"Attempting to commit StateSupervisionContact row for "
                        f"contacted_agent=None,"
                        f"contacting_staff_external_id={instance.contacting_staff_external_id},"
                        f"contacted_agent.external_id should match contacting_staff_external_id."
                    )
                return

            if (
                instance.contacted_agent.external_id
                != instance.contacting_staff_external_id
            ):
                raise IntegrityError(
                    f"Attempting to commit StateSupervisionContact row for "
                    f"contacted_agent.external_id={instance.contacted_agent.external_id},"
                    f"contacting_staff_external_id={instance.contacting_staff_external_id}"
                )

        if isinstance(instance, StateProgramAssignment):
            if not instance.referring_agent:
                if instance.referring_staff_external_id:
                    raise IntegrityError(
                        f"Attempting to commit StateProgramAssignment row for "
                        f"referring_agent=None,"
                        f"referring_staff_external_id={instance.referring_staff_external_id},"
                        f"referring_agent.external_id should match referring_staff_external_id."
                    )
                return

            if (
                instance.referring_agent.external_id
                != instance.referring_staff_external_id
            ):
                raise IntegrityError(
                    f"Attempting to commit StateProgramAssignment row for "
                    f"referring_agent.external_id={instance.referring_agent.external_id},"
                    f"referring_staff_external_id={instance.referring_staff_external_id}"
                )
