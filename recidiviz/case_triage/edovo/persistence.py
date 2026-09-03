# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Write path for Edovo course completions with idempotency guarantees."""

import uuid
from datetime import datetime

from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError

from recidiviz.case_triage.edovo.course_completion_models import CourseCompletionRequest
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdovoCourseCompletion,
)
from recidiviz.persistence.database.session import Session

_NO_DOUBLE_CREDIT_CONSTRAINT = "edovo_course_completions_no_double_credit"


class AlreadyCompletedError(Exception):
    """Raised when this (state, external id, course) already has credit under a different idempotency key.

    Carries the ``existing`` record that already holds the credit, so the
    endpoint can tell Edovo which submission of theirs we are pointing at.
    """

    def __init__(self, existing: EdovoCourseCompletion) -> None:
        self.existing = existing
        super().__init__("This person has already received credit for this course.")


def find_completion_by_idempotency_key(
    session: Session, idempotency_key: uuid.UUID
) -> EdovoCourseCompletion | None:
    """Returns the completion already recorded under |idempotency_key|, if any.

    Exposed separately from ``persist_completion`` so the endpoint can answer a
    replay from what we already recorded, without first re-running the checks
    that produced the original answer.
    """
    return (
        session.query(EdovoCourseCompletion)
        .filter_by(idempotency_key=idempotency_key)
        .one_or_none()
    )


def persist_completion(
    session: Session,
    request: CourseCompletionRequest,
    idempotency_key: uuid.UUID,
    received_at: datetime,
) -> tuple[EdovoCourseCompletion, bool]:
    """Write a course completion to the database and return (record, is_new).

    A key we have already recorded is detected by the unique constraint on
    flush rather than by a lookup first: the endpoint answers a replay from
    ``find_completion_by_idempotency_key`` before it ever gets here, so a
    pre-check would only re-run that query for every accepted request. A
    replay that does reach here is still returned as (record, False).

    On constraint failure the session transaction is rolled back. Callers must
    not rely on any prior session state surviving a raised exception. Callers
    are responsible for committing |session| on success.

    Raises AlreadyCompletedError when the same (state_code, person_external_id,
    course_id) was previously recorded under a different idempotency key. This is
    a best-effort, capture-time guard keyed on the external id, not an
    authoritative per-person guarantee (see EdovoCourseCompletion's constraint
    comment); the authoritative no-double-credit check happens downstream.
    """
    record = EdovoCourseCompletion(
        idempotency_key=idempotency_key,
        person_external_id=request.person_external_id,
        id_type=request.id_type,
        state_code=request.state_code,
        course_id=request.course_id,
        course_name=request.course_name,
        first_name=request.first_name,
        last_name=request.last_name,
        facility=request.facility,
        content_hours=request.content_hours,
        completed_at=request.completed_at,
        received_at=received_at,
    )
    session.add(record)
    try:
        session.flush()
        return record, True
    except IntegrityError as exc:
        session.rollback()
        if not isinstance(exc.orig, UniqueViolation):
            raise
        # A replay of an accepted key violates both constraints, and Postgres
        # reports whichever it checked first, so resolve from the key instead.
        existing = find_completion_by_idempotency_key(session, idempotency_key)
        if existing is not None:
            return existing, False
        if exc.orig.diag.constraint_name == _NO_DOUBLE_CREDIT_CONSTRAINT:
            already_credited = (
                session.query(EdovoCourseCompletion)
                .filter_by(
                    state_code=request.state_code,
                    person_external_id=request.person_external_id,
                    course_id=request.course_id,
                )
                .one_or_none()
            )
            if already_credited is None:
                raise ValueError(
                    f"Course [{request.course_id}] violated "
                    f"[{_NO_DOUBLE_CREDIT_CONSTRAINT}] but the conflicting "
                    "record could not be found."
                ) from exc
            raise AlreadyCompletedError(already_credited) from exc
        raise ValueError(
            f"Idempotency key [{idempotency_key}] caused a unique violation on "
            f"[{exc.orig.diag.constraint_name}] but the conflicting record "
            "could not be found."
        ) from exc
