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

from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError

from recidiviz.case_triage.edovo.course_completion_models import CourseCompletionRequest
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdevoCourseCompletion,
)
from recidiviz.persistence.database.session import Session

_NO_DOUBLE_CREDIT_CONSTRAINT = "edovo_course_completions_no_double_credit"


class AlreadyCompletedError(Exception):
    """Raised when the person+course pair already has credit under a different idempotency key."""


def persist_completion(
    session: Session,
    request: CourseCompletionRequest,
    person_id: str,
    idempotency_key: uuid.UUID,
) -> tuple[EdevoCourseCompletion, bool]:
    """Write a course completion to the database and return (record, is_new).

    On constraint failure the session transaction is rolled back. Callers must
    not rely on any prior session state surviving a raised exception. Callers
    are responsible for committing |session| on success.

    Raises AlreadyCompletedError when the same person+course combination was
    previously recorded with a different idempotency key.
    """
    existing = (
        session.query(EdevoCourseCompletion)
        .filter_by(idempotency_key=idempotency_key)
        .one_or_none()
    )
    if existing is not None:
        return existing, False

    record = EdevoCourseCompletion(
        idempotency_key=idempotency_key,
        person_id=person_id,
        state_code=request.state_code,
        course_id=request.course_id,
        course_name=request.course_name,
        content_hours=request.content_hours,
        completed_at=request.completed_at,
    )
    session.add(record)
    try:
        session.flush()
        return record, True
    except IntegrityError as exc:
        session.rollback()
        if isinstance(exc.orig, UniqueViolation):
            if exc.orig.diag.constraint_name == _NO_DOUBLE_CREDIT_CONSTRAINT:
                raise AlreadyCompletedError() from exc
            # Concurrent request won the race on the idempotency key.
            existing = (
                session.query(EdevoCourseCompletion)
                .filter_by(idempotency_key=idempotency_key)
                .one_or_none()
            )
            if existing is None:
                raise ValueError(
                    f"Idempotency key {idempotency_key} caused a unique violation "
                    "but the conflicting record could not be found."
                ) from exc
            return existing, False
        raise
