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
"""State schema validator functions to be run before session commit to ensure there is no bad database state."""

import logging
from typing import List, Callable

from more_itertools import one
from sqlalchemy import func

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema.state.dao import check_not_dirty
from recidiviz.persistence.database.session import Session


def state_allows_multiple_ids_same_type(state_code: str) -> bool:
    if state_code.upper() in ("US_ND", "US_PA"):
        return True

    # By default, states don't allow multiple different ids of the same type
    return False


def check_people_do_not_have_multiple_ids_same_type(
    session: Session, region_code: str, output_people: List[schema.StatePerson]
) -> bool:
    """Validates that person has two ids of the same type (for states configured to enforce this invariant)."""

    check_not_dirty(session)

    logging.info(
        "[Invariant validation] Checking that no person has multiple external ids of the same type."
    )
    if state_allows_multiple_ids_same_type(region_code):
        logging.info(
            "[Invariant validation] Multiple external ids of the same type allowed for [%s] - skipping.",
            region_code,
        )
        return True

    person_ids = {p.person_id for p in output_people}
    if not person_ids:
        logging.warning(
            "[Invariant validation] No StatePersonExternalIds in the output set - skipping validations."
        )
        return True

    counts_subquery = (
        session.query(
            schema.StatePersonExternalId.state_code.label("state_code"),
            schema.StatePersonExternalId.person_id.label("person_id"),
            schema.StatePersonExternalId.id_type.label("id_type"),
            func.count().label("cnt"),
        )
        .filter(schema.StatePersonExternalId.state_code == region_code.upper())
        .filter(
            # Ideally we would not filter by person_ids, but that query takes ~10s on
            # a test of US_PA external ids. Since this will be run for every file,
            # that sort of performance is prohibitive. We instead filter by just the
            # person ids we think we have touched this session.
            schema.StatePersonExternalId.person_id.in_(person_ids)
        )
        .group_by(
            schema.StatePersonExternalId.state_code,
            schema.StatePersonExternalId.person_id,
            schema.StatePersonExternalId.id_type,
        )
        .subquery()
    )

    query = (
        session.query(
            counts_subquery.c.state_code,
            counts_subquery.c.person_id,
            counts_subquery.c.id_type,
            counts_subquery.c.cnt,
        )
        .filter(counts_subquery.c.cnt > 1)
        .limit(1)
    )

    results = query.all()

    if results:
        _state_code, person_id, id_type, count = one(results)
        logging.error(
            "[Invariant validation] Found people with multiple ids of the same type. First example: "
            "person_id=[%s], id_type=[%s] is used [%s] times.",
            person_id,
            id_type,
            count,
        )
        return False

    logging.info(
        "[Invariant validation] Found no people with multiple external ids of the same type."
    )
    return True


def get_state_database_invariant_validators() -> List[
    Callable[[Session, str, List[schema.StatePerson]], bool]
]:
    return [check_people_do_not_have_multiple_ids_same_type]
