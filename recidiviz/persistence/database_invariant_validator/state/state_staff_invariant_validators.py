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
"""StateStaff schema validator functions to be run before session commit to ensure there
is no bad database state.
"""

import logging
from typing import Callable, List

from more_itertools import first
from sqlalchemy import func

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema.state.dao import check_not_dirty
from recidiviz.persistence.database.session import Session


def state_allows_multiple_ids_same_type(state_code: str) -> bool:
    if state_code.upper() in ("US_MI", "US_IX"):
        return True

    # By default, states don't allow multiple different ids of the same type
    return False


def check_staff_do_not_have_multiple_ids_same_type(
    session: Session, region_code: str, output_staff: List[schema.StateStaff]
) -> bool:
    """Validates that no staff member has two ids of the same type."""

    check_not_dirty(session)

    logging.info(
        "[Invariant validation] Checking that no staff member has multiple external "
        "ids of the same type."
    )

    if state_allows_multiple_ids_same_type(region_code):
        logging.info(
            "[Invariant validation] Multiple external ids of the same type allowed for [%s] - skipping.",
            region_code,
        )
        return True

    staff_ids = {s.staff_id for s in output_staff}
    if not staff_ids:
        logging.warning(
            "[Invariant validation] No StateStaffExternalIds in the output set - skipping validations."
        )
        return True

    counts_subquery = (
        session.query(
            schema.StateStaffExternalId.state_code.label("state_code"),
            schema.StateStaffExternalId.staff_id.label("staff_id"),
            schema.StateStaffExternalId.id_type.label("id_type"),
            func.count().label("cnt"),
        )
        .filter(schema.StateStaffExternalId.state_code == region_code.upper())
        .filter(
            # Ideally we would not filter by staff_ids, but that query takes ~10s on
            # a test of US_PA external ids. Since this will be run for every file,
            # that sort of performance is prohibitive. We instead filter by just the
            # staff ids we think we have touched this session.
            schema.StateStaffExternalId.staff_id.in_(staff_ids)
        )
        .group_by(
            schema.StateStaffExternalId.state_code,
            schema.StateStaffExternalId.staff_id,
            schema.StateStaffExternalId.id_type,
        )
        .subquery()
    )

    query = (
        session.query(
            counts_subquery.c.state_code,
            counts_subquery.c.staff_id,
            counts_subquery.c.id_type,
            counts_subquery.c.cnt,
        )
        .filter(counts_subquery.c.cnt > 1)
        .limit(1)
    )

    results = query.all()

    if results:
        _state_code, staff_id, id_type, count = first(results)
        logging.error(
            "[Invariant validation] Found staff members with multiple ids of the same "
            "type. First example: staff_id=[%s], id_type=[%s] is used [%s] times.",
            staff_id,
            id_type,
            count,
        )
        return False

    logging.info(
        "[Invariant validation] Found no staff members with multiple external ids of "
        "the same type."
    )
    return True


def check_all_staff_have_an_external_id(
    session: Session, region_code: str, _output_staff: List[schema.StateStaff]
) -> bool:
    """Validates that all state_staff have at least one external id."""

    check_not_dirty(session)

    logging.info(
        "[Invariant validation] Checking that all staff members have at least one "
        "external id."
    )

    query = f"""
SELECT staff_id
FROM 
  state_staff s
LEFT OUTER JOIN 
  state_staff_external_id eid
USING (staff_id)
WHERE s.state_code = '{region_code.upper()}' AND eid.staff_id IS NULL
LIMIT 1;"""

    results = session.execute(query).all()

    if results:
        staff_id = first(results)
        logging.error(
            "[Invariant validation] Found staff members with no external ids. "
            "First example: staff_id=[%s].",
            staff_id,
        )
        return False

    logging.info("[Invariant validation] Found no staff members without external ids.")
    return True


def get_state_staff_database_invariant_validators() -> (
    List[Callable[[Session, str, List[schema.StateStaff]], bool]]
):
    return [
        check_staff_do_not_have_multiple_ids_same_type,
        check_all_staff_have_an_external_id,
    ]
