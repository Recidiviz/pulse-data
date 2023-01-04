# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Data Access Object (DAO) with logic for accessing state-level information
from a SQL Database."""
import logging
from typing import Iterable, List

from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.errors import PersistenceError
from recidiviz.utils import environment


class SessionIsDirtyError(PersistenceError):
    pass


def check_not_dirty(session: Session) -> None:
    if session.dirty or session.new:
        raise SessionIsDirtyError(
            "Session unexpectedly dirty - flush before querying the database."
        )


def read_people_by_external_ids(
    session: Session, state_code: str, cls_external_ids: Iterable[str]
) -> List[schema.StatePerson]:
    """Reads all StatePerson in the given |state_code| with an external id in
    |cls_external_ids|.
    """
    check_not_dirty(session)

    logging.info(
        "[DAO] Starting read of external ids of class [%s]",
        schema.StatePersonExternalId.__name__,
    )
    person_ids_result = (
        session.query(schema.StatePersonExternalId.person_id)
        .filter(schema.StatePersonExternalId.external_id.in_(cls_external_ids))
        .filter(schema.StatePersonExternalId.state_code == state_code.upper())
        .all()
    )
    person_ids = [res[0] for res in person_ids_result]
    logging.info(
        "[DAO] Finished read of external ids of class [%s]. Found [%s] person ids.",
        schema.StatePersonExternalId.__name__,
        len(person_ids),
    )

    query = session.query(schema.StatePerson).filter(
        schema.StatePerson.person_id.in_(person_ids)
    )
    schema_persons = query.all()
    logging.info("[DAO] Finished read of [%s] persons.", len(schema_persons))
    return schema_persons


@environment.test_only
def read_all_people(session: Session) -> List[schema.StatePerson]:
    """Read all StatePerson in the database. For test use only."""
    check_not_dirty(session)

    return session.query(schema.StatePerson).all()
