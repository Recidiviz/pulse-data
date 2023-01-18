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
from typing import Iterable, List, Type

from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.errors import PersistenceError
from recidiviz.persistence.persistence_utils import SchemaRootEntityT
from recidiviz.utils import environment


class SessionIsDirtyError(PersistenceError):
    pass


def check_not_dirty(session: Session) -> None:
    if session.dirty or session.new:
        raise SessionIsDirtyError(
            "Session unexpectedly dirty - flush before querying the database."
        )


def read_root_entities_by_external_ids(
    session: Session,
    state_code: str,
    schema_root_entity_cls: Type[SchemaRootEntityT],
    cls_external_ids: Iterable[str],
) -> List[SchemaRootEntityT]:
    # TODO(#17854): Figure out how to actually generalize the logic in these functions
    #  to support any HasMultipleExternalIds entity.
    if schema_root_entity_cls is schema.StatePerson:
        return _read_people_by_external_ids(session, state_code, cls_external_ids)
    if schema_root_entity_cls is schema.StateStaff:
        return _read_staff_by_external_ids(session, state_code, cls_external_ids)

    raise ValueError(f"Unexpected root entity cls [{schema_root_entity_cls.__name__}]")


def _read_people_by_external_ids(
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


def _read_staff_by_external_ids(
    session: Session, state_code: str, cls_external_ids: Iterable[str]
) -> List[schema.StateStaff]:
    """Reads all StateStaff in the given |state_code| with an external id in
    |cls_external_ids|.
    """
    check_not_dirty(session)

    logging.info(
        "[DAO] Starting read of external ids of class [%s]",
        schema.StateStaffExternalId.__name__,
    )
    staff_ids_result = (
        session.query(schema.StateStaffExternalId.staff_id)
        .filter(schema.StateStaffExternalId.external_id.in_(cls_external_ids))
        .filter(schema.StateStaffExternalId.state_code == state_code.upper())
        .all()
    )
    staff_ids = [res[0] for res in staff_ids_result]
    logging.info(
        "[DAO] Finished read of external ids of class [%s]. Found [%s] staff ids.",
        schema.StateStaffExternalId.__name__,
        len(staff_ids),
    )

    query = session.query(schema.StateStaff).filter(
        schema.StateStaff.staff_id.in_(staff_ids)
    )
    schema_staff = query.all()
    logging.info("[DAO] Finished read of [%s] staff.", len(schema_staff))
    return schema_staff


# TODO(#17854): Consider combining these two functions into a single
#  read_all_root_entities() function.
@environment.test_only
def read_all_people(session: Session) -> List[schema.StatePerson]:
    """Read all StatePerson in the database. For test use only."""
    check_not_dirty(session)

    return session.query(schema.StatePerson).all()


@environment.test_only
def read_all_staff(session: Session) -> List[schema.StateStaff]:
    """Read all StateStaff in the database. For test use only."""
    check_not_dirty(session)

    return session.query(schema.StateStaff).all()
