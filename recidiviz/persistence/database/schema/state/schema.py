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
# ============================================================================

"""Define the ORM schema objects that map directly to the database,
for state-level entities.

The below schema uses only generic SQLAlchemy types, and therefore should be
portable between database implementations.

NOTE: Many of the tables in the below schema are historical tables. The primary
key of a historical table exists only due to the requirements of SQLAlchemy,
and should not be referenced by any other table. The key which should be used
to reference a historical table is the key shared with the master table. For
the historical table, this key is non-unique. This is necessary to allow the
desired temporal table behavior. Because of this, any foreign key column on a
historical table must point to the *master* table (which has a unique key), not
the historical table (which does not). Because the key is shared between the
master and historical tables, this allows an indirect guarantee of referential
integrity to the historical tables as well.
"""

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.base_schema import Base

from recidiviz.persistence.database.schema.shared_enums import (
    gender,
    residency_status,
)


# TODO(1625) - Move this to a shared location for use in other schemas.
class _HistoryTableSharedColumns:
    """A mixin which defines all columns common any history table"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _HistoryTableSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)


# StatePerson

class _StatePersonSharedColumns:
    """A mixin which defines all columns common to StatePerson and
    StatePersonHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StatePersonSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    current_address = Column(Text)

    full_name = Column(String(255), index=True)
    # Serialized list, deserialized in entity layer
    aliases = Column(Text)

    birthdate = Column(Date, index=True)
    birthdate_inferred_from_age = Column(Boolean)

    gender = Column(gender)
    gender_raw_text = Column(String(255))

    residency_status = Column(residency_status)


# TODO(1625): Once these fields match those on entities.StatePerson, update
#  schema entity converter to handle StatePerson properly.
class StatePerson(Base, DatabaseEntity, _StatePersonSharedColumns):
    """Represents a person in the state SQL schema"""
    __tablename__ = 'state_person'

    person_id = Column(Integer, primary_key=True)


class StatePersonHistory(Base,
                         DatabaseEntity,
                         _StatePersonSharedColumns,
                         _HistoryTableSharedColumns):

    """Represents the historical state of a state person"""
    __tablename__ = 'state_person_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_history_id = Column(Integer, primary_key=True)

    person_id = Column(
        Integer, ForeignKey('state_person.person_id'),
        nullable=False, index=True)
