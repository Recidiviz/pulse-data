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
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

import recidiviz.common.constants.state.enum_canonical_strings as enum_strings
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.base_schema import Base

from recidiviz.persistence.database.schema.shared_enums import (
    gender,
    race,
    ethnicity,
    residency_status,
)

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between the master and historical tables for each entity.


state_assessment_class = Enum(
    enum_strings.state_assessment_class_mental_health,
    enum_strings.state_assessment_class_risk,
    enum_strings.state_assessment_class_security_classification,
    enum_strings.state_assessment_class_substance_abuse,
    name='state_assessment_class')

state_assessment_type = Enum(
    enum_strings.state_assessment_type_asi,
    enum_strings.state_assessment_type_lsir,
    enum_strings.state_assessment_type_oras,
    enum_strings.state_assessment_type_psa,
    name='state_assessment_type')

# TODO(1625): Add state-specific schema enums here


# Shared mixin columns
class _ReferencesStatePersonSharedColumns:
    """A mixin which defines columns for any table whose rows reference an
    individual StatePerson"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _ReferencesStatePersonSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    @declared_attr
    def person_id(self):
        return Column(
            Integer, ForeignKey('state_person.person_id'), nullable=False)


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


# StatePersonExternalId

class _StatePersonExternalIdSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StatePersonExternalId and
    StatePersonExternalIdHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StatePersonExternalIdSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)


class StatePersonExternalId(Base,
                            DatabaseEntity,
                            _StatePersonExternalIdSharedColumns):
    """Represents a state person in the SQL schema"""
    __tablename__ = 'state_person_external_id'

    person_external_id_id = Column(Integer, primary_key=True)


class StatePersonExternalIdHistory(Base,
                                   DatabaseEntity,
                                   _StatePersonExternalIdSharedColumns,
                                   _HistoryTableSharedColumns):
    """Represents the historical state of a state person external id"""
    __tablename__ = 'state_person_external_id_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_external_id_history_id = Column(Integer, primary_key=True)

    person_external_id_id = Column(
        Integer, ForeignKey(
            'state_person_external_id.person_external_id_id'),
        nullable=False, index=True)


# StatePersonRace

class _StatePersonRaceSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StatePersonRace and
    StatePersonRaceHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StatePersonRaceSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    race = Column(race)
    race_raw_text = Column(String(255))


class StatePersonRace(Base,
                      DatabaseEntity,
                      _StatePersonRaceSharedColumns):
    """Represents a state person in the SQL schema"""
    __tablename__ = 'state_person_race'

    person_race_id = Column(Integer, primary_key=True)


class StatePersonRaceHistory(Base,
                             DatabaseEntity,
                             _StatePersonRaceSharedColumns,
                             _HistoryTableSharedColumns):
    """Represents the historical state of a state person race"""
    __tablename__ = 'state_person_race_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_race_history_id = Column(Integer, primary_key=True)

    person_race_id = Column(
        Integer, ForeignKey(
            'state_person_race.person_race_id'),
        nullable=False, index=True)


# StatePersonEthnicity

class _StatePersonEthnicitySharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StatePersonEthnicity and
    StatePersonEthnicityHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StatePersonEthnicitySharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    ethnicity = Column(ethnicity)
    ethnicity_raw_text = Column(String(255))


class StatePersonEthnicity(Base,
                           DatabaseEntity,
                           _StatePersonEthnicitySharedColumns):
    """Represents a state person in the SQL schema"""
    __tablename__ = 'state_person_ethnicity'

    person_ethnicity_id = Column(Integer, primary_key=True)


class StatePersonEthnicityHistory(Base,
                                  DatabaseEntity,
                                  _StatePersonEthnicitySharedColumns,
                                  _HistoryTableSharedColumns):
    """Represents the historical state of a state person ethnicity"""
    __tablename__ = 'state_person_ethnicity_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_ethnicity_history_id = Column(Integer, primary_key=True)

    person_ethnicity_id = Column(
        Integer, ForeignKey(
            'state_person_ethnicity.person_ethnicity_id'),
        nullable=False, index=True)


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

    external_ids = relationship('StatePersonExternalId', lazy='joined')
    races = relationship('StatePersonRace', lazy='joined')
    ethnicities = relationship('StatePersonEthnicity', lazy='joined')
    assessments = relationship('StateAssessment', lazy='joined')
    sentence_groups = relationship('StateSentenceGroup', lazy='joined')


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


# StateAssessment

class _StateAssessmentSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateAssessment and
    StateAssessmentHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateAssessmentSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    # TODO(1625) Fill out StateAssessment columns


class StateAssessment(Base,
                      DatabaseEntity,
                      _StateAssessmentSharedColumns):
    """Represents an assessment in the SQL schema"""
    __tablename__ = 'assessment'

    assessment_id = Column(Integer, primary_key=True)


class StateAssessmentHistory(Base,
                             DatabaseEntity,
                             _StateAssessmentSharedColumns,
                             _HistoryTableSharedColumns):
    """Represents the historical state of an StateAssessment"""
    __tablename__ = 'assessment_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    assessment_history_id = Column(Integer, primary_key=True)

    assessment_id = Column(
        Integer, ForeignKey(
            'assessment.assessment_id'),
        nullable=False, index=True)


# StateSentenceGroup

class _StateSentenceGroupSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateSentenceGroup and
    StateSentenceGroupHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateSentenceGroupSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    # TODO(1625) Fill out StateSentenceGroup columns


class StateSentenceGroup(Base,
                         DatabaseEntity,
                         _StateSentenceGroupSharedColumns):
    """Represents a StateSentenceGroup in the SQL schema"""
    __tablename__ = 'sentence_group'

    sentence_group_id = Column(Integer, primary_key=True)


class StateSentenceGroupHistory(Base,
                                DatabaseEntity,
                                _StateSentenceGroupSharedColumns,
                                _HistoryTableSharedColumns):
    """Represents the historical state of a StateSentenceGroup"""
    __tablename__ = 'sentence_group_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    sentence_group_history_id = Column(Integer, primary_key=True)

    sentence_group_id = Column(
        Integer, ForeignKey(
            'sentence_group.sentence_group_id'),
        nullable=False, index=True)

# TODO(1625): Add further state schema entities here
