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
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    Table)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from recidiviz.common.constants.state import (
    enum_canonical_strings as state_enum_strings
)
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.base_schema import Base
from recidiviz.persistence.database.schema.history_table_shared_columns_mixin \
    import HistoryTableSharedColumns

from recidiviz.persistence.database.schema.shared_enums import (
    gender,
    race,
    ethnicity,
    residency_status,
    bond_status,
    bond_type,
    charge_status,
    degree,
)

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between the master and historical tables for each entity.


state_assessment_class = Enum(
    state_enum_strings.state_assessment_class_mental_health,
    state_enum_strings.state_assessment_class_risk,
    state_enum_strings.state_assessment_class_security_classification,
    state_enum_strings.state_assessment_class_substance_abuse,
    name='state_assessment_class')

state_assessment_type = Enum(
    state_enum_strings.state_assessment_type_asi,
    state_enum_strings.state_assessment_type_lsir,
    state_enum_strings.state_assessment_type_oras,
    state_enum_strings.state_assessment_type_psa,
    name='state_assessment_type')

state_assessment_level = Enum(
    state_enum_strings.state_assessment_level_low,
    state_enum_strings.state_assessment_level_low_medium,
    state_enum_strings.state_assessment_level_medium,
    state_enum_strings.state_assessment_level_medium_high,
    state_enum_strings.state_assessment_level_high,
    name='state_assessment_level')

state_sentence_status = Enum(
    state_enum_strings.state_sentence_status_commuted,
    state_enum_strings.state_sentence_status_completed,
    enum_strings.external_unknown,
    enum_strings.present_without_info,
    state_enum_strings.state_sentence_status_serving,
    state_enum_strings.state_sentence_status_suspended,
    name='state_sentence_status')

state_supervision_type = Enum(
    state_enum_strings.state_supervision_type_halfway_house,
    state_enum_strings.state_supervision_type_parole,
    state_enum_strings.state_supervision_type_post_confinement,
    state_enum_strings.state_supervision_type_pre_confinement,
    state_enum_strings.state_supervision_type_probation,
    name='state_supervision_type')

state_charge_classification = Enum(
    state_enum_strings.state_charge_classification_civil,
    enum_strings.external_unknown,
    state_enum_strings.state_charge_classification_felony,
    state_enum_strings.state_charge_classification_infraction,
    state_enum_strings.state_charge_classification_misdemeanor,
    state_enum_strings.state_charge_classification_other,
    name='state_charge_classification')

state_fine_status = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_fine_status_paid,
    enum_strings.present_without_info,
    state_enum_strings.state_fine_status_unpaid,
    name='state_fine_status')

state_incarceration_type = Enum(
    state_enum_strings.state_incarceration_type_county_jail,
    state_enum_strings.state_incarceration_type_state_prison,
    name='state_incarceration_type')

state_court_case_status = Enum(
    enum_strings.external_unknown,
    # TODO(1697): Add values here
    name='state_court_case_status')

state_court_type = Enum(
    enum_strings.present_without_info,
    # TODO(1697): Add values here,
    name='state_court_type')

state_agent_type = Enum(
    state_enum_strings.state_agent_correctional_officer,
    state_enum_strings.state_agent_judge,
    state_enum_strings.state_agent_parole_board_member,
    state_enum_strings.state_agent_supervision_officer,
    state_enum_strings.state_agent_unit_supervisor,
    name='state_agent_type')

state_incarceration_period_status = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_incarceration_period_status_in_custody,
    state_enum_strings.state_incarceration_period_status_not_in_custody,
    enum_strings.present_without_info,
    name='state_incarceration_period_status')

state_incarceration_facility_security_level = Enum(
    state_enum_strings.state_incarceration_facility_security_level_maximum,
    state_enum_strings.state_incarceration_facility_security_level_medium,
    state_enum_strings.state_incarceration_facility_security_level_minimum,
    name='state_incarceration_facility_security_level')

state_incarceration_period_admission_reason = Enum(
    state_enum_strings.
    state_incarceration_period_admission_reason_new_admission,
    state_enum_strings.
    state_incarceration_period_admission_reason_parole_revocation,
    state_enum_strings.
    state_incarceration_period_admission_reason_probation_revocation,
    state_enum_strings.
    state_incarceration_period_admission_reason_return_from_escape,
    state_enum_strings.state_incarceration_period_admission_reason_transfer,
    name='state_incarceration_period_admission_reason')

state_incarceration_period_release_reason = Enum(
    state_enum_strings.
    state_incarceration_period_release_reason_conditional_release,
    state_enum_strings.state_incarceration_period_release_reason_death,
    state_enum_strings.state_incarceration_period_release_reason_escape,
    state_enum_strings.
    state_incarceration_period_release_reason_sentence_served,
    state_enum_strings.state_incarceration_period_release_reason_transfer,
    name='state_incarceration_period_release_reason'
)

state_supervision_period_status = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_supervision_period_status_terminated,
    state_enum_strings.state_supervision_period_status_under_supervision,
    enum_strings.present_without_info,
    name='state_supervision_period_status')

state_supervision_period_admission_reason = Enum(
    state_enum_strings.
    state_supervision_period_admission_reason_conditional_release,
    state_enum_strings.
    state_supervision_period_admission_reason_court_sentence,
    state_enum_strings.
    state_supervision_period_admission_reason_return_from_absconsion,
    state_enum_strings.
    state_supervision_period_admission_reason_return_from_suspension,
    name='state_supervision_period_admission_reason')

state_supervision_level = Enum(
    enum_strings.external_unknown,
    # TODO(1697): Add values here
    name='state_supervision_level')

state_supervision_period_termination_reason = Enum(
    state_enum_strings.state_supervision_period_termination_reason_absconsion,
    state_enum_strings.state_supervision_period_termination_reason_discharge,
    state_enum_strings.state_supervision_period_termination_reason_revocation,
    state_enum_strings.state_supervision_period_termination_reason_suspension,
    name='state_supervision_period_termination_reason')

state_incarceration_incident_offense = Enum(
    state_enum_strings.state_incarceration_incident_offense_contraband,
    state_enum_strings.state_incarceration_incident_offense_violent,
    name='state_incarceration_incident_offense')

state_incarceration_incident_outcome = Enum(
    state_enum_strings.state_incarceration_incident_outcome_privilege_loss,
    state_enum_strings.state_incarceration_incident_outcome_solitary,
    state_enum_strings.state_incarceration_incident_outcome_warning,
    state_enum_strings.state_incarceration_incident_outcome_write_up,
    name='state_incarceration_incident_outcome')

state_supervision_violation_type = Enum(
    state_enum_strings.state_supervision_violation_type_absconded,
    state_enum_strings.state_supervision_violation_type_felony,
    state_enum_strings.state_supervision_violation_type_misdemeanor,
    state_enum_strings.state_supervision_violation_type_municipal,
    state_enum_strings.state_supervision_violation_type_technical,
    name='state_supervision_violation_type')

state_supervision_violation_response_type = Enum(
    state_enum_strings.
    state_supervision_violation_response_type_citation,
    state_enum_strings.
    state_supervision_violation_response_type_violation_report,
    state_enum_strings.
    state_supervision_violation_response_type_permanent_decision,
    name='state_supervision_violation_response_type')

state_supervision_violation_response_decision = Enum(
    state_enum_strings.
    state_supervision_violation_response_decision_continuance,
    state_enum_strings.state_supervision_violation_response_decision_extension,
    state_enum_strings.state_supervision_violation_response_decision_revocation,
    state_enum_strings.state_supervision_violation_response_decision_suspension,
    name='state_supervision_violation_response_decision')

state_supervision_violation_response_revocation_type = Enum(
    state_enum_strings.
    state_supervision_violation_response_revocation_type_shock_incarceration,
    state_enum_strings.
    state_supervision_violation_response_revocation_type_standard,
    state_enum_strings.
    state_supervision_violation_response_revocation_type_treatment_in_prison,
    name='state_supervision_violation_response_revocation_type')

state_supervision_violation_response_deciding_body_type = Enum(
    state_enum_strings.
    state_supervision_violation_response_deciding_body_type_court,
    state_enum_strings.
    state_supervision_violation_response_deciding_body_parole_board,
    state_enum_strings.
    state_supervision_violation_response_deciding_body_type_supervision_officer,
    name='state_supervision_violation_response_deciding_body_type')

state_parole_decision_outcome = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_parole_decision_parole_denied,
    state_enum_strings.state_parole_decision_parole_granted,
    name='state_parole_decision_outcome'
)

# Join tables

state_supervision_sentence_incarceration_period_association_table = \
    Table('state_supervision_sentence_incarceration_period_association',
          Base.metadata,
          Column('supervision_sentence_id',
                 Integer,
                 ForeignKey(
                     'state_supervision_sentence.supervision_sentence_id')),
          Column('incarceration_period_id',
                 Integer,
                 ForeignKey(
                     'state_incarceration_period.incarceration_period_id')))

state_supervision_sentence_supervision_period_association_table = \
    Table('state_supervision_sentence_supervision_period_association',
          Base.metadata,
          Column('supervision_sentence_id',
                 Integer,
                 ForeignKey(
                     'state_supervision_sentence.supervision_sentence_id')),
          Column('supervision_period_id',
                 Integer,
                 ForeignKey(
                     'state_supervision_period.supervision_period_id')))

state_incarceration_sentence_incarceration_period_association_table = \
    Table('state_incarceration_sentence_incarceration_period_association',
          Base.metadata,
          Column('incarceration_sentence_id',
                 Integer,
                 ForeignKey(
                     'state_incarceration_sentence.incarceration_sentence_id')),
          Column('incarceration_period_id',
                 Integer,
                 ForeignKey(
                     'state_incarceration_period.incarceration_period_id')))

state_incarceration_sentence_supervision_period_association_table = \
    Table('state_incarceration_sentence_supervision_period_association',
          Base.metadata,
          Column('incarceration_sentence_id',
                 Integer,
                 ForeignKey(
                     'state_incarceration_sentence.incarceration_sentence_id')),
          Column('supervision_period_id',
                 Integer,
                 ForeignKey(
                     'state_supervision_period.supervision_period_id')))

state_charge_incarceration_sentence_association_table = \
    Table('state_charge_incarceration_sentence_association',
          Base.metadata,
          Column('charge_id',
                 Integer,
                 ForeignKey('state_charge.charge_id')),
          Column('incarceration_sentence_id',
                 Integer,
                 ForeignKey(
                     'state_incarceration_sentence.incarceration_sentence_id')))

state_charge_supervision_sentence_association_table = \
    Table('state_charge_supervision_sentence_association',
          Base.metadata,
          Column('charge_id',
                 Integer,
                 ForeignKey('state_charge.charge_id')),
          Column('supervision_sentence_id',
                 Integer,
                 ForeignKey(
                     'state_supervision_sentence.supervision_sentence_id')))

state_charge_fine_association_table = \
    Table('state_charge_fine_association',
          Base.metadata,
          Column('charge_id',
                 Integer,
                 ForeignKey('state_charge.charge_id')),
          Column('fine_id',
                 Integer,
                 ForeignKey(
                     'state_fine.fine_id')))

state_parole_decision_decision_agent_association_table = \
    Table('state_parole_decision_decision_agent_association',
          Base.metadata,
          Column('parole_decision_id',
                 Integer,
                 ForeignKey(
                     'state_parole_decision.parole_decision_id')),
          Column('agent_id',
                 Integer,
                 ForeignKey('state_agent.agent_id')),
          )

state_supervision_violation_response_decision_agent_association_table = \
    Table('state_supervision_violation_response_decision_agent_association',
          Base.metadata,
          Column('supervision_violation_response_id',
                 Integer,
                 ForeignKey(
                     'state_supervision_violation_response.'
                     'supervision_violation_response_id')),
          Column('agent_id',
                 Integer,
                 ForeignKey('state_agent.agent_id')),
          )


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
        return Column(Integer,
                      ForeignKey('state_person.person_id',
                                 deferrable=True,
                                 initially='DEFERRED'),
                      nullable=False)


class _ReferencesStateSentenceGroupSharedColumns:
    """A mixin which defines columns for any table whose rows reference an
    individual StateSentenceGroup"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _ReferencesStateSentenceGroupSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    @declared_attr
    def sentence_group_id(self):
        return Column(
            Integer,
            ForeignKey('state_sentence_group.sentence_group_id',
                       deferrable=True,
                       initially='DEFERRED'),
            nullable=False)


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
    id_type = Column(String(255), nullable=False)


class StatePersonExternalId(Base,
                            DatabaseEntity,
                            _StatePersonExternalIdSharedColumns):
    """Represents a StatePersonExternalId in the SQL schema"""
    __tablename__ = 'state_person_external_id'

    person_external_id_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson',
                          uselist=False,
                          back_populates='external_ids')


class StatePersonExternalIdHistory(Base,
                                   DatabaseEntity,
                                   _StatePersonExternalIdSharedColumns,
                                   HistoryTableSharedColumns):
    """Represents the historical state of a StatePersonExternalId"""
    __tablename__ = 'state_person_external_id_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_external_id_history_id = Column(Integer, primary_key=True)

    person_external_id_id = Column(
        Integer, ForeignKey(
            'state_person_external_id.person_external_id_id'),
        nullable=False, index=True)


# StatePersonAlias

class _StatePersonAliasSharedColumns(_ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to StatePersonAlias and
    StatePersonAliasHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StatePersonAliasSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    state_code = Column(String(255), nullable=False, index=True)
    full_name = Column(String(255))
    surname = Column(String(255))
    given_names = Column(String(255))
    middle_names = Column(String(255))
    name_suffix = Column(String(255))


class StatePersonAlias(Base,
                       DatabaseEntity,
                       _StatePersonAliasSharedColumns):
    """Represents a StatePersonAlias in the SQL schema"""
    __tablename__ = 'state_person_alias'

    person_alias_id = Column(Integer, primary_key=True)
    person = relationship('StatePerson',
                          uselist=False,
                          back_populates='aliases')


class StatePersonAliasHistory(Base,
                              DatabaseEntity,
                              _StatePersonAliasSharedColumns,
                              HistoryTableSharedColumns):
    """Represents the historical state of a StatePersonAlias"""
    __tablename__ = 'state_person_alias_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_alias_history_id = Column(Integer, primary_key=True)

    person_alias_id = Column(
        Integer, ForeignKey(
            'state_person_alias.person_alias_id'),
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

    state_code = Column(String(255), nullable=False, index=True)
    race = Column(race)
    race_raw_text = Column(String(255))


class StatePersonRace(Base,
                      DatabaseEntity,
                      _StatePersonRaceSharedColumns):
    """Represents a StatePersonRace in the SQL schema"""
    __tablename__ = 'state_person_race'

    person_race_id = Column(Integer, primary_key=True)
    person = relationship('StatePerson',
                          uselist=False,
                          back_populates='races')


class StatePersonRaceHistory(Base,
                             DatabaseEntity,
                             _StatePersonRaceSharedColumns,
                             HistoryTableSharedColumns):
    """Represents the historical state of a StatePersonRace"""
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

    state_code = Column(String(255), nullable=False, index=True)
    ethnicity = Column(ethnicity)
    ethnicity_raw_text = Column(String(255))


class StatePersonEthnicity(Base,
                           DatabaseEntity,
                           _StatePersonEthnicitySharedColumns):
    """Represents a state person in the SQL schema"""
    __tablename__ = 'state_person_ethnicity'

    person_ethnicity_id = Column(Integer, primary_key=True)
    person = relationship('StatePerson',
                          uselist=False,
                          back_populates='ethnicities')


class StatePersonEthnicityHistory(Base,
                                  DatabaseEntity,
                                  _StatePersonEthnicitySharedColumns,
                                  HistoryTableSharedColumns):
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

    birthdate = Column(Date, index=True)
    birthdate_inferred_from_age = Column(Boolean)

    gender = Column(gender)
    gender_raw_text = Column(String(255))

    residency_status = Column(residency_status)


class StatePerson(Base, DatabaseEntity, _StatePersonSharedColumns):
    """Represents a StatePerson in the state SQL schema"""
    __tablename__ = 'state_person'

    person_id = Column(Integer, primary_key=True)

    external_ids = \
        relationship('StatePersonExternalId', back_populates='person')
    aliases = relationship('StatePersonAlias', back_populates='person')
    races = relationship('StatePersonRace', back_populates='person')
    ethnicities = relationship('StatePersonEthnicity', back_populates='person')
    assessments = relationship('StateAssessment', back_populates='person')
    sentence_groups = \
        relationship('StateSentenceGroup', back_populates='person')


class StatePersonHistory(Base,
                         DatabaseEntity,
                         _StatePersonSharedColumns,
                         HistoryTableSharedColumns):

    """Represents the historical state of a StatePerson"""
    __tablename__ = 'state_person_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_history_id = Column(Integer, primary_key=True)

    person_id = Column(
        Integer, ForeignKey('state_person.person_id'),
        nullable=False, index=True)


# StateBond

class _StateBondSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateBond and
    StateBond"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateBondSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(bond_status, nullable=False)
    status_raw_text = Column(String(255))
    bond_type = Column(bond_type, nullable=False)
    bond_type_raw_text = Column(String(255))
    date_paid = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    amount_dollars = Column(Integer)
    bond_agent = Column(String(255))


class StateBond(Base,
                DatabaseEntity,
                _StateBondSharedColumns):
    """Represents a StateBond in the SQL schema"""
    __tablename__ = 'state_bond'

    bond_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    charges = relationship('StateCharge', back_populates='bond')


class StateBondHistory(Base,
                       DatabaseEntity,
                       _StateBondSharedColumns,
                       HistoryTableSharedColumns):
    """Represents the historical state of a StateBond"""
    __tablename__ = 'state_bond_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    bond_history_id = Column(Integer, primary_key=True)

    bond_id = Column(
        Integer, ForeignKey('state_bond.bond_id'),
        nullable=False,
        index=True)


# StateCourtCase

class _StateCourtCaseSharedColumns(_ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to StateCourtCase and
    StateCourtCaseHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateCourtCaseSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(state_court_case_status)
    status_raw_text = Column(String(255))
    court_type = Column(state_court_type)
    court_type_raw_text = Column(String(255))
    date_convicted = Column(Date)
    next_court_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    court_fee_dollars = Column(Integer)

    @declared_attr
    def judge_id(self):
        return Column(
            Integer,
            ForeignKey('state_agent.agent_id'),
            nullable=True)


class StateCourtCase(Base,
                     DatabaseEntity,
                     _StateCourtCaseSharedColumns):
    """Represents a StateCourtCase in the SQL schema"""
    __tablename__ = 'state_court_case'

    court_case_id = Column(Integer, primary_key=True)
    person = relationship('StatePerson', uselist=False)
    charges = relationship('StateCharge',
                           back_populates='court_case')
    judge = relationship('StateAgent', uselist=False)


class StateCourtCaseHistory(Base,
                            DatabaseEntity,
                            _StateCourtCaseSharedColumns,
                            HistoryTableSharedColumns):
    """Represents the historical state of a StateCourtCase"""
    __tablename__ = 'state_court_case_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    court_case_history_id = Column(Integer, primary_key=True)

    court_case_id = Column(
        Integer, ForeignKey(
            'state_court_case.court_case_id'),
        nullable=False, index=True)


# StateCharge

class _StateChargeSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateCharge and
    StateChargeHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateChargeSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(charge_status, nullable=False)
    status_raw_text = Column(String(255))
    offense_date = Column(Date)
    date_charged = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    statute = Column(String(255))
    description = Column(Text)
    attempted = Column(Boolean)
    charge_classification = Column(state_charge_classification)
    charge_classification_raw_text = Column(String(255))
    degree = Column(degree)
    degree_raw_text = Column(String(255))
    counts = Column(Integer)
    charge_notes = Column(Text)
    charging_entity = Column(String(255))

    @declared_attr
    def court_case_id(self):
        return Column(Integer, ForeignKey('state_court_case.court_case_id'))

    @declared_attr
    def bond_id(self):
        return Column(Integer, ForeignKey('state_bond.bond_id'))


class StateCharge(Base,
                  DatabaseEntity,
                  _StateChargeSharedColumns):
    """Represents a StateCharge in the SQL schema"""
    __tablename__ = 'state_charge'

    charge_id = Column(Integer, primary_key=True)

    # Cross-entity relationships
    person = relationship('StatePerson', uselist=False)
    court_case = relationship('StateCourtCase',
                              uselist=False,
                              back_populates='charges')
    bond = relationship('StateBond',
                        uselist=False,
                        back_populates='charges')
    incarceration_sentences = relationship(
        'StateIncarcerationSentence',
        secondary=state_charge_incarceration_sentence_association_table,
        back_populates='charges')
    supervision_sentences = relationship(
        'StateSupervisionSentence',
        secondary=state_charge_supervision_sentence_association_table,
        back_populates='charges')
    fines = relationship(
        'StateFine',
        secondary=state_charge_fine_association_table,
        back_populates='charges')


class StateChargeHistory(Base,
                         DatabaseEntity,
                         _StateChargeSharedColumns,
                         HistoryTableSharedColumns):
    """Represents the historical state of a StateCharge"""
    __tablename__ = 'state_charge_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    charge_history_id = Column(Integer, primary_key=True)

    charge_id = Column(
        Integer, ForeignKey('state_charge.charge_id'),
        nullable=False,
        index=True)


# StateAssessment

class _StateAssessmentSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateAssessment and
    StateAssessmentHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateAssessmentSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    assessment_class = Column(state_assessment_class)
    assessment_class_raw_text = Column(String(255))
    assessment_type = Column(state_assessment_type)
    assessment_type_raw_text = Column(String(255))
    assessment_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    assessment_score = Column(Integer)
    assessment_level = Column(state_assessment_level)
    assessment_level_raw_text = Column(String(255))
    assessment_metadata = Column(Text)

    @declared_attr
    def incarceration_period_id(self):
        return Column(
            Integer,
            ForeignKey('state_incarceration_period.incarceration_period_id'),
            nullable=True)

    @declared_attr
    def supervision_period_id(self):
        return Column(
            Integer,
            ForeignKey('state_supervision_period.supervision_period_id'),
            nullable=True)

    @declared_attr
    def conducting_agent_id(self):
        return Column(
            Integer,
            ForeignKey('state_agent.agent_id'),
            nullable=True)


class StateAssessment(Base,
                      DatabaseEntity,
                      _StateAssessmentSharedColumns):
    """Represents a StateAssessment in the SQL schema"""
    __tablename__ = 'state_assessment'

    assessment_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson',
                          uselist=False,
                          back_populates='assessments')
    incarceration_period = relationship('StateIncarcerationPeriod',
                                        uselist=False,
                                        back_populates='assessments')
    supervision_period = relationship('StateSupervisionPeriod',
                                      uselist=False,
                                      back_populates='assessments')
    conducting_agent = relationship('StateAgent', uselist=False)


class StateAssessmentHistory(Base,
                             DatabaseEntity,
                             _StateAssessmentSharedColumns,
                             HistoryTableSharedColumns):
    """Represents the historical state of a StateAssessment"""
    __tablename__ = 'state_assessment_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    assessment_history_id = Column(Integer, primary_key=True)

    assessment_id = Column(
        Integer, ForeignKey(
            'state_assessment.assessment_id'),
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

    external_id = Column(String(255), index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    date_imposed = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)


class StateSentenceGroup(Base,
                         DatabaseEntity,
                         _StateSentenceGroupSharedColumns):
    """Represents a StateSentenceGroup in the SQL schema"""
    __tablename__ = 'state_sentence_group'

    sentence_group_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson',
                          uselist=False,
                          back_populates='sentence_groups')
    supervision_sentences = relationship('StateSupervisionSentence',
                                         back_populates='sentence_group')
    incarceration_sentences = relationship('StateIncarcerationSentence',
                                           back_populates='sentence_group')
    fines = relationship('StateFine',
                         back_populates='sentence_group')


class StateSentenceGroupHistory(Base,
                                DatabaseEntity,
                                _StateSentenceGroupSharedColumns,
                                HistoryTableSharedColumns):
    """Represents the historical state of a StateSentenceGroup"""
    __tablename__ = 'state_sentence_group_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    sentence_group_history_id = Column(Integer, primary_key=True)

    sentence_group_id = Column(
        Integer, ForeignKey(
            'state_sentence_group.sentence_group_id'),
        nullable=False, index=True)


# StateSupervisionSentence

class _StateSupervisionSentenceSharedColumns(
        _ReferencesStatePersonSharedColumns,
        _ReferencesStateSentenceGroupSharedColumns):
    """A mixin which defines all columns common to StateSupervisionSentence and
    StateSupervisionSentenceHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateSupervisionSentenceSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    supervision_type = Column(state_supervision_type)
    supervision_type_raw_text = Column(String(255))
    projected_completion_date = Column(Date)
    completion_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)


class StateSupervisionSentence(Base,
                               DatabaseEntity,
                               _StateSupervisionSentenceSharedColumns):
    """Represents a StateSupervisionSentence in the SQL schema"""
    __tablename__ = 'state_supervision_sentence'

    supervision_sentence_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    sentence_group = relationship('StateSentenceGroup',
                                  back_populates='supervision_sentences',
                                  uselist=False)
    charges = relationship(
        'StateCharge',
        secondary=state_charge_supervision_sentence_association_table,
        back_populates='supervision_sentences')
    incarceration_periods = relationship(
        'StateIncarcerationPeriod',
        secondary=
        state_supervision_sentence_incarceration_period_association_table,
        back_populates='supervision_sentences')
    supervision_periods = relationship(
        'StateSupervisionPeriod',
        secondary=
        state_supervision_sentence_supervision_period_association_table,
        back_populates='supervision_sentences')


class StateSupervisionSentenceHistory(Base,
                                      DatabaseEntity,
                                      _StateSupervisionSentenceSharedColumns,
                                      HistoryTableSharedColumns):
    """Represents the historical state of a StateSupervisionSentence"""
    __tablename__ = 'state_supervision_sentence_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_sentence_history_id = Column(Integer, primary_key=True)

    supervision_sentence_id = Column(
        Integer, ForeignKey(
            'state_supervision_sentence.supervision_sentence_id'),
        nullable=False, index=True)


# StateIncarcerationSentence

class _StateIncarcerationSentenceSharedColumns(
        _ReferencesStatePersonSharedColumns,
        _ReferencesStateSentenceGroupSharedColumns):
    """
    A mixin which defines all columns common to StateIncarcerationSentence and
    StateIncarcerationSentenceHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateIncarcerationSentenceSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    incarceration_type = Column(state_incarceration_type)
    incarceration_type_raw_text = Column(String(255))
    date_imposed = Column(Date)
    projected_min_release_date = Column(Date)
    projected_max_release_date = Column(Date)
    parole_eligibility_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)
    is_life = Column(Boolean)
    parole_possible = Column(Boolean)
    initial_time_served_days = Column(Integer)
    good_time_days = Column(Integer)
    earned_time_days = Column(Integer)


class StateIncarcerationSentence(Base,
                                 DatabaseEntity,
                                 _StateIncarcerationSentenceSharedColumns):
    """Represents a StateIncarcerationSentence in the SQL schema"""
    __tablename__ = 'state_incarceration_sentence'

    incarceration_sentence_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    sentence_group = relationship('StateSentenceGroup',
                                  back_populates='incarceration_sentences',
                                  uselist=False)
    charges = relationship(
        'StateCharge',
        secondary=state_charge_incarceration_sentence_association_table,
        back_populates='incarceration_sentences')
    incarceration_periods = relationship(
        'StateIncarcerationPeriod',
        secondary=
        state_incarceration_sentence_incarceration_period_association_table,
        back_populates='incarceration_sentences')

    supervision_periods = relationship(
        'StateSupervisionPeriod',
        secondary=
        state_incarceration_sentence_supervision_period_association_table,
        back_populates='incarceration_sentences')


class StateIncarcerationSentenceHistory(
        Base,
        DatabaseEntity,
        _StateIncarcerationSentenceSharedColumns,
        HistoryTableSharedColumns):
    """Represents the historical state of a StateIncarcerationSentence"""
    __tablename__ = 'state_incarceration_sentence_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    incarceration_sentence_history_id = Column(Integer, primary_key=True)

    incarceration_sentence_id = Column(
        Integer, ForeignKey(
            'state_incarceration_sentence.incarceration_sentence_id'),
        nullable=False, index=True)


# StateFine

class _StateFineSharedColumns(
        _ReferencesStatePersonSharedColumns,
        _ReferencesStateSentenceGroupSharedColumns):
    """
    A mixin which defines all columns common to StateFine and StateFineHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateFineSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(state_fine_status, nullable=False)
    status_raw_text = Column(String(255))
    date_paid = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    fine_dollars = Column(Integer)


class StateFine(Base,
                DatabaseEntity,
                _StateFineSharedColumns):
    """Represents a StateFine in the SQL schema"""
    __tablename__ = 'state_fine'

    fine_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    sentence_group = relationship('StateSentenceGroup',
                                  back_populates='fines',
                                  uselist=False)
    charges = relationship(
        'StateCharge',
        secondary=state_charge_fine_association_table,
        back_populates='fines')


class StateFineHistory(Base,
                       DatabaseEntity,
                       _StateFineSharedColumns,
                       HistoryTableSharedColumns):
    """Represents the historical state of a StateFine"""
    __tablename__ = 'state_fine_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    fine_history_id = Column(Integer, primary_key=True)

    fine_id = Column(
        Integer, ForeignKey(
            'state_fine.fine_id'),
        nullable=False, index=True)


# StateIncarcerationPeriod

class _StateIncarcerationPeriodSharedColumns(
        _ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to StateIncarcerationPeriod and
    StateIncarcerationPeriodHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateIncarcerationPeriodSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(state_incarceration_period_status, nullable=False)
    status_raw_text = Column(String(255))
    incarceration_type = Column(state_incarceration_type)
    incarceration_type_raw_text = Column(String(255))
    admission_date = Column(Date)
    release_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    facility = Column(String(255))
    housing_unit = Column(String(255))
    facility_security_level = \
        Column(state_incarceration_facility_security_level)
    facility_security_level_raw_text = Column(String(255))
    admission_reason = \
        Column(state_incarceration_period_admission_reason)
    admission_reason_raw_text = Column(String(255))
    projected_release_reason = \
        Column(state_incarceration_period_release_reason)
    projected_release_reason_raw_text = Column(String(255))
    release_reason = \
        Column(state_incarceration_period_release_reason)
    release_reason_raw_text = Column(String(255))

    @declared_attr
    def source_supervision_violation_response_id(self):
        return Column(
            Integer,
            ForeignKey('state_supervision_violation_response.'
                       'supervision_violation_response_id'),
            nullable=True)


class StateIncarcerationPeriod(Base,
                               DatabaseEntity,
                               _StateIncarcerationPeriodSharedColumns):
    """Represents a StateIncarcerationPeriod in the SQL schema"""
    __tablename__ = 'state_incarceration_period'

    incarceration_period_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    incarceration_sentences = relationship(
        'StateIncarcerationSentence',
        secondary=
        state_incarceration_sentence_incarceration_period_association_table,
        back_populates='incarceration_periods')
    supervision_sentences = relationship(
        'StateSupervisionSentence',
        secondary=
        state_supervision_sentence_incarceration_period_association_table,
        back_populates='incarceration_periods')
    incarceration_incidents = relationship(
        'StateIncarcerationIncident',
        back_populates='incarceration_period')
    parole_decisions = relationship(
        'StateParoleDecision',
        back_populates='incarceration_period')
    assessments = relationship('StateAssessment',
                               back_populates='incarceration_period')

    source_supervision_violation_response = \
        relationship('StateSupervisionViolationResponse',
                     uselist=False)


class StateIncarcerationPeriodHistory(Base,
                                      DatabaseEntity,
                                      _StateIncarcerationPeriodSharedColumns,
                                      HistoryTableSharedColumns):
    """Represents the historical state of a StateIncarcerationPeriod"""
    __tablename__ = 'state_incarceration_period_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    incarceration_period_history_id = Column(Integer, primary_key=True)

    incarceration_period_id = Column(
        Integer, ForeignKey(
            'state_incarceration_period.incarceration_period_id'),
        nullable=False, index=True)


# StateSupervisionPeriod

class _StateSupervisionPeriodSharedColumns(_ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to StateSupervisionPeriod and
    StateSupervisionPeriodHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateSupervisionPeriodSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(state_supervision_period_status, nullable=False)
    status_raw_text = Column(String(255))
    supervision_type = Column(state_supervision_type)
    supervision_type_raw_text = Column(String(255))
    start_date = Column(Date)
    termination_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    admission_reason = \
        Column(state_supervision_period_admission_reason)
    admission_reason_raw_text = Column(String(255))
    termination_reason = \
        Column(state_supervision_period_termination_reason)
    termination_reason_raw_text = Column(String(255))
    supervision_level = \
        Column(state_supervision_level)
    supervision_level_raw_text = Column(String(255))
    conditions = Column(String(255))


class StateSupervisionPeriod(Base,
                             DatabaseEntity,
                             _StateSupervisionPeriodSharedColumns):
    """Represents a StateSupervisionPeriod in the SQL schema"""
    __tablename__ = 'state_supervision_period'

    supervision_period_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    incarceration_sentences = relationship(
        'StateIncarcerationSentence',
        secondary=
        state_incarceration_sentence_supervision_period_association_table,
        back_populates='supervision_periods')
    supervision_sentences = relationship(
        'StateSupervisionSentence',
        secondary=
        state_supervision_sentence_supervision_period_association_table,
        back_populates='supervision_periods')
    supervision_violations = relationship(
        'StateSupervisionViolation',
        back_populates='supervision_period')
    assessments = relationship('StateAssessment',
                               back_populates='supervision_period')


class StateSupervisionPeriodHistory(Base,
                                    DatabaseEntity,
                                    _StateSupervisionPeriodSharedColumns,
                                    HistoryTableSharedColumns):
    """Represents the historical state of a StateSupervisionPeriod"""
    __tablename__ = 'state_supervision_period_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_period_history_id = Column(Integer, primary_key=True)

    supervision_period_id = Column(
        Integer, ForeignKey(
            'state_supervision_period.supervision_period_id'),
        nullable=False, index=True)


# StateIncarcerationIncident

class _StateIncarcerationIncidentSharedColumns(
        _ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to StateIncarcerationIncident and
    StateIncarcerationIncidentHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateIncarcerationIncidentSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    incident_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    location_within_facility = Column(String(255))
    offense = Column(state_incarceration_incident_offense)
    offense_raw_text = Column(String(255))
    outcome = Column(state_incarceration_incident_outcome)
    outcome_raw_text = Column(String(255))

    @declared_attr
    def incarceration_period_id(self):
        return Column(
            Integer,
            ForeignKey('state_incarceration_period.incarceration_period_id'),
            nullable=True)

    @declared_attr
    def responding_officer_id(self):
        return Column(
            Integer,
            ForeignKey('state_agent.agent_id'),
            nullable=True)


class StateIncarcerationIncident(Base,
                                 DatabaseEntity,
                                 _StateIncarcerationIncidentSharedColumns):
    """Represents a StateIncarcerationIncident in the SQL schema"""
    __tablename__ = 'state_incarceration_incident'

    incarceration_incident_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    responding_officer = relationship('StateAgent',
                                      uselist=False)
    incarceration_period = relationship(
        'StateIncarcerationPeriod',
        uselist=False,
        back_populates='incarceration_incidents')


class StateIncarcerationIncidentHistory(
        Base,
        DatabaseEntity,
        _StateIncarcerationIncidentSharedColumns,
        HistoryTableSharedColumns):
    """Represents the historical state of a StateIncarcerationIncident"""
    __tablename__ = 'state_incarceration_incident_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    incarceration_incident_history_id = Column(Integer, primary_key=True)

    incarceration_incident_id = Column(
        Integer, ForeignKey(
            'state_incarceration_incident.incarceration_incident_id'),
        nullable=False, index=True)


# StateParoleDecision

class _StateParoleDecisionSharedColumns(
        _ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to StateParoleDecision and
    StateParoleDecisionHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateParoleDecisionSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)

    received_parole = Column(Boolean)
    decision_date = Column(Date)
    corrective_action_deadline = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    decision_outcome = Column(state_parole_decision_outcome)
    decision_outcome_raw_text = Column(String(255))
    decision_reasoning = Column(String(255))
    corrective_action = Column(String(255))

    @declared_attr
    def incarceration_period_id(self):
        return Column(
            Integer,
            ForeignKey('state_incarceration_period.incarceration_period_id'),
            nullable=True)


class StateParoleDecision(Base,
                          DatabaseEntity,
                          _StateParoleDecisionSharedColumns):
    """Represents a StateParoleDecision in the SQL schema"""
    __tablename__ = 'state_parole_decision'

    parole_decision_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    incarceration_period = relationship(
        'StateIncarcerationPeriod',
        uselist=False,
        back_populates='parole_decisions')
    decision_agents = relationship(
        'StateAgent',
        secondary=state_parole_decision_decision_agent_association_table)


class StateParoleDecisionHistory(Base,
                                 DatabaseEntity,
                                 _StateParoleDecisionSharedColumns,
                                 HistoryTableSharedColumns):
    """Represents the historical state of a StateParoleDecision"""
    __tablename__ = 'state_parole_decision_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    parole_decision_history_id = Column(Integer, primary_key=True)

    parole_decision_id = Column(
        Integer, ForeignKey(
            'state_parole_decision.parole_decision_id'),
        nullable=False, index=True)


# StateSupervisionViolation

class _StateSupervisionViolationSharedColumns(
        _ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to StateSupervisionViolation and
    StateSupervisionViolationHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateSupervisionViolationSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    violation_type = Column(state_supervision_violation_type)
    violation_type_raw_text = Column(String(255))
    violation_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    is_violent = Column(Boolean)
    violated_conditions = Column(String(255))

    @declared_attr
    def supervision_period_id(self):
        return Column(
            Integer,
            ForeignKey('state_supervision_period.supervision_period_id'),
            nullable=True)


class StateSupervisionViolation(Base,
                                DatabaseEntity,
                                _StateSupervisionViolationSharedColumns):
    """Represents a StateSupervisionViolation in the SQL schema"""
    __tablename__ = 'state_supervision_violation'

    supervision_violation_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    supervision_period = relationship(
        'StateSupervisionPeriod',
        uselist=False,
        back_populates='supervision_violations')
    supervision_violation_responses = relationship(
        'StateSupervisionViolationResponse',
        back_populates='supervision_violation')


class StateSupervisionViolationHistory(Base,
                                       DatabaseEntity,
                                       _StateSupervisionViolationSharedColumns,
                                       HistoryTableSharedColumns):
    """Represents the historical state of a StateSupervisionViolation"""
    __tablename__ = 'state_supervision_violation_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_violation_history_id = Column(Integer, primary_key=True)

    supervision_violation_id = Column(
        Integer, ForeignKey(
            'state_supervision_violation.supervision_violation_id'),
        nullable=False, index=True)


# StateSupervisionViolationResponse

class _StateSupervisionViolationResponseSharedColumns(
        _ReferencesStatePersonSharedColumns):
    """
    A mixin which defines all columns common to
    StateSupervisionViolationResponse and
    StateSupervisionViolationResponseHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateSupervisionViolationResponseSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    response_type = Column(state_supervision_violation_response_type)
    response_type_raw_text = Column(String(255))
    response_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    decision = Column(state_supervision_violation_response_decision)
    decision_raw_text = Column(String(255))
    revocation_type = \
        Column(state_supervision_violation_response_revocation_type)
    revocation_type_raw_text = Column(String(255))
    deciding_body_type = \
        Column(state_supervision_violation_response_deciding_body_type)
    deciding_body_type_raw_text = Column(String(255))

    @declared_attr
    def supervision_violation_id(self):
        return Column(
            Integer,
            ForeignKey('state_supervision_violation.supervision_violation_id'),
            nullable=True)


class StateSupervisionViolationResponse(
        Base,
        DatabaseEntity,
        _StateSupervisionViolationResponseSharedColumns):
    """Represents a StateSupervisionViolationResponse in the SQL schema"""
    __tablename__ = 'state_supervision_violation_response'

    supervision_violation_response_id = Column(Integer, primary_key=True)

    person = relationship('StatePerson', uselist=False)
    supervision_violation = relationship(
        'StateSupervisionViolation',
        uselist=False,
        back_populates='supervision_violation_responses')
    decision_agents = relationship(
        'StateAgent',
        secondary=
        state_supervision_violation_response_decision_agent_association_table)


class StateSupervisionViolationResponseHistory(
        Base,
        DatabaseEntity,
        _StateSupervisionViolationResponseSharedColumns,
        HistoryTableSharedColumns):
    """Represents the historical state of a StateSupervisionViolationResponse"""
    __tablename__ = 'state_supervision_violation_response_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_violation_response_history_id = Column(Integer,
                                                       primary_key=True)

    supervision_violation_response_id = Column(
        Integer, ForeignKey(
            'state_supervision_violation_response.'
            'supervision_violation_response_id'),
        nullable=False, index=True)


# StateAgent

class _StateAgentSharedColumns:
    """
    A mixin which defines all columns common to StateAgent and
    StateAgentHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _StateAgentSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    agent_type = Column(state_agent_type, nullable=False)
    agent_type_raw_text = Column(String(255))
    state_code = Column(String(255), nullable=False, index=True)
    full_name = Column(String(255))


class StateAgent(Base,
                 DatabaseEntity,
                 _StateAgentSharedColumns):
    """Represents a StateAgent in the SQL schema"""
    __tablename__ = 'state_agent'

    agent_id = Column(Integer, primary_key=True)


class StateAgentHistory(Base,
                        DatabaseEntity,
                        _StateAgentSharedColumns,
                        HistoryTableSharedColumns):
    """Represents the historical state of a StateAgent"""
    __tablename__ = 'state_agent_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    agent_history_id = Column(Integer, primary_key=True)

    agent_id = Column(
        Integer, ForeignKey(
            'state_agent.agent_id'),
        nullable=False, index=True)
