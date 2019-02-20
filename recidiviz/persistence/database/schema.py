# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Define the ORM schema objects that map directly to the database.

The below schema uses only generic SQLAlchemy types, and therefore should be
portable between database implementations.

NOTE: Many of the tables in the below schema are historical tables. The primary
key of a historical table exists only due to the requirements of SQLAlchemy,
and should not be referenced by any other table. The key which should be used
to reference a historical table is the key shared with the master table. For
the historical table, this key is non-unique. This is necessary to allow the
desired temporal table behavior. Because of this non-uniqueness, any foreign key
pointing to a historical table does NOT have a foreign key constraint. As a
result of this difference in constraints, foreign key columns cannot be shared
between tables in the "SharedColumn" mixins.
"""
from sqlalchemy import Boolean, Column, Date, DateTime, Enum, ForeignKey, \
    Integer, String, Text, UniqueConstraint
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import relationship

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.persistence.database.database_entity import DatabaseEntity

# Base class for all table classes
Base: DeclarativeMeta = declarative_base()

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between the master and historical tables for each entity.

# Person

gender = Enum(enum_strings.external_unknown,
              enum_strings.gender_female,
              enum_strings.gender_male,
              enum_strings.gender_other,
              enum_strings.gender_trans_female,
              enum_strings.gender_trans_male,
              name='gender')

race = Enum(enum_strings.race_american_indian,
            enum_strings.race_asian,
            enum_strings.race_black,
            enum_strings.external_unknown,
            enum_strings.race_hawaiian,
            enum_strings.race_other,
            enum_strings.race_white,
            name='race')

ethnicity = Enum(enum_strings.external_unknown,
                 enum_strings.ethnicity_hispanic,
                 enum_strings.ethnicity_not_hispanic,
                 name='ethnicity')

# Booking

admission_reason = Enum(enum_strings.admission_reason_escape,
                        enum_strings.admission_reason_new_commitment,
                        enum_strings.admission_reason_parole_violation,
                        enum_strings.admission_reason_probation_violation,
                        enum_strings.admission_reason_transfer,
                        name='admission_reason')

release_reason = Enum(enum_strings.release_reason_acquittal,
                      enum_strings.release_reason_bond,
                      enum_strings.release_reason_case_dismissed,
                      enum_strings.release_reason_death,
                      enum_strings.release_reason_escape,
                      enum_strings.release_reason_expiration,
                      enum_strings.external_unknown,
                      enum_strings.release_reason_recognizance,
                      enum_strings.release_reason_parole,
                      enum_strings.release_reason_probation,
                      enum_strings.release_reason_transfer,
                      name='release_reason')

custody_status = Enum(enum_strings.custody_status_escaped,
                      enum_strings.custody_status_elsewhere,
                      enum_strings.custody_status_in_custody,
                      enum_strings.custody_status_inferred_release,
                      enum_strings.custody_status_released,
                      enum_strings.unknown_found_in_source,
                      enum_strings.unknown_removed_from_source,
                      name='custody_status')

classification = Enum(enum_strings.external_unknown,
                      enum_strings.classification_high,
                      enum_strings.classification_low,
                      enum_strings.classification_maximum,
                      enum_strings.classification_medium,
                      enum_strings.classification_minimum,
                      enum_strings.classification_work_release,
                      name='classification')

# Hold

hold_status = Enum(enum_strings.hold_status_active,
                   enum_strings.hold_status_inactive,
                   enum_strings.hold_status_inferred_dropped,
                   enum_strings.unknown_found_in_source,
                   enum_strings.unknown_removed_from_source,
                   name='hold_status')

# Bond

bond_type = Enum(enum_strings.external_unknown,
                 enum_strings.bond_type_cash,
                 enum_strings.bond_type_no_bond,
                 enum_strings.bond_type_partial_cash,
                 enum_strings.bond_type_secured,
                 enum_strings.unknown_removed_from_source,
                 enum_strings.bond_type_unsecured,
                 name='bond_type')

bond_status = Enum(enum_strings.bond_status_denied,
                   enum_strings.unknown_found_in_source,
                   enum_strings.bond_status_inferred_set,
                   enum_strings.bond_status_not_required,
                   enum_strings.bond_status_pending,
                   enum_strings.bond_status_posted,
                   enum_strings.bond_status_revoked,
                   enum_strings.unknown_removed_from_source,
                   enum_strings.bond_status_set,
                   name='bond_status')

# Sentence

sentence_status = Enum(enum_strings.sentence_status_commuted,
                       enum_strings.sentence_status_completed,
                       enum_strings.sentence_status_serving,
                       enum_strings.unknown_found_in_source,
                       enum_strings.unknown_removed_from_source,
                       name='sentence_status')

# SentenceRelationship

sentence_relationship_type = Enum(
    enum_strings.sentence_relationship_type_concurrent,
    enum_strings.sentence_relationship_type_consecutive,
    name='sentence_relationship_type')

# Charge

degree = Enum(enum_strings.external_unknown,
              enum_strings.degree_first,
              enum_strings.degree_fourth,
              enum_strings.degree_second,
              enum_strings.degree_third,
              name='degree')

charge_class = Enum(enum_strings.charge_class_civil,
                    enum_strings.external_unknown,
                    enum_strings.charge_class_felony,
                    enum_strings.charge_class_infraction,
                    enum_strings.charge_class_misdemeanor,
                    enum_strings.charge_class_other,
                    enum_strings.charge_class_parole_violation,
                    enum_strings.charge_class_probation_violation,
                    name='charge_class')

charge_status = Enum(enum_strings.charge_status_acquitted,
                     enum_strings.charge_status_completed,
                     enum_strings.charge_status_convicted,
                     enum_strings.charge_status_dropped,
                     enum_strings.charge_status_inferred_dropped,
                     enum_strings.external_unknown,
                     enum_strings.charge_status_pending,
                     enum_strings.charge_status_pretrial,
                     enum_strings.charge_status_sentenced,
                     enum_strings.unknown_found_in_source,
                     enum_strings.unknown_removed_from_source,
                     name='charge_status')

court_type = Enum(enum_strings.court_type_circuit,
                  enum_strings.court_type_civil,
                  enum_strings.court_type_district,
                  enum_strings.external_unknown,
                  enum_strings.court_type_other,
                  enum_strings.court_type_superior,
                  name='court_type')

# Aggregate

report_granularity = Enum(enum_strings.daily_granularity,
                          enum_strings.weekly_granularity,
                          enum_strings.monthly_granularity,
                          enum_strings.yearly_granularity,
                          name='report_granularity')


class _PersonSharedColumns:
    """A mixin which defines all columns common to Person and PersonHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _PersonSharedColumns:
            raise Exception('_PersonSharedColumns cannot be instantiated')
        return super().__new__(cls)

    # NOTE: PersonHistory does not contain full_name or birthdate columns. This
    # is to ensure that PII is only stored in a single location (on the master
    # table) and can be easily deleted when it should no longer be stored for a
    # given individual.

    external_id = Column(String(255), index=True)
    gender = Column(gender)
    gender_raw_text = Column(String(255))
    race = Column(race)
    race_raw_text = Column(String(255))
    ethnicity = Column(ethnicity)
    ethnicity_raw_text = Column(String(255))
    place_of_residence = Column(String(255))
    region = Column(String(255), nullable=False, index=True)


class Person(Base, DatabaseEntity, _PersonSharedColumns):
    """Represents a person in the SQL schema"""
    __tablename__ = 'person'

    person_id = Column(Integer, primary_key=True)
    full_name = Column(String(255), index=True)
    birthdate = Column(Date, index=True)
    birthdate_inferred_from_age = Column(Boolean)

    bookings = relationship('Booking', back_populates='person', lazy='joined')


class PersonHistory(Base, DatabaseEntity, _PersonSharedColumns):
    """Represents the historical state of a person"""
    __tablename__ = 'person_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_history_id = Column(Integer, primary_key=True)

    person_id = Column(
        Integer, ForeignKey('person.person_id'), nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)


class _BookingSharedColumns:
    """A mixin which defines all columns common to Booking and BookingHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _BookingSharedColumns:
            raise Exception('_BookingSharedColumns cannot be instantiated')
        return super().__new__(cls)

    # NOTE: BookingHistory does not contain last_seen_time column. This is to
    # avoid needing to create a new BookingHistory entity when a booking is
    # re-scraped and no values have changed except last_scraped_date.

    external_id = Column(String(255), index=True)
    admission_date = Column(Date)
    admission_reason = Column(admission_reason)
    admission_reason_raw_text = Column(String(255))
    admission_date_inferred = Column(Boolean)
    release_date = Column(Date)
    release_date_inferred = Column(Boolean)
    projected_release_date = Column(Date)
    release_reason = Column(release_reason)
    release_reason_raw_text = Column(String(255))
    custody_status = Column(custody_status, nullable=False)
    custody_status_raw_text = Column(String(255))
    facility = Column(String(255))
    classification = Column(classification)
    classification_raw_text = Column(String(255))


class Booking(Base, DatabaseEntity, _BookingSharedColumns):
    """Represents a booking in the SQL schema"""
    __tablename__ = 'booking'

    booking_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey('person.person_id'), nullable=False)
    last_seen_time = Column(DateTime, nullable=False)

    person = relationship('Person', back_populates='bookings')

    holds = relationship('Hold', back_populates='booking', lazy='joined')
    arrest = relationship('Arrest', uselist=False, back_populates='booking',
                          lazy='joined')
    charges = relationship('Charge', back_populates='booking', lazy='joined')


class BookingHistory(Base, DatabaseEntity, _BookingSharedColumns):
    """Represents the historical state of a booking"""
    __tablename__ = 'booking_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    booking_history_id = Column(Integer, primary_key=True)

    booking_id = Column(
        Integer, ForeignKey('booking.booking_id'), nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    person_id = Column(Integer, nullable=False, index=True)


class _HoldSharedColumns:
    """A mixin which defines all columns common to Hold and HoldHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _HoldSharedColumns:
            raise Exception('_HoldSharedColumns cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    jurisdiction_name = Column(String(255))
    status = Column(hold_status, nullable=False)
    status_raw_text = Column(String(255))


class Hold(Base, DatabaseEntity, _HoldSharedColumns):
    """Represents a hold from another jurisdiction against a booking"""
    __tablename__ = 'hold'

    hold_id = Column(Integer, primary_key=True)
    booking_id = Column(
        Integer, ForeignKey('booking.booking_id'), nullable=False)

    booking = relationship('Booking', back_populates='holds')


class HoldHistory(Base, DatabaseEntity, _HoldSharedColumns):
    """Represents the historical state of a hold"""
    __tablename__ = 'hold_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    hold_history_id = Column(Integer, primary_key=True)

    hold_id = Column(
        Integer, ForeignKey('hold.hold_id'), nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    booking_id = Column(Integer, nullable=False, index=True)


class _ArrestSharedColumns:
    """A mixin which defines all columns common to Arrest and ArrestHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _ArrestSharedColumns:
            raise Exception('_ArrestSharedColumns cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    arrest_date = Column(Date)
    location = Column(String(255))
    agency = Column(String(255))
    officer_name = Column(String(255))
    officer_id = Column(String(255))


class Arrest(Base, DatabaseEntity, _ArrestSharedColumns):
    """Represents an arrest in the SQL schema"""
    __tablename__ = 'arrest'

    arrest_id = Column(Integer, primary_key=True)
    booking_id = Column(
        Integer, ForeignKey('booking.booking_id'), nullable=False)

    booking = relationship('Booking', back_populates='arrest')


class ArrestHistory(Base, DatabaseEntity, _ArrestSharedColumns):
    """Represents the historical state of an arrest"""
    __tablename__ = 'arrest_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    arrest_history_id = Column(Integer, primary_key=True)

    arrest_id = Column(
        Integer, ForeignKey('arrest.arrest_id'), nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    booking_id = Column(Integer, nullable=False, index=True)


class _BondSharedColumns:
    """A mixin which defines all columns common to Bond and BondHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _BondSharedColumns:
            raise Exception('_BondSharedColumns cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    amount_dollars = Column(Integer)
    bond_type = Column(bond_type)
    bond_type_raw_text = Column(String(255))
    status = Column(bond_status, nullable=False)
    status_raw_text = Column(String(255))
    bond_agent = Column(String(255))


class Bond(Base, DatabaseEntity, _BondSharedColumns):
    """Represents a bond in the SQL schema"""
    __tablename__ = 'bond'

    bond_id = Column(Integer, primary_key=True)
    # This foreign key is usually redundant, as a bond can be linked to a
    # booking through a charge. This foreign key serves to prevent orphaning
    # a bond if all charges that point to it are updated to point to other
    # bonds. It does not have a corresponding SQLAlchemy relationship, to avoid
    # redundant relationships.
    booking_id = Column(
        Integer, ForeignKey('booking.booking_id'), nullable=False)

    charges = relationship('Charge', back_populates='bond')


class BondHistory(Base, DatabaseEntity, _BondSharedColumns):
    """Represents the historical state of a bond"""
    __tablename__ = 'bond_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    bond_history_id = Column(Integer, primary_key=True)

    bond_id = Column(
        Integer, ForeignKey('bond.bond_id'), nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    booking_id = Column(Integer, nullable=False)


class _SentenceSharedColumns:
    """A mixin which defines all columns common to Sentence and
    SentenceHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _SentenceSharedColumns:
            raise Exception('_SentenceSharedColumns cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    status = Column(sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    sentencing_region = Column(String(255))
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)
    is_life = Column(Boolean)
    is_probation = Column(Boolean)
    is_suspended = Column(Boolean)
    fine_dollars = Column(Integer)
    parole_possible = Column(Boolean)
    post_release_supervision_length_days = Column(Integer)


class Sentence(Base, DatabaseEntity, _SentenceSharedColumns):
    """Represents a sentence in the SQL schema"""
    __tablename__ = 'sentence'

    sentence_id = Column(Integer, primary_key=True)
    # This foreign key is usually redundant, as a sentence can be linked to a
    # booking through a charge. This foreign key serves to prevent orphaning
    # a sentence if all charges that point to it are updated to point to other
    # sentences. It does not have a corresponding SQLAlchemy relationship, to
    # avoid redundant relationships.
    booking_id = Column(
        Integer, ForeignKey('booking.booking_id'), nullable=False)

    charges = relationship('Charge', back_populates='sentence')

    # Due to the SQLAlchemy requirement that both halves of an association pair
    # be represented by different relationships, a sentence must have two sets
    # of relationships with other sentences, depending on which side of the pair
    # it's on.
    related_sentences_a = association_proxy(
        'a_sentence_relations', 'sentence_a')
    related_sentences_b = association_proxy(
        'b_sentence_relations', 'sentence_b')


class SentenceHistory(Base, DatabaseEntity, _SentenceSharedColumns):
    """Represents the historical state of a sentence"""
    __tablename__ = 'sentence_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    sentence_history_id = Column(Integer, primary_key=True)

    sentence_id = Column(
        Integer,
        ForeignKey('sentence.sentence_id'),
        nullable=False,
        index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    booking_id = Column(Integer, nullable=False)


class _SentenceRelationshipSharedColumns:
    """A mixin which defines all columns common to SentenceRelationship and
    SentenceRelationshipHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _SentenceRelationshipSharedColumns:
            raise Exception(
                '_SentenceRelationshipSharedColumns cannot be instantiated')
        return super().__new__(cls)

    # Manually set name to avoid conflict with Python reserved keyword
    sentence_relationship_type = Column('type', sentence_relationship_type)
    sentence_relation_type_raw_text = Column(String(255))


class SentenceRelationship(Base, DatabaseEntity,
                           _SentenceRelationshipSharedColumns):
    """Represents the relationship between two sentences"""
    __tablename__ = 'sentence_relationship'

    # NOTE: (A,B) is equal to (B,A). There should only be one
    # SentenceRelationship for any pair of sentences.

    sentence_relationship_id = Column(Integer, primary_key=True)
    sentence_a_id = Column(
        Integer, ForeignKey('sentence.sentence_id'), nullable=False)
    sentence_b_id = Column(
        Integer, ForeignKey('sentence.sentence_id'), nullable=False)

    sentence_a = relationship(
        'Sentence',
        backref='b_sentence_relations',
        primaryjoin='Sentence.sentence_id==SentenceRelationship.sentence_b_id')
    sentence_b = relationship(
        'Sentence',
        backref='a_sentence_relations',
        primaryjoin='Sentence.sentence_id==SentenceRelationship.sentence_a_id')


class SentenceRelationshipHistory(Base, DatabaseEntity,
                                  _SentenceRelationshipSharedColumns):
    """Represents the historical state of the relationship between two sentences
    """
    __tablename__ = 'sentence_relationship_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    sentence_relationship_history_id = Column(Integer, primary_key=True)

    sentence_relationship_id = Column(
        Integer,
        ForeignKey('sentence_relationship.sentence_relationship_id'),
        nullable=False,
        index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    sentence_a_id = Column(Integer, nullable=False, index=True)
    sentence_b_id = Column(Integer, nullable=False, index=True)


class _ChargeSharedColumns:
    """A mixin which defines all columns common to Charge and ChargeHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _ChargeSharedColumns:
            raise Exception('_ChargeSharedColumns cannot be instantiated')
        return super().__new__(cls)

    external_id = Column(String(255), index=True)
    offense_date = Column(Date)
    statute = Column(String(255))
    name = Column(Text)
    attempted = Column(Boolean)
    degree = Column(degree)
    degree_raw_text = Column(String(255))
    # Manually set name to avoid conflict with Python reserved keyword
    charge_class = Column('class', charge_class)
    class_raw_text = Column(String(255))
    level = Column(String(255))
    fee_dollars = Column(Integer)
    charging_entity = Column(String(255))
    status = Column(charge_status, nullable=False)
    status_raw_text = Column(String(255))
    court_type = Column(court_type)
    court_type_raw_text = Column(String(255))
    case_number = Column(String(255))
    next_court_date = Column(Date)
    judge_name = Column(String(255))
    charge_notes = Column(Text)


class Charge(Base, DatabaseEntity, _ChargeSharedColumns):
    """Represents a charge in the SQL schema"""
    __tablename__ = 'charge'

    charge_id = Column(Integer, primary_key=True)
    booking_id = Column(
        Integer, ForeignKey('booking.booking_id'), nullable=False)
    bond_id = Column(Integer, ForeignKey('bond.bond_id'))
    sentence_id = Column(
        Integer, ForeignKey('sentence.sentence_id'))

    booking = relationship('Booking', back_populates='charges')
    bond = relationship('Bond', back_populates='charges', lazy='joined')
    sentence = relationship('Sentence', back_populates='charges', lazy='joined')


class ChargeHistory(Base, DatabaseEntity, _ChargeSharedColumns):
    """Represents the historical state of a charge"""
    __tablename__ = 'charge_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    charge_history_id = Column(Integer, primary_key=True)

    charge_id = Column(
        Integer, ForeignKey('charge.charge_id'), nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    booking_id = Column(Integer, nullable=False, index=True)
    bond_id = Column(Integer, index=True)
    sentence_id = Column(Integer, index=True)


# ==================== Aggregate Tables ====================
# Note that we generally don't aggregate any fields in the schemas below.  The
# fields are a one to one mapping from the column names in the PDF tables from
# the aggregate reports.  Any aggregation will happen later in the post
# processing.  The exception is total_population which some states did not
# include a column for, but could be calculated from multiple columns.  It is
# an important enough field that we add it as a field.

class _AggregateTableMixin:
    """A mixin which defines common fields between all Aggregate Tables."""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _AggregateTableMixin:
            raise Exception('_AggregateTableMixin cannot be instantiated')
        return super().__new__(cls)

    # Use a synthetic primary key and enforce uniqueness over a set of columns
    # (instead of setting these columns as a MultiColumn Primary Key) to allow
    # each row to be referenced by an int directly.
    record_id = Column(Integer, primary_key=True)

    # TODO(#689): Ensure that `fips` also supports facility level fips
    # TODO(#689): Consider adding fips_type to denote county vs facility
    fips = Column(String(255), nullable=False)

    report_date = Column(Date, nullable=False)
    report_granularity = Column(report_granularity, nullable=False)


class CaFacilityAggregate(Base, _AggregateTableMixin):
    """CA state-provided aggregate statistics."""
    __tablename__ = 'ca_facility_aggregate'
    __table_args__ = (
        UniqueConstraint(
            'fips', 'facility_name', 'report_date', 'report_granularity'
        ),
    )

    jurisdiction_name = Column(String(255))
    facility_name = Column(String(255))
    average_daily_population = Column(String(255))
    unsentenced_male_adp = Column(String(255))
    unsentenced_female_adp = Column(String(255))
    sentenced_male_adp = Column(String(255))
    sentenced_female_adp = Column(String(255))


class FlCountyAggregate(Base, _AggregateTableMixin):
    """FL state-provided aggregate statistics."""
    __tablename__ = 'fl_county_aggregate'
    __table_args__ = (
        UniqueConstraint('fips', 'report_date', 'report_granularity'),
    )

    county_name = Column(String(255), nullable=False)
    county_population = Column(Integer)
    average_daily_population = Column(Integer)

    # If a county fails to send updated statistics to FL State, date_reported
    # will be set with the last time valid data was add to this report.
    date_reported = Column(Date)


class FlFacilityAggregate(Base, _AggregateTableMixin):
    """FL state-provided pretrial aggregate statistics.

    Note: This 2nd FL database table is special because FL reports contain a 2nd
    table for Pretrial information by Facility.
    """
    __tablename__ = 'fl_facility_aggregate'
    __table_args__ = (
        UniqueConstraint(
            'fips', 'facility_name', 'report_date', 'report_granularity'
        ),
    )

    facility_name = Column(String(255), nullable=False)
    average_daily_population = Column(Integer)
    number_felony_pretrial = Column(Integer)
    number_misdemeanor_pretrial = Column(Integer)


class GaCountyAggregate(Base, _AggregateTableMixin):
    """GA state-provided aggregate statistics."""
    __tablename__ = 'ga_county_aggregate'
    __table_args__ = (
        UniqueConstraint('fips', 'report_date', 'report_granularity'),
    )

    county_name = Column(String(255), nullable=False)
    total_number_of_inmates_in_jail = Column(Integer)
    jail_capacity = Column(Integer)

    number_of_inmates_sentenced_to_state = Column(Integer)
    number_of_inmates_awaiting_trial = Column(Integer)  # Pretrial
    number_of_inmates_serving_county_sentence = Column(Integer)  # Sentenced
    number_of_other_inmates = Column(Integer)


class HiFacilityAggregate(Base, _AggregateTableMixin):
    """HI state-provided aggregate statistics."""
    __tablename__ = 'hi_facility_aggregate'
    __table_args__ = (
        UniqueConstraint(
            'fips', 'facility_name', 'report_date', 'report_granularity'
        ),
    )

    facility_name = Column(String(255), nullable=False)

    design_bed_capacity = Column(Integer)
    operation_bed_capacity = Column(Integer)

    total_population = Column(Integer)
    male_population = Column(Integer)
    female_population = Column(Integer)

    sentenced_felony_male_population = Column(Integer)
    sentenced_felony_female_population = Column(Integer)

    sentenced_felony_probation_male_population = Column(Integer)
    sentenced_felony_probation_female_population = Column(Integer)

    sentenced_misdemeanor_male_population = Column(Integer)
    sentenced_misdemeanor_female_population = Column(Integer)

    sentenced_pretrial_felony_male_population = Column(Integer)
    sentenced_pretrial_felony_female_population = Column(Integer)

    sentenced_pretrial_misdemeanor_male_population = Column(Integer)
    sentenced_pretrial_misdemeanor_female_population = Column(Integer)

    held_for_other_jurisdiction_male_population = Column(Integer)
    held_for_other_jurisdiction_female_population = Column(Integer)

    parole_violation_male_population = Column(Integer)
    parole_violation_female_population = Column(Integer)

    probation_violation_male_population = Column(Integer)
    probation_violation_female_population = Column(Integer)


class KyFacilityAggregate(Base, _AggregateTableMixin):
    """KY state-provided aggregate statistics."""
    __tablename__ = 'ky_county_aggregate'
    __table_args__ = (
        UniqueConstraint(
            'fips', 'facility_name', 'report_date', 'report_granularity'
        ),
    )

    facility_name = Column(String(255), nullable=False)

    total_jail_beds = Column(Integer)
    reported_population = Column(Integer)  # TODO: Is this adp or population

    male_population = Column(Integer)
    female_population = Column(Integer)

    class_d_male_population = Column(Integer)
    class_d_female_population = Column(Integer)

    community_custody_male_population = Column(Integer)
    community_custody_female_population = Column(Integer)

    alternative_sentence_male_population = Column(Integer)
    alternative_sentence_female_population = Column(Integer)

    controlled_intake_male_population = Column(Integer)
    controlled_intake_female_population = Column(Integer)

    parole_violators_male_population = Column(Integer)
    parole_violators_female_population = Column(Integer)

    federal_male_population = Column(Integer)
    federal_female_population = Column(Integer)


class NyFacilityAggregate(Base, _AggregateTableMixin):
    """NY state-provided aggregate statistics."""
    __tablename__ = 'ny_facility_aggregate'
    __table_args__ = (
        UniqueConstraint(
            'fips', 'facility_name', 'report_date', 'report_granularity'
        ),
    )

    facility_name = Column(String(255), nullable=False)

    census = Column(Integer)  # `In House` - `Boarded In` + `Boarded Out`
    in_house = Column(Integer)  # ADP of people assigned to facility
    boarded_in = Column(Integer)  # This is held_for_other_jurisdiction_adp
    boarded_out = Column(Integer)  # sent_to_other_jurisdiction_adp

    sentenced = Column(Integer)
    civil = Column(Integer)  # TODO: What is this?
    federal = Column(Integer)  # TODO: What is this?
    technical_parole_violators = Column(
        Integer)  # TODO: Can we drop 'technical'?
    state_readies = Column(Integer)  # TODO: What is this?
    other_unsentenced = Column(Integer)


class TxCountyAggregate(Base, _AggregateTableMixin):
    """TX state-provided aggregate statistics."""
    __tablename__ = 'tx_county_aggregate'
    __table_args__ = (
        UniqueConstraint(
            'fips', 'facility_name', 'report_date', 'report_granularity'
        ),
    )

    facility_name = Column(String(255), nullable=False)

    pretrial_felons = Column(Integer)

    # These 2 added are 'sentenced' as defined by Vera
    convicted_felons = Column(Integer)
    convicted_felons_sentenced_to_county_jail = Column(Integer)

    parole_violators = Column(Integer)
    parole_violators_with_new_charge = Column(Integer)

    pretrial_misdemeanor = Column(Integer)
    convicted_misdemeanor = Column(Integer)

    bench_warrants = Column(Integer)

    federal = Column(Integer)
    pretrial_sjf = Column(Integer)
    convicted_sjf_sentenced_to_county_jail = Column(Integer)
    convicted_sjf_sentenced_to_state_jail = Column(Integer)

    # We ignore Total Local, since that's the sum of above
    total_contract = Column(Integer)  # This is held_for_other_population
    total_population = Column(Integer)  # This is Total Population

    total_other = Column(Integer)

    total_capacity = Column(Integer)
    available_beds = Column(Integer)


class DcFacilityAggregate(Base, _AggregateTableMixin):
    """DC state-provided aggregate statistics."""
    __tablename__ = 'dc_facility_aggregate'
    __table_args__ = (
        UniqueConstraint('fips', 'report_date', 'report_granularity'),
    )

    facility_name = Column(String(255), nullable=False)

    total_population = Column(Integer)
    male_population = Column(Integer)
    female_population = Column(Integer)

    stsf_male_population = Column(Integer)
    stsf_female_population = Column(Integer)

    usms_gb_male_population = Column(Integer)
    usms_gb_female_population = Column(Integer)

    juvenile_male_population = Column(Integer)
    juvenile_female_population = Column(Integer)


class PaFacilityPopAggregate(Base, _AggregateTableMixin):
    """PA state-provided aggregate population statistics."""
    __tablename__ = 'pa_facility_pop_aggregate'
    __table_args__ = (
        UniqueConstraint(
            'fips', 'facility_name', 'report_date', 'report_granularity'
        ),
    )

    facility_name = Column(Integer)
    bed_capacity = Column(Integer)
    work_release_community_corrections_beds = Column(Integer)
    in_house_adp = Column(Integer)
    housed_elsewhere_adp = Column(Integer)
    work_release_adp = Column(Integer)
    admissions = Column(Integer)
    discharge = Column(Integer)
