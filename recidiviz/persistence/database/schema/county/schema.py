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
for county-level entities.

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
from sqlalchemy import Boolean, CheckConstraint, Column, Date, DateTime, Enum, \
    ForeignKey, Integer, String, Text
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship, validates

from recidiviz.common.constants.county import (
    enum_canonical_strings as county_enum_strings
)
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.persistence.database.base_schema import JailsBase
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
)

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between the master and historical tables for each entity.

# Booking

admission_reason = Enum(county_enum_strings.admission_reason_escape,
                        county_enum_strings.admission_reason_new_commitment,
                        county_enum_strings.admission_reason_parole_violation,
                        county_enum_strings.admission_reason_probation_violation,
                        county_enum_strings.admission_reason_supervision_violation_for_sex_offense,
                        county_enum_strings.admission_reason_transfer,
                        name='admission_reason')

release_reason = Enum(county_enum_strings.release_reason_acquittal,
                      county_enum_strings.release_reason_bond,
                      county_enum_strings.release_reason_case_dismissed,
                      county_enum_strings.release_reason_death,
                      county_enum_strings.release_reason_escape,
                      county_enum_strings.release_reason_expiration,
                      enum_strings.external_unknown,
                      county_enum_strings.release_reason_recognizance,
                      county_enum_strings.release_reason_parole,
                      county_enum_strings.release_reason_probation,
                      county_enum_strings.release_reason_transfer,
                      name='release_reason')

custody_status = Enum(county_enum_strings.custody_status_escaped,
                      county_enum_strings.custody_status_elsewhere,
                      county_enum_strings.custody_status_in_custody,
                      county_enum_strings.custody_status_inferred_release,
                      county_enum_strings.custody_status_released,
                      enum_strings.present_without_info,
                      enum_strings.removed_without_info,
                      name='custody_status')

classification = Enum(enum_strings.external_unknown,
                      county_enum_strings.classification_high,
                      county_enum_strings.classification_low,
                      county_enum_strings.classification_maximum,
                      county_enum_strings.classification_medium,
                      county_enum_strings.classification_minimum,
                      county_enum_strings.classification_work_release,
                      name='classification')

# Hold

hold_status = Enum(county_enum_strings.hold_status_active,
                   county_enum_strings.hold_status_inactive,
                   county_enum_strings.hold_status_inferred_dropped,
                   enum_strings.present_without_info,
                   enum_strings.removed_without_info,
                   name='hold_status')

# Sentence

sentence_status = Enum(county_enum_strings.sentence_status_commuted,
                       county_enum_strings.sentence_status_completed,
                       county_enum_strings.sentence_status_serving,
                       enum_strings.present_without_info,
                       enum_strings.removed_without_info,
                       name='sentence_status')

# SentenceRelationship

sentence_relationship_type = Enum(
    county_enum_strings.sentence_relationship_type_concurrent,
    county_enum_strings.sentence_relationship_type_consecutive,
    name='sentence_relationship_type')

charge_class = Enum(county_enum_strings.charge_class_civil,
                    enum_strings.external_unknown,
                    county_enum_strings.charge_class_felony,
                    county_enum_strings.charge_class_infraction,
                    county_enum_strings.charge_class_misdemeanor,
                    county_enum_strings.charge_class_other,
                    county_enum_strings.charge_class_parole_violation,
                    county_enum_strings.charge_class_probation_violation,
                    county_enum_strings.charge_class_supervision_violation_for_sex_offense,
                    name='charge_class')

degree = Enum(enum_strings.external_unknown,
              county_enum_strings.degree_first,
              county_enum_strings.degree_fourth,
              county_enum_strings.degree_second,
              county_enum_strings.degree_third,
              name='degree')


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
    residency_status = Column(residency_status)
    resident_of_region = Column(Boolean)
    region = Column(String(255), nullable=False, index=True)
    jurisdiction_id = Column(String(8), nullable=False)

    @validates('jurisdiction_id')
    def validate_jurisdiction_id(self, _, jurisdiction_id: str) -> str:
        if len(jurisdiction_id) != 8:
            raise ValueError(
                'Jurisdiction ID invalid length: {} characters, should be '
                '8'.format(len(jurisdiction_id)))
        return jurisdiction_id


class Person(JailsBase, _PersonSharedColumns):
    """Represents a person in the SQL schema"""
    __tablename__ = 'person'
    __table_args__ = (
        CheckConstraint(
            'LENGTH(jurisdiction_id) = 8',
            name='person_jurisdiction_id_length_check'),
    )

    person_id = Column(Integer, primary_key=True)
    full_name = Column(String(255), index=True)
    birthdate = Column(Date, index=True)
    birthdate_inferred_from_age = Column(Boolean)

    bookings = relationship('Booking', lazy='joined')


class PersonHistory(JailsBase,
                    _PersonSharedColumns,
                    HistoryTableSharedColumns):
    """Represents the historical state of a person"""
    __tablename__ = 'person_history'
    __table_args__ = (
        CheckConstraint(
            'LENGTH(jurisdiction_id) = 8',
            name='person_history_jurisdiction_id_length_check'),
    )

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_history_id = Column(Integer, primary_key=True)

    person_id = Column(
        Integer, ForeignKey('person.person_id'), nullable=False, index=True)


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
    #
    # BookingHistory also does not contain first_seen_time column. This is
    # because this value should never change, so it does not reflect the state
    # at a single point in time.

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
    facility_id = Column(String(16), nullable=True)
    classification = Column(classification)
    classification_raw_text = Column(String(255))

    @declared_attr
    def person_id(self):
        return Column(Integer, ForeignKey('person.person_id'), nullable=False)

    @validates('facility_id')
    def validate_facility_id(self, _, facility_id: str) -> str:
        if facility_id and len(facility_id) != 16:
            raise ValueError(
                'Facility ID invalid length: {} characters, should be '
                '16'.format(len(facility_id)))
        return facility_id

class Booking(JailsBase, _BookingSharedColumns):
    """Represents a booking in the SQL schema"""
    __tablename__ = 'booking'
    __table_args__ = (
        CheckConstraint(
            'LENGTH(facility_id) = 16',
            name='booking_facility_id_length_check'),
    )

    booking_id = Column(Integer, primary_key=True)
    last_seen_time = Column(DateTime, nullable=False)
    first_seen_time = Column(DateTime, nullable=False)

    holds = relationship('Hold', lazy='joined')
    arrest = relationship('Arrest', uselist=False, lazy='joined')
    charges = relationship('Charge', lazy='joined')


class BookingHistory(JailsBase,
                     _BookingSharedColumns,
                     HistoryTableSharedColumns):
    """Represents the historical state of a booking"""
    __tablename__ = 'booking_history'
    __table_args__ = (
        CheckConstraint(
            'LENGTH(facility_id) = 16',
            name='booking_history_facility_id_length_check'),
    )

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    booking_history_id = Column(Integer, primary_key=True)

    booking_id = Column(
        Integer, ForeignKey('booking.booking_id'), nullable=False, index=True)


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

    @declared_attr
    def booking_id(self):
        return Column(
            Integer, ForeignKey('booking.booking_id'), nullable=False)


class Hold(JailsBase, _HoldSharedColumns):
    """Represents a hold from another jurisdiction against a booking"""
    __tablename__ = 'hold'

    hold_id = Column(Integer, primary_key=True)


class HoldHistory(JailsBase,
                  _HoldSharedColumns,
                  HistoryTableSharedColumns):
    """Represents the historical state of a hold"""
    __tablename__ = 'hold_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    hold_history_id = Column(Integer, primary_key=True)

    hold_id = Column(
        Integer, ForeignKey('hold.hold_id'), nullable=False, index=True)


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

    @declared_attr
    def booking_id(self):
        return Column(
            Integer, ForeignKey('booking.booking_id'), nullable=False)


class Arrest(JailsBase, _ArrestSharedColumns):
    """Represents an arrest in the SQL schema"""
    __tablename__ = 'arrest'

    arrest_id = Column(Integer, primary_key=True)


class ArrestHistory(JailsBase,
                    _ArrestSharedColumns,
                    HistoryTableSharedColumns):
    """Represents the historical state of an arrest"""
    __tablename__ = 'arrest_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    arrest_history_id = Column(Integer, primary_key=True)

    arrest_id = Column(
        Integer, ForeignKey('arrest.arrest_id'), nullable=False, index=True)


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

    # This foreign key is usually redundant, as a bond can be linked to a
    # booking through a charge. This foreign key serves to prevent orphaning
    # a bond if all charges that point to it are updated to point to other
    # bonds. It does not have a corresponding SQLAlchemy relationship, to avoid
    # redundant relationships.
    @declared_attr
    def booking_id(self):
        return Column(
            Integer,
            # Because this key does not correspond to a SQLAlchemy
            # relationship, it needs to be manually set. To avoid raising an
            # error during any transient invalid states during processing, the
            # constraint must be deferred to only be checked at commit time.
            ForeignKey(
                'booking.booking_id', deferrable=True, initially='DEFERRED'),
            nullable=False)


class Bond(JailsBase, _BondSharedColumns):
    """Represents a bond in the SQL schema"""
    __tablename__ = 'bond'

    bond_id = Column(Integer, primary_key=True)


class BondHistory(JailsBase,
                  _BondSharedColumns,
                  HistoryTableSharedColumns):
    """Represents the historical state of a bond"""
    __tablename__ = 'bond_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    bond_history_id = Column(Integer, primary_key=True)

    bond_id = Column(
        Integer, ForeignKey('bond.bond_id'), nullable=False, index=True)


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
    date_imposed = Column(Date)
    completion_date = Column(Date)
    projected_completion_date = Column(Date)
    is_life = Column(Boolean)
    is_probation = Column(Boolean)
    is_suspended = Column(Boolean)
    fine_dollars = Column(Integer)
    parole_possible = Column(Boolean)
    post_release_supervision_length_days = Column(Integer)

    # This foreign key is usually redundant, as a sentence can be linked to a
    # booking through a charge. This foreign key serves to prevent orphaning
    # a sentence if all charges that point to it are updated to point to other
    # sentences. It does not have a corresponding SQLAlchemy relationship, to
    # avoid redundant relationships.
    @declared_attr
    def booking_id(self):
        return Column(
            Integer,
            # Because this key does not correspond to a SQLAlchemy
            # relationship, it needs to be manually set. To avoid raising an
            # error during any transient invalid states during processing, the
            # constraint must be deferred to only be checked at commit time.
            ForeignKey(
                'booking.booking_id', deferrable=True, initially='DEFERRED'),
            nullable=False)


class Sentence(JailsBase, _SentenceSharedColumns):
    """Represents a sentence in the SQL schema"""
    __tablename__ = 'sentence'

    sentence_id = Column(Integer, primary_key=True)

    # Due to the SQLAlchemy requirement that both halves of an association pair
    # be represented by different relationships, a sentence must have two sets
    # of relationships with other sentences, depending on which side of the pair
    # it's on.
    related_sentences_a = association_proxy(
        'a_sentence_relations', 'sentence_a')
    related_sentences_b = association_proxy(
        'b_sentence_relations', 'sentence_b')


class SentenceHistory(JailsBase,
                      _SentenceSharedColumns,
                      HistoryTableSharedColumns):
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
    sentence_relationship_type = Column(
        'type', sentence_relationship_type, nullable=False)
    sentence_relation_type_raw_text = Column(String(255))

    # NOTE: A sentence relationship is undirected: if sentence A is served
    # concurrently with sentence B, sentence B is served concurrently with
    # sentence A. However, due to the limits of a standard SQL table, we have
    # to define the relationship directionally here, by arbitrarily choosing one
    # sentence to label as A and one to label as B. This choice of (A,B) is
    # equal to the other arbitrary choice of (B,A), so there should only be one
    # SentenceRelationship defined for any pair of sentences. (I.e. don't
    # create both (A,B) and (B,A) SentenceRelationships for one pair of
    # sentences.)

    @declared_attr
    def sentence_a_id(self):
        return Column(
            Integer, ForeignKey('sentence.sentence_id'), nullable=False)

    @declared_attr
    def sentence_b_id(self):
        return Column(
            Integer, ForeignKey('sentence.sentence_id'), nullable=False)


class SentenceRelationship(JailsBase,
                           _SentenceRelationshipSharedColumns):
    """Represents the relationship between two sentences"""
    __tablename__ = 'sentence_relationship'

    sentence_relationship_id = Column(Integer, primary_key=True)

    sentence_a = relationship(
        'Sentence',
        backref='b_sentence_relations',
        primaryjoin='Sentence.sentence_id==SentenceRelationship.sentence_b_id')
    sentence_b = relationship(
        'Sentence',
        backref='a_sentence_relations',
        primaryjoin='Sentence.sentence_id==SentenceRelationship.sentence_a_id')


class SentenceRelationshipHistory(JailsBase,
                                  _SentenceRelationshipSharedColumns,
                                  HistoryTableSharedColumns):
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
    court_type = Column(String(255))
    case_number = Column(String(255))
    next_court_date = Column(Date)
    judge_name = Column(String(255))
    charge_notes = Column(Text)

    @declared_attr
    def booking_id(self):
        return Column(
            Integer, ForeignKey('booking.booking_id'), nullable=False)

    @declared_attr
    def bond_id(self):
        return Column(Integer, ForeignKey('bond.bond_id'))

    @declared_attr
    def sentence_id(self):
        return Column(Integer, ForeignKey('sentence.sentence_id'))


class Charge(JailsBase, _ChargeSharedColumns):
    """Represents a charge in the SQL schema"""
    __tablename__ = 'charge'

    charge_id = Column(Integer, primary_key=True)

    bond = relationship('Bond', lazy='joined')
    sentence = relationship('Sentence', lazy='joined')


class ChargeHistory(JailsBase,
                    _ChargeSharedColumns,
                    HistoryTableSharedColumns):
    """Represents the historical state of a charge"""
    __tablename__ = 'charge_history'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    charge_history_id = Column(Integer, primary_key=True)

    charge_id = Column(
        Integer, ForeignKey('charge.charge_id'), nullable=False, index=True)


class ScraperSuccess(JailsBase):
    """Represents the successful completion of a scrape session."""
    __tablename__ = 'scraper_success'

    __table_args__ = (
        CheckConstraint(
            'LENGTH(jid) = 8',
            name='single_count_jid_length_check'),
    )

    scraper_success_id = Column(Integer, primary_key=True)
    jid = Column(String(8), nullable=False)
    date = Column(Date, nullable=False)
