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

NOTE: Many of the tables in the below schema are historical tables. Historical
tables have no primary key. Their equivalent of the primary key from the
corresponding master table is non-unique. A composite primary key could
technically be generated from the ID and the period columns, but there would be
no benefit from it, as that would require any foreign key relationship to
directly use the period columns, which is contrary to the purpose of the
temporal table design. Because of this non-uniqueness, any foreign key
pointing to a historical table does NOT have a foreign key constraint.
"""

from sqlalchemy import Boolean, Column, Date, DateTime, Enum, ForeignKey, \
    Integer, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


# Enum values. Exposed separately from the SQLAlchemy Enum that owns them for
# easier access to the values.
#
# These values should not be used directly by application code, but should be
# mapped to application layer enums in order to maintain decoupling between
# the application and database.
#
# TODO(176): replace with Python enums after converting to Python 3

# Person
gender = ('Female',
          'Male')
race = ('American Indian/Alaskan Native',
        'Asian',
        'Black',
        'Native Hawaiian/Pacific Islander',
        'Other',
        'White')
ethnicity = ('Hispanic',
             'Not Hispanic')

# Booking
release_reason = ('Bond',
                  'Death',
                  'Escape',
                  'Expiration of Sentence',
                  'Own Recognizance',
                  'Parole',
                  'Probation',
                  'Transfer')
custody_status = ('Escaped',
                  'Held Elsewhere',
                  'In Custody',
                  'Released')
classification = ('High',
                  'Low',
                  'Maximum',
                  'Medium',
                  'Minimum',
                  'Work Release')

# Hold
hold_status = ('Active',
               'Inactive')

# Bond
bond_type = ('Bond Denied',
             'Cash',
             'No Bond',
             'Secured',
             'Unsecured')
bond_status = ('Active',
               'Posted')

# SentenceRelationship
sentence_relationship_type = ('Concurrent',
                              'Consecutive')

# Charge
degree = ('First',
          'Second',
          'Third')
charge_class = ('Felony',
                'Misdemenor',
                'Parole Violation',
                'Probation Violation')
charge_status = ('Acquitted',
                 'Completed Sentence',
                 'Convicted',
                 'Dropped',
                 'Pending',
                 'Pretrial',
                 'Sentenced')
court_type = ('Circuit',
              'District',
              'Other',
              'Superior')


# SQLAlchemy enums. Created separately from the tables so they can be shared
# between the master and historical tables for each entity.
bond_status_enum = Enum(*bond_status, name='bond_status')
bond_type_enum = Enum(*bond_type, name='bond_type')
charge_class_enum = Enum(*charge_class, name='charge_class')
charge_status_enum = Enum(*charge_status, name='charge_status')
classification_enum = Enum(*classification, name='classification')
court_type_enum = Enum(*court_type, name='court_type')
custody_status_enum = Enum(*custody_status, name='custody_status')
degree_enum = Enum(*degree, name='degree')
ethnicity_enum = Enum(*ethnicity, name='ethnicity')
gender_enum = Enum(*gender, name='gender')
hold_status_enum = Enum(*hold_status, name='hold_status')
race_enum = Enum(*race, name='race')
release_reason_enum = Enum(*release_reason, name='release_reason')
sentence_relationship_type_enum = Enum(
    *sentence_relationship_type, name='sentence_relationship_type')


# Base class for all table classes
Base = declarative_base()


class Person(Base):
    """Represents a person in the SQL schema"""
    __tablename__ = 'Person'

    person_id = Column(Integer, primary_key=True)
    external_id = Column(String(255), index=True)
    surname = Column(String(255), index=True)
    given_names = Column(String(255), index=True)
    birthdate = Column(Date, index=True)
    birthdate_inferred_from_age = Column(Boolean)
    gender = Column(gender_enum)
    race = Column(race_enum)
    ethnicity = Column(ethnicity_enum)
    place_of_residence = Column(String(255))

    bookings = relationship('Booking', back_populates='person')


class PersonHistory(Base):
    """Represents the historical state of a person"""
    __tablename__ = 'PersonHistory'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_history_id = Column(Integer, primary_key=True)

    person_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    external_id = Column(String(255))
    surname = Column(String(255))
    given_names = Column(String(255))
    birthdate = Column(Date)
    birthdate_inferred_from_age = Column(Boolean)
    gender = Column(gender_enum)
    race = Column(race_enum)
    ethnicity = Column(ethnicity_enum)
    place_of_residence = Column(String(255))


class Booking(Base):
    """Represents a booking in the SQL schema"""
    __tablename__ = 'Booking'

    booking_id = Column(Integer, primary_key=True)

    person_id = Column(Integer, ForeignKey('Person.person_id'), nullable=False)
    external_id = Column(String(255), index=True)
    admission_date = Column(Date)
    release_date = Column(Date)
    release_date_inferred = Column(Boolean)
    projected_release_date = Column(Date)
    release_reason = Column(release_reason_enum)
    custody_status = Column(custody_status_enum, nullable=False)
    held_for_other_jurisdiction = Column(Boolean)
    facility = Column(String(255))
    classification = Column(classification_enum)
    region = Column(String(255), nullable=False, index=True)
    last_scraped_time = Column(DateTime, nullable=False)

    person = relationship('Person', back_populates='bookings')
    holds = relationship('Hold', back_populates='booking')
    arrest = relationship('Arrest', uselist=False, back_populates='booking')
    charges = relationship('Charge', back_populates='booking')


class BookingHistory(Base):
    """Represents the historical state of a booking"""
    __tablename__ = 'BookingHistory'

    # NOTE: BookingHistory does not contain last_scraped_time column. This is to
    # avoid needing to create a new BookingHistory entity when a booking is
    # re-scraped and no values have changed except last_scraped_date.

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    booking_history_id = Column(Integer, primary_key=True)

    booking_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    external_id = Column(String(255), index=True)
    person_id = Column(Integer, nullable=False, index=True)
    admission_date = Column(Date)
    release_date = Column(Date)
    release_date_inferred = Column(Boolean)
    projected_release_date = Column(Date)
    release_reason = Column(release_reason_enum)
    custody_status = Column(custody_status_enum, nullable=False)
    held_for_other_jurisdiction = Column(Boolean)
    facility = Column(String(255))
    classification = Column(classification_enum)
    region = Column(String(255), nullable=False)


class Hold(Base):
    """Represents a hold from another jurisdiction against a booking"""
    __tablename__ = 'Hold'

    hold_id = Column(Integer, primary_key=True)
    booking_id = Column(
        Integer, ForeignKey('Booking.booking_id'), nullable=False)
    jurisdiction_name = Column(String(255))
    hold_status = Column(hold_status_enum, nullable=False)

    booking = relationship('Booking', back_populates='holds')


class HoldHistory(Base):
    """Represents the historical state of a hold"""
    __tablename__ = 'HoldHistory'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    hold_history_id = Column(Integer, primary_key=True)

    hold_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    booking_id = Column(Integer, nullable=False, index=True)
    jurisdiction_name = Column(String(255))
    hold_status = Column(hold_status_enum, nullable=False)


class Arrest(Base):
    """Represents an arrest in the SQL schema"""
    __tablename__ = 'Arrest'

    arrest_id = Column(Integer, primary_key=True)
    external_id = Column(String(255), index=True)
    booking_id = Column(
        Integer, ForeignKey('Booking.booking_id'), nullable=False)
    date = Column(Date)
    location = Column(String(255))
    agency = Column(String(255))
    officer_name = Column(String(255))
    officer_id = Column(String(255))

    booking = relationship('Booking', back_populates='arrest')


class ArrestHistory(Base):
    """Represents the historical state of an arrest"""
    __tablename__ = 'ArrestHistory'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    arrest_history_id = Column(Integer, primary_key=True)

    arrest_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    external_id = Column(String(255), index=True)
    booking_id = Column(Integer, nullable=False, index=True)
    date = Column(Date)
    location = Column(String(255))
    agency = Column(String(255))
    officer_name = Column(String(255))
    officer_id = Column(String(255))


class Bond(Base):
    """Represents a bond in the SQL schema"""
    __tablename__ = 'Bond'

    bond_id = Column(Integer, primary_key=True)
    external_id = Column(String(255), index=True)
    amount_dollars = Column(Integer)
    bond_type = Column(bond_type_enum)
    status = Column(bond_status_enum, nullable=False)

    charges = relationship('Charge', back_populates='bond')


class BondHistory(Base):
    """Represents the historical state of a bond"""
    __tablename__ = 'BondHistory'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    bond_history_id = Column(Integer, primary_key=True)

    bond_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    external_id = Column(String(255), index=True)
    amount_dollars = Column(Integer)
    bond_type = Column(bond_type_enum)
    status = Column(bond_status_enum, nullable=False)


class Sentence(Base):
    """Represents a sentence in the SQL schema"""
    __tablename__ = 'Sentence'

    sentence_id = Column(Integer, primary_key=True)
    external_id = Column(String(255), index=True)
    date_imposed = Column(Date)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)
    is_life = Column(Boolean)
    is_probation = Column(Boolean)
    is_suspended = Column(Boolean)
    fine_dollars = Column(Integer)
    parole_possible = Column(Boolean)
    post_release_supervision_length_days = Column(Integer)

    charges = relationship('Charge', back_populates='sentence')

    # Due to the SQLAlchemy requirement that both halves of an association pair
    # be represented by different relationships, a sentence must have two sets
    # of relationships with other sentences, depending on which side of the pair
    # it's on.
    related_sentences_a = association_proxy(
        'a_sentence_relations', 'sentence_a')
    related_sentences_b = association_proxy(
        'b_sentence_relations', 'sentence_b')


class SentenceHistory(Base):
    """Represents the historical state of a sentence"""
    __tablename__ = 'SentenceHistory'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    sentence_history_id = Column(Integer, primary_key=True)

    sentence_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    external_id = Column(String(255), index=True)
    date_imposed = Column(Date)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)
    is_life = Column(Boolean)
    is_probation = Column(Boolean)
    is_suspended = Column(Boolean)
    fine_dollars = Column(Integer)
    parole_possible = Column(Boolean)
    post_release_supervision_length_days = Column(Integer)


class SentenceRelationship(Base):
    """Represents the relationship between two sentences"""
    __tablename__ = 'SentenceRelationship'

    # NOTE: (A,B) is equal to (B,A). There should only be one
    # SentenceRelationship for any pair of sentences.

    sentence_relationship_id = Column(Integer, primary_key=True)
    sentence_a_id = Column(
        Integer, ForeignKey('Sentence.sentence_id'), nullable=False)
    sentence_b_id = Column(
        Integer, ForeignKey('Sentence.sentence_id'), nullable=False)
    # Manually set name to avoid conflict with Python reserved keyword
    sentence_relationship_type = Column('type', sentence_relationship_type_enum)

    sentence_a = relationship(
        'Sentence',
        backref='b_sentence_relations',
        primaryjoin='Sentence.sentence_id==SentenceRelationship.sentence_b_id')
    sentence_b = relationship(
        'Sentence',
        backref='a_sentence_relations',
        primaryjoin='Sentence.sentence_id==SentenceRelationship.sentence_a_id')


class SentenceRelationshipHistory(Base):
    """Represents the historical state of the relationship between two sentences
    """
    __tablename__ = 'SentenceRelationshipHistory'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    sentence_relationship_history_id = Column(Integer, primary_key=True)

    sentence_relationship_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    sentence_a_id = Column(Integer, nullable=False, index=True)
    sentence_b_id = Column(Integer, nullable=False, index=True)
    # Manually set name to avoid conflict with Python reserved keyword
    sentence_relationship_type = Column('type', sentence_relationship_type_enum)


class Charge(Base):
    """Represents a charge in the SQL schema"""
    __tablename__ = 'Charge'

    charge_id = Column(Integer, primary_key=True)
    external_id = Column(String(255), index=True)
    booking_id = Column(
        Integer, ForeignKey('Booking.booking_id'), nullable=False)
    bond_id = Column(Integer, ForeignKey('Bond.bond_id'))
    sentence_id = Column(
        Integer, ForeignKey('Sentence.sentence_id'))
    offense_date = Column(Date)
    statute = Column(String(255))
    offense_code = Column(Integer)
    name = Column(String(255))
    attempted = Column(Boolean)
    degree = Column(degree_enum)
    # Manually set name to avoid conflict with Python reserved keyword
    charge_class = Column('class', charge_class_enum)
    level = Column(String(255))
    fee_dollars = Column(Integer)
    charging_entity = Column(String(255))
    status = Column(charge_status_enum, nullable=False)
    number_of_counts = Column(Integer)
    court_type = Column(court_type_enum)
    case_number = Column(String(255))
    next_court_date = Column(Date)
    judge_name = Column(String(255))

    booking = relationship('Booking', back_populates='charges')
    bond = relationship('Bond', back_populates='charges')
    sentence = relationship('Sentence', back_populates='charges')


class ChargeHistory(Base):
    """Represents the historical state of a charge"""
    __tablename__ = 'ChargeHistory'

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    charge_history_id = Column(Integer, primary_key=True)

    charge_id = Column(Integer, nullable=False, index=True)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    external_id = Column(String(255), index=True)
    booking_id = Column(Integer, nullable=False, index=True)
    bond_id = Column(Integer, index=True)
    sentence_id = Column(Integer, index=True)
    offense_date = Column(Date)
    statute = Column(String(255))
    offense_code = Column(Integer)
    name = Column(String(255))
    attempted = Column(Boolean)
    degree = Column(degree_enum)
    # Manually set name to avoid conflict with Python reserved keyword
    charge_class = Column('class', charge_class_enum)
    level = Column(String(255))
    fee_dollars = Column(Integer)
    charging_entity = Column(String(255))
    status = Column(charge_status_enum, nullable=False)
    number_of_counts = Column(Integer)
    court_type = Column(court_type_enum)
    case_number = Column(String(255))
    next_court_date = Column(Date)
    judge_name = Column(String(255))
