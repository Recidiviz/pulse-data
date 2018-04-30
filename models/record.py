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
# =============================================================================


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel


class Offense(ndb.Model):
    """Model to describe a particular crime of conviction

    Datastore model for a specific crime that led to an incarceration event. Note
    that incarceration events often result from multiple crimes (e.g. larceny AND
    evading arrest, etc.) so there may be multiple Offense entities in a single
    Record.

    One or more Offense entities are stored in the 'offense' property of a
    Record.

    Attributes:
        crime_description: (string) Scraped from prison site describing the crime
        crime_class: (string) Scraped from prison site describing class of crime
    """
    crime_description = ndb.StringProperty()
    crime_class = ndb.StringProperty()


class SentenceDuration(ndb.Model):
    """Duration of time, used to represent possible sentence durations (min/max)

    Describes a duration of time for a sentence (could be minimum duration,
    could be maximum - this is used for both fields in Record entities).

    A SentenceDuration entity is stored as one of the sentence duration properties
    of a Record entity.

    Attributes:
        life_sentence: (bool) Whether or not this is a life sentence
        years: (int) Number of years in addition to the above for sentence
        months: (int) Number of months in addition to the above for sentence
        days: (int) Number of days in addition to the above for sentence
    """
    life_sentence = ndb.BooleanProperty()
    years = ndb.IntegerProperty()
    months = ndb.IntegerProperty()
    days = ndb.IntegerProperty()


class Record(polymodel.PolyModel):
    """Top-level PolyModel class to describe the record of a criminal event

    Datastore model for a record of a particular criminal event. This is intended
    to be a 1:1 mapping to human beings in the prison system.

    Record entities are never duplicated - if a change is discovered during re-
    scraping (e.g., the parole date has been pushed back), the Record entity is
    updated to reflect the new details and a Snapshot entity is created to note
    the changed fields / new values.

    Because prison systems frequently collect new (and sometimes conflicting)
    information each time an inmate is entered into custody, potentially
    mutable Inmate field names are duplicated here (to ensure we capture all
    variants of e.g. an Inmate's birth date).

    Individual region scrapers are expected to create their own subclasses which
    inherit these common properties, then add more if their region has unique
    information. See us_ny_scraper.py for an example.

    Attributes:
        created_on: (datetime) Creation date of this record. If data is
            migrated in the future, effort will be made to preserve this field
        updated_on: (date) Date of last change / update to this record
        offense: (record.Offense) State-provided strings describing the crimes
            of conviction and (if available) class of crimes.
        record_id: (string) The identifier the state site uses for this crime
        min_sentence: (record.SentenceDuration) Minimum sentence to be served
        max_sentence: (record.SentenceDuration) Maximum sentence to be served
        custody_date: (date) Date the inmate's sentence started
        offense_date: (date) Date the offense was committed
        latest_facility: (string) The name of the most recent facility the inmate has
            been held in
        latest_release_date: (date) Most recent date of release
        latest_release_type: (string) Reason given for most recent release
        is_released: (bool) Whether the inmate has been released from this
            sentence
        given_names: (string) Any given names provided by the source
        last_name: (string) The inmate's surname, as provided by the source
        birthday: (date) Date of birth for the inmate as provided by the source
        sex: (string) Sex of the prisoner as provided by the prison system
        race: (string) Race of the prisoner as provided by prison system
    """
    offense = ndb.StructuredProperty(Offense, repeated=True)
    record_id = ndb.StringProperty()
    min_sentence_length = ndb.StructuredProperty(SentenceDuration, repeated=False)
    max_sentence_length = ndb.StructuredProperty(SentenceDuration, repeated=False)
    custody_date = ndb.DateProperty()
    offense_date = ndb.DateProperty()
    latest_facility = ndb.StringProperty()
    latest_release_date = ndb.DateProperty()
    latest_release_type = ndb.StringProperty()
    is_released = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    last_name = ndb.StringProperty()
    birthday = ndb.DateProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateProperty(auto_now=True)
