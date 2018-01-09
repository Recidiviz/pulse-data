# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel
from inmate import Inmate


"""
Offense

Datastore model for a specific crime that led to an incarceration event. Note
that many incarceration events result from multiple crimes (e.g. larceny AND
evading arrest, etc.) so this field repeats in a Record.

Fields:
    - crime_description: String scraped from prison site describing the crime
    - crime_class: String scraped from prison site describing class of crime
"""
class Offense(ndb.Model):
    crime_description = ndb.StringProperty()
    crime_class = ndb.StringProperty()


"""
SentenceDuration

Entry describing a duration of time for a sentence (could be minimum duration,
could be maximum - this is used for both for a particular Record).

Fields:
    - life_sentence: Whether or not this is a life sentence
    - years: Number of years in addition to the above for sentence
    - months: Number of months in addition to the above for sentence
    - days: Number of days in addition to the above for sentence
"""
class SentenceDuration(ndb.Model):
    life_sentence = ndb.BooleanProperty()
    years = ndb.IntegerProperty()
    months = ndb.IntegerProperty()
    days = ndb.IntegerProperty()


"""
Record

Datastore model for historical violation records (e.g. 
ROBB. WPN-NOT DEADLY, 04/21/1995, sentence: 20mo). Multiple records may map
to one inmate.

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.

Fields:
    - offense: JSON'ified dict. {'crime_string': 'class', ...} Contains
        state-provided string describing the crimes and (if available) 
        class of crimes. 
    - record_id: The identifier the state site uses for this crime
    - min_sentence: (json encoded dict) Minimum sentence to be served {Life: 
        bool, years: int, months: int, days: int} - Life is always False.
    - max_sentence: (json encoded dict) Maximum sentence to be served {Life: 
        bool, years: int, months: int, days: int} - Life is whether it's a life
        sentence.
    - custody_date: Date the inmate's sentence started, if available
    - offense_date: Date the offense was committed, if available
    - is_released: Whether the inmate was released (at least, for this crime)
    - given_names: Any first and middle names provided by the prison system
    - last_name: The inmate's surname, as provided by the prison system
    - birthday: Date of birth for the prisoner, as provided by prison system
    - sex: Sex of the prisoner as provided by the prison system
    - race: Race of the prisoner as provided by prison system.

Analytics tooling will use record_id for dedup, so it's important to update 
records based on that key, rather than attempting to store them each time. 
"""
class Record(polymodel.PolyModel):
    offense = ndb.StructuredProperty(Offense, repeated=True)
    record_id = ndb.StringProperty()
    min_sentence_length = ndb.StructuredProperty(SentenceDuration, repeated=False)
    max_sentence_length = ndb.StructuredProperty(SentenceDuration, repeated=False)
    custody_date = ndb.DateProperty()
    offense_date = ndb.DateProperty()
    is_released = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    last_name = ndb.StringProperty()
    birthday = ndb.DateProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
