# Recidiviz a platform for tracking granular recidivism metrics in real time
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

"""Incarcerated individuals in the criminal justice system."""


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel


class Inmate(polymodel.PolyModel):
    """Top-level PolyModel class to describe an incarcerated individual

    Datastore model for a snapshot of a particular inmate listing. This is
    intended to be a 1:1 mapping to human beings in the prison system. We don't
    want multiple copies of the same inmate, so we update them when re-scraped.

    Individual region scrapers are expected to create their own subclasses which
    inherit these common properties, then add more if their region has more
    available. See us_ny_scraper.py for an example.

    If no corrections-provided ID can be found for the inmate, we generate a
    random 10-digit alphanumeric ID and set record_id_is_fuzzy to True.

    For any inmate listing, there will be 1+ Records and 1+ Snapshots.

    Attributes:
        inmate_id: (string) The identifier the state site uses for this person
        inmate_id_is_fuzzy: Whether we generated this ID/it's not consistent
            across multiple scrapes of the same inmate.
        given_names: (string) First and middle names (space separated),
            if available
        last_name: (string) Last name, if provided
        suffix: (string) Suffix portion of the person's name.
        alias: (string) Alias(es) known to be used by this person.
        birthday: (date) Birth date, if available
        age: (int) Age, if birth date is not available.
        region: (string) The region code for the scraper that captured this
        sex: (string) Sex of inmate in listing, as provided by prison system
        race: (sring) Race of inmate in the listing, for now string provided
            by region
        created_on: (datetime) Python datetime object of first time we added
            this record
        updated_on: (date) Python datetime object of last time we updated
            this record
    """
    # TODO: Delete this class after migration
    inmate_id = ndb.StringProperty()
    inmate_id_is_fuzzy = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    last_name = ndb.StringProperty()
    birthday = ndb.DateProperty()
    age = ndb.IntegerProperty()
    region = ndb.StringProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateProperty(auto_now=True)


class Person(polymodel.PolyModel):
    """Top-level PolyModel class to describe an incarcerated individual

    Datastore model for a snapshot of a particular person listing. This is
    intended to be a 1:1 mapping to human beings in the jail or prison system.
    We don't want multiple copies of the same person, so we update them when
    re-scraped.

    Individual region scrapers are expected to create their own subclasses which
    inherit these common properties, then add more if their region has more
    region-specific fields available. See us_ny_scraper.py for an example.

    If no corrections-provided ID can be found for the person, we generate a
    random 10-digit alphanumeric ID and set record_id_is_fuzzy to True.

    For any person listing, there will be 1+ Records and 1+ Snapshots.

    Attributes:
        person_id: (string) The identifier the state site uses for this person
        person_id_is_fuzzy: Whether we generated this ID/it's not consistent
            across multiple scrapes of the same person.
        given_names: (string) First and middle names (space separated),
            if available
        surname: (string) Last name, if provided
        birthdate: (date) Birth date, if available
        age: (int) Age, if birth date is not available.
        region: (string) The region code for the scraper that captured this
        sex: (string) Sex of person in listing, as provided by prison system
        race: (sring) Race of person in the listing, for now string provided
            by region
        created_on: (datetime) Python datetime object of first time we added
            this record
        updated_on: (date) Python datetime object of last time we updated
            this record
    """
    person_id = ndb.StringProperty()
    person_id_is_fuzzy = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    surname = ndb.StringProperty()
    suffix = ndb.StringProperty()
    alias = ndb.StringProperty()
    birthdate = ndb.DateProperty()
    age = ndb.IntegerProperty()
    region = ndb.StringProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateProperty(auto_now=True)
