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


"""
Inmate

Datastore model for a snapshot of a particular inmate listing. This is intended
to be a 1:1 mapping to human beings in the prison system. We don't want 
multiple copies of the same inmate, so we update them when re-scraped.

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.  

Fields:
    - inmate_id: The identifier the state site uses for this listing
    - inmate_id_is_fuzzy: Whether we generated this ID/it's not x-scrape stable
    - given_names: First and middle names (space separated), if provided
    - last_name: Last name, if provided
    - birthday: Birth date, if available
    - age: Age, if birth date is not available.
    - region: The region code for the scraper that captured this
    - sex: Sex of inmate in listing, as provided by prison system
    - race:  Race of inmate in the listing, for now string provided by region
    - created_on: Python datetime object of first time we added this record
    - updated_on: Python datetime object of last time we updated this record

Analytics tooling may use inmate_id + region to dedup if available, so it's
important to update the listing based on that field, rather than just write a 
new one. If none can be found, generate a random 10-digit 
alphanumeric ID and set record_id_is_fuzzy to True. This will help in 
analytics logic.

For any inmate listing, there will be 1+ Records and 1+ InmateFacilitySnapshots.
"""
class Inmate(polymodel.PolyModel):
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