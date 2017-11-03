# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.


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
    - sex: Sex of inmate in listing, if provided ("Male" or "Female")
        >> This is due to classification in the systems scraped - if they 
        shift to a less binary approach, we can improve this <<
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