# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel
from inmate import Inmate
from record import Record


"""
InmateFacilitySnapshot

Datastore model for the facility an inmate was located in at the time of a 
particular scraping. An inmate should have at least one entry for this from
time of creation, but it will only be updated when the value changes. In
very rare cases, the scraper might not find a value, in which case the
'facility' will be empty (but the snapshot still collected.)

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.

Fields:
    - snapshot_date: Python datetime for time of snapshot
    - facility: State-provided facility name
    - associated_inmate: Inmate this data is about
    - associated_record: Record that this is from
"""
class InmateFacilitySnapshot(polymodel.PolyModel):
    snapshot_date = ndb.DateTimeProperty(auto_now_add=True)
    facility = ndb.StringProperty()