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
"""
class InmateFacilitySnapshot(polymodel.PolyModel):
    snapshot_date = ndb.DateTimeProperty(auto_now_add=True)
    facility = ndb.StringProperty()