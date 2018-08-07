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

"""Snapshots of changes to Record data.

Whereas Records hold the latest truth about a particular sentencing in the
criminal justice system, those Records can be updated for many reasons
throughout its lifetime. Each time we scrape a particular Record and find that
it has changed in some way, we persist that change in a Snapshot. The Snapshot
will only have those fields set which have changed since the last scrape.

The first scrape that finds a new Record yields a Snapshot with all fields set.
"""


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel
from recidiviz.models.record import SentenceDuration
from recidiviz.models.record import Offense


class Snapshot(polymodel.PolyModel):
    """Model to describe updates to Inmate or Record data

    Datastore model for mutable details about an inmate or criminal record
    at the time of a particular scraping. An inmate should have at least one
    entry for this from time of creation, as well as an additional entity for
    each subsequent scrape in which a change was found. Only updated fields
    are stored (includes all fields for the first snapshot of a Record entity).

    Individual region scrapers are expected to create their own subclasses which
    inherit these common properties, then add more if their region has more
    available. See us_ny_scraper.py for an example.

    Attributes:
        created_on: (datetime) Timestamp for creation time of snapshot
        latest_facility: (string) The name of the facility the inmate was
            held in
        latest_release_date: (date) Most recent date of release
        latest_release_type: (string) Reason given for most recent release
        is_released: (bool) Whether the inmate has been released from the
        min_sentence: (record.SentenceDuration) Minimum sentence to be served
        max_sentence: (record.SentenceDuration) Maximum sentence to be served
            sentence
        offense: (record.Offense) State-provided strings describing the crimes
            of conviction and (if available) class of crimes.
        offense_date: (date) Date the offense was committed
        custody_date: (date) Date the inmate's sentence started
        birthday: (date) Date of birth for the inmate as provided by the source
        sex: (string) Sex of the prisoner as provided by the prison system
        race: (string) Race of the prisoner as provided by prison system
        last_name: (string) The inmate's surname, as provided by the source
        given_names: (string) Any given names provided by the source

    """
    latest_facility = ndb.StringProperty()
    latest_release_date = ndb.DateProperty()
    latest_release_type = ndb.StringProperty()
    is_released = ndb.BooleanProperty()
    min_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    max_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    offense = ndb.StructuredProperty(Offense, repeated=True)
    offense_date = ndb.DateProperty()
    custody_date = ndb.DateProperty()
    birthday = ndb.DateProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
    last_name = ndb.StringProperty()
    given_names = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)


class InmateFacilitySnapshot(polymodel.PolyModel):
    """
    InmateFacilitySnapshot

    Old model, keeping for short-term migration purposes only.
    TODO(andrew): Remove post-migration to Snapshot

    Attributes:
        snapshot_date: (datetime) Timestamp for creation time of snapshot
        facility: (string) The name of the facility the inmate was held in
    """
    snapshot_date = ndb.DateTimeProperty(auto_now_add=True)
    facility = ndb.StringProperty()
