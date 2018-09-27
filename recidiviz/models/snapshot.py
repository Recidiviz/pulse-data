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
    """Model to describe updates to Person or Record data

    Datastore model for mutable details about a person or criminal record
    at the time of a particular scraping. A person should have at least one
    entry for this from time of creation, as well as an additional entity for
    each subsequent scrape in which a change was found. Only updated fields
    are stored (includes all fields for the first snapshot of a Record entity).

    Individual region scrapers are expected to create their own subclasses which
    inherit these common properties, then add more if their region has more
    available. See us_ny_scraper.py for an example.

    Attributes:
        created_on: (datetime) Timestamp for creation time of snapshot
        latest_facility: (string) The name of the facility the person was
            held in
        latest_release_date: (date) Most recent date of release
        latest_release_type: (string) Reason given for most recent release
        is_released: (bool) Whether the person has been released from the
        min_sentence: (record.SentenceDuration) Minimum sentence to be served
        max_sentence: (record.SentenceDuration) Maximum sentence to be served
            sentence
        offense: (record.Offense) State-provided strings describing the crimes
            of conviction and (if available) class of crimes.
        offense_date: (date) Date the offense was committed
        custody_date: (date) Date the person's sentence started
        birthdate: (date) Date of birth for the person as provided by the source
        sex: (string) Sex of the prisoner as provided by the prison system
        race: (string) Race of the prisoner as provided by prison system
        surname: (string) The person's surname, as provided by the source
        given_names: (string) Any given names provided by the source
        last_custody_date: (date) Most recent date person returned for this
            sentence (may not be the initial custody date - e.g., if parole
            was violated, may be readmitted for remainder of prison term)
        admission_type: (string) 'New commitment' is beginning to serve a term,
            other reasons are usually after term has started (e.g. parole issue)
        county_of_commit: (string) County the person was convicted/committed in
        custody_status: (string) Scraped string on custody status (more granular
            than just 'released' / 'not-released')
        earliest_release_date: (date) Earliest date to be released based on
            min_sentence. In certain circumstances, may be released before this.
        earliest_release_type: (string) The reason for the earliest possible
            release date.
        parole_hearing_date: (date) Date of next hearing before Parole Board
        parole_hearing_type: (string) Type of hearing for next PB appearance.
        parole_elig_date: (date) Date person will be eligible for parole
        cond_release_date: (date) Release date based on prison discretion for
            'good time off' based on behavior. Releases prisoner on parole, but
            bypasses PB review.
        max_expir_date: (date) Date of release if no PB or conditional release,
            maximum obligation to the state.
        max_expir_date_parole: (date) Last possible date of ongoing parole
            supervision. Doesn't apply to all people.
        max_expir_date_superv: (date) Last possible date of post-release
            supervision. Doesn't apply to all people.
        parole_discharge_date: (date) Final date of parole supervision, based on
            the parole board's decision to end supervision before max
            expiration.
        region: (string) The Recidiviz region code that this Record belongs to

    """
    admission_type = ndb.StringProperty()
    birthdate = ndb.DateProperty()
    cond_release_date = ndb.DateProperty()
    county_of_commit = ndb.StringProperty()
    custody_date = ndb.DateProperty()
    custody_status = ndb.StringProperty()
    earliest_release_date = ndb.DateProperty()
    earliest_release_type = ndb.StringProperty()
    is_released = ndb.BooleanProperty()
    last_custody_date = ndb.DateProperty()
    latest_facility = ndb.StringProperty()
    latest_release_date = ndb.DateProperty()
    latest_release_type = ndb.StringProperty()
    max_expir_date = ndb.DateProperty()
    max_expir_date_parole = ndb.DateProperty()
    max_expir_date_superv = ndb.DateProperty()
    max_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    min_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    offense = ndb.StructuredProperty(Offense, repeated=True)
    offense_date = ndb.DateProperty()
    parole_discharge_date = ndb.DateProperty()
    parole_elig_date = ndb.DateProperty()
    parole_hearing_date = ndb.DateProperty()
    parole_hearing_type = ndb.StringProperty()
    race = ndb.StringProperty()
    region = ndb.StringProperty()
    sex = ndb.StringProperty()
    surname = ndb.StringProperty()
    given_names = ndb.StringProperty()

    created_on = ndb.DateTimeProperty(auto_now_add=True)
