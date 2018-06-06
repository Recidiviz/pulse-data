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

"""Session tracking for individual scraper executions."""


from google.appengine.ext import ndb


class ScrapeSession(ndb.Model):
    """Model to describe a scraping session's current state

    Datastore model for information about a scraping session. Used by the
    scraper to store state during scraping, and handle resumption of work from a
    prior session if continuing work after a pause.

    Attributes:
        start: (datetime) Date/time this session started
        end: (datetime) Date/time when this session finished
        docket_item: (string) The task name of the docket item currently leased
            by this session. Used to delete docket items after completion.
        last_scraped: String in the form "SURNAME, FIRST" of the last
            scraped name in this session
        region: Region code, e.g. us_ny
        scrape_type: (string) 'background' or 'snapshot'
    """
    start = ndb.DateTimeProperty(auto_now_add=True)
    end = ndb.DateTimeProperty()
    docket_item = ndb.StringProperty()
    last_scraped = ndb.StringProperty()
    region = ndb.StringProperty()
    scrape_type = ndb.StringProperty(choices=("background", "snapshot"))


class ScrapedRecord(ndb.Model):
    """Model to describe a record that has been scraped, to prevent re-scraping

    Datastore model for a scraped record entry. We use this to track which
    records we've already scraped in the session to avoid accidental duplicate
    scraping during the same session.

    Fields:
        created_on: (datetime) Date/time when this entry was created
        record_id: (string) Dept. ID Number, the ID for a record we've scraped
        region: (string) Region code, e.g. us_ny
    """
    record_id = ndb.StringProperty()
    region = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)
