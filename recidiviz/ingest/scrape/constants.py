# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Constants file used by everyone."""
import enum


# These constants tell the generic scraper what functions to perform
class TaskType(enum.Flag):
    INITIAL = enum.auto()
    SCRAPE_DATA = enum.auto()
    GET_MORE_TASKS = enum.auto()

    # Convenience definitions for scraper task types
    INITIAL_AND_MORE = INITIAL | GET_MORE_TASKS
    INITIAL_AND_SCRAPE_AND_MORE = INITIAL | GET_MORE_TASKS | SCRAPE_DATA
    SCRAPE_DATA_AND_MORE = SCRAPE_DATA | GET_MORE_TASKS


@enum.unique
class ScrapeType(enum.Enum):
    BACKGROUND = "background"
    SNAPSHOT = "snapshot"


MAX_PEOPLE_TO_LOG = 4
BATCH_PUBSUB_TYPE = "scraper_batch"


@enum.unique
class ResponseType(enum.Enum):
    HTML = "html"
    JSON = "json"
    JSONP = "jsonp"
    TEXT = "text"
    RAW = "raw"
