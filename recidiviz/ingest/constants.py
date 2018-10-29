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

"""Constants file used by everyone."""

# These constants tell the generic scraper what functions to perform
INITIAL_TASK = 0x1
SCRAPE_PERSON = 0x2
SCRAPE_RECORD = 0X4
GET_MORE_TASKS = 0x8

# Convenience definitions for scraper task types
INITIAL_TASK_AND_MORE = INITIAL_TASK | GET_MORE_TASKS
SCRAPE_PERSON_AND_MORE = SCRAPE_PERSON | GET_MORE_TASKS
SCRAPE_PERSON_AND_RECORD = SCRAPE_PERSON | SCRAPE_RECORD
SCRAPE_RECORD_AND_MORE = SCRAPE_RECORD | GET_MORE_TASKS
