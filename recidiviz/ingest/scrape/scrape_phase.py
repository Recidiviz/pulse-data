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
# ============================================================================
"""Defines the order of processes to run once a scrape has finished"""

from typing import Optional

_next_phase = {
    'scraper_status.check_for_finished_scrapers':
        'scraper_control.scraper_stop',
    'scraper_control.scraper_stop': 'batch.read_and_persist',
    'batch.read_and_persist': 'infer_release.infer_release',
    'infer_release.infer_release': None,
}

def next_phase(current_phase: Optional[str]) -> Optional[str]:
    if current_phase is None:
        raise ValueError('Unable to identify current scrape phase.')
    return _next_phase[current_phase]
