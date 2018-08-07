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

"""The core models of the Recidiviz data platform.

These are the models that must be shared between ingest and calculation. They
capture, conceptually:
    An Inmate who serves a particular sentence in the criminal justice system.
    A historical Record of that sentence, including facility, dates, offense,
        sentencing information, and more.
    An Inmate Facility Snapshot that records where an Inmate was incarcerated
        at a particular point in time.
"""


from . import inmate
from . import record
from . import snapshot
