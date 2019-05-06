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

"""Module for handling the historical aspect of our database.

We use application-time temporal tables. Application-time temporal tables
reflect the state the real-world system was in at any historical point. Every
individual-level table, for both county and state schemas, features a *History
temporal table that is "valid from" a particular time and "valid to" a
particular time. In an application-time table, these "snapshots" have valid
from and valid to timestamps correlated to actual events in the real world,
e.g. the date they were first admitted to the justice system in the real world.
"""
