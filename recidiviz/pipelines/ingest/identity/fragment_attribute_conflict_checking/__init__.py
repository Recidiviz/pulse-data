# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Conflict checking for clusters built by the identity ingest pipeline.

Glossary, used consistently across this package and its callers:

- Attribute values are said to be "divergent" if they record different values.
- Attribute values are said to be "conflicting" if they are divergent in
  such a way that they likely do not describe one person.
- A cluster with fragments that have conflicting attributes will be "rejected".
  Otherwise, it will be "kept".
- "Resolution" is how the identity pipeline decides what value an attribute in
  a cluster should have if that attribute's values are divergent across the
  cluster's fragments.
"""
