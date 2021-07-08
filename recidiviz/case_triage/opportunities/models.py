# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements data models for opportunities that are not part of a database schema."""
import attr

from recidiviz.case_triage.opportunities.types import Opportunity


@attr.s(auto_attribs=True)
class ComputedOpportunity(Opportunity):
    """An opportunity not backed by an underlying database entity."""

    state_code: str
    supervising_officer_external_id: str
    person_external_id: str
    opportunity_type: str
    opportunity_metadata: dict
