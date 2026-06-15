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
"""Intercom-related types"""

from typing import Any, Dict

import attr
import cattrs


@attr.frozen
class IntercomTicket:
    """Represents an Intercom ticket request schema."""

    # The ID of the type of ticket you want to create
    ticket_type_id: int
    # The email you have defined for the contact who is being added as a participant. If a contact with this email does not exist, one will be created.
    email: str
    # Title of the ticket
    default_title: str
    # Description for the ticket
    default_description: str

    def to_dict(self) -> Dict[str, Any]:
        """Converts an object into the intercom format"""
        return {
            "ticket_type_id": self.ticket_type_id,
            "contacts": [{"email": self.email}],
            "ticket_attributes": {
                "_default_title_": self.default_title,
                "_default_description_": self.default_description,
            },
        }

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.frozen
class IntercomTicketResponse:
    """Represents an Intercom ticket response schema."""

    # The id for the created ticket
    id: str

    def to_json(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)
