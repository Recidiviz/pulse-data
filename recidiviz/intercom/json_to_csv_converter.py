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
"""Translates JSON from inbound Intercom data into fields that match the corresponding BQ tables."""

import os
from datetime import datetime, timezone
from typing import Any, Callable

import attr
import pandas as pd

from recidiviz.common.constants.states import StateCode
from recidiviz.intercom.types import (
    IntercomContact,
    IntercomSearchTicket,
    IntercomTicketCategory,
    IntercomTicketState,
)

TICKETS = "tickets"
CONTACTS = "contacts"
# ticket JSON fields
TICKET_ID = "ticket_id"
TICKET_STATE = "ticket_state"
TICKET_ATTRIBUTES = "ticket_attributes"
CREATED_AT = "created_at"
UPDATED_AT = "updated_at"
DEFAULT_TITLE = "_default_title_"
DEFAULT_DESCRIPTION = "_default_description_"
CATEGORY = "category"
# contact JSON fields
NAME = "name"
EMAIL = "email"
ID = "id"
EXTERNAL_ID = "external_id"
CUSTOM_ATTRIBUTES = "custom_attributes"
STATE_CODE = "state_code"
ROLE = "role"


@attr.define(frozen=True, kw_only=True)
class IntercomJsonToCsvConverter:
    """Translates JSON from Intercom into fields that match the corresponding BQ tables."""

    @staticmethod
    def flatten_ticket(ticket_json: dict[str, Any]) -> IntercomSearchTicket:
        """Flattens Intercom ticket JSON into flat fields to match the defined BQ table schema"""

        created_at_timestamp = ticket_json[CREATED_AT]
        updated_at_timestamp = ticket_json[UPDATED_AT]

        # As of 8/14/26, all tickets for the past two years have all of these fields,
        # so we assume that each field is present and non-null for every ticket
        return IntercomSearchTicket(
            ticket_id=ticket_json[TICKET_ID],
            ticket_category=IntercomTicketCategory(ticket_json[CATEGORY]),
            ticket_name=ticket_json[TICKET_ATTRIBUTES][DEFAULT_TITLE],
            description=ticket_json[TICKET_ATTRIBUTES][DEFAULT_DESCRIPTION],
            created_at=datetime.fromtimestamp(created_at_timestamp, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(updated_at_timestamp, tz=timezone.utc),
            ticket_state=IntercomTicketState(ticket_json[TICKET_STATE]),
            raw_ticket_json=ticket_json,
        )

    @staticmethod
    def flatten_contact(contact_json: dict) -> IntercomContact:
        """Flattens Intercom contact JSON into flat fields to match the defined BQ table schema"""

        # "custom_attributes" always exists as a key in the contact JSON, but is often empty
        if (
            CUSTOM_ATTRIBUTES in contact_json
            and STATE_CODE in contact_json[CUSTOM_ATTRIBUTES]
        ):
            state_code = StateCode(contact_json[CUSTOM_ATTRIBUTES][STATE_CODE])
        else:
            state_code = None

        # As of 8/14/26, all contacts created in the past two years have all of these fields
        # (besides state_code, which is accounted for above), so we assume that each field
        # is present and non-null for every contact
        return IntercomContact(
            contact_id=contact_json[ID],
            intercom_external_id=contact_json[EXTERNAL_ID],
            state_code=state_code,
            name=contact_json[NAME],
            email=contact_json[EMAIL],
        )

    @staticmethod
    def write_data_to_csv(
        data_list: list[dict[str, Any]],
        flatten_method: Callable[[dict[str, Any]], Any],
        output_dir: str,
        base_name: str,
    ) -> str:
        """Writes a list of JSON data to a CSV file

        Args:
            data_list: list of JSON data
            flatten_method: method that flattens to JSON data to match the
                schema of the corresponding BQ table
            output_dir: output directory for the CSV
            base_name: the data name, used to construct the CSV filename

        Returns:
            the filepath to the CSV
        """

        flattened_json: list[dict[str, Any]] = [
            flatten_method(data_record).to_json() for data_record in data_list
        ]

        df = pd.DataFrame(flattened_json)

        filename = f"{base_name}.csv"
        output_path = os.path.join(
            output_dir,
            filename,
        )

        df.to_csv(output_path, index=False)
        return output_path

    @classmethod
    def create_inbound_data_csvs(
        cls,
        tickets: list[dict[str, Any]],
        contacts: list[dict[str, Any]],
        output_dir: str,
    ) -> dict[str, str]:
        """Creates CSV files for Inbound Intercom tickets and contacts data

        Args:
            tickets: List of raw ticket JSON
            contacts: List of raw contact JSON
            output_dir: output directory for the CSVs

        Returns:
            dictionary of output filepaths for each data type (tickets and contacts)

        """

        file_paths = {}

        file_paths[TICKETS] = cls.write_data_to_csv(
            data_list=tickets,
            flatten_method=cls.flatten_ticket,
            output_dir=output_dir,
            base_name=TICKETS,
        )

        file_paths[CONTACTS] = cls.write_data_to_csv(
            data_list=contacts,
            flatten_method=cls.flatten_contact,
            output_dir=output_dir,
            base_name=CONTACTS,
        )

        return file_paths
