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

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable

import attr
import pandas as pd

from recidiviz.common.constants.states import StateCode
from recidiviz.intercom.intercom_export_columns import UPDATE_DATETIME_COLUMN_NAME
from recidiviz.intercom.types import (
    IntercomContact,
    IntercomConversation,
    IntercomConversationState,
    IntercomSearchTicket,
    IntercomTicketCategory,
    IntercomTicketState,
)

TICKETS = "tickets"
CONTACTS = "contacts"
CONVERSATIONS = "conversations"
# Singular entity labels used in per-record error/log messages.
TICKET = "ticket"
CONVERSATION = "conversation"
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
# conversation JSON fields
STATE = "state"
PRIORITY = "priority"
TOPICS = "topics"


def _csv_safe(value: Any) -> Any:
    """Serializes dict/list values to JSON strings so they survive the round trip
    through a scalar CSV column as valid JSON, rather than as a Python repr (which
    uses single quotes and True/False/None). Scalars pass through unchanged."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


@attr.define(frozen=True, kw_only=True)
class IntercomJsonToCsvConverter:
    """Translates JSON from Intercom into fields that match the corresponding BQ tables."""

    @staticmethod
    def _first_contact_id(
        entity_json: dict[str, Any], *, entity_label: str, entity_id: str
    ) -> str:
        """Returns the id of the first contact on a ticket or conversation.

        Both nest contacts as entity["contacts"]["contacts"], a list of contact
        objects each with an "id". Intercom lists the contact who started the
        ticket/conversation first; later entries are people added afterwards. We
        pull from this array rather than the author blob because the author is
        usually a bot.
        """
        contacts = entity_json[CONTACTS][CONTACTS]
        if not contacts:
            raise ValueError(
                f"Expected at least one contact for {entity_label} "
                f"[{entity_id}], found none."
            )
        if len(contacts) > 1:
            logging.warning(
                "%s [%s] has [%s] contacts; recording the first as contact_id.",
                entity_label.capitalize(),
                entity_id,
                len(contacts),
            )
        return contacts[0][ID]

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
            contact_id=IntercomJsonToCsvConverter._first_contact_id(
                ticket_json, entity_label=TICKET, entity_id=ticket_json[TICKET_ID]
            ),
            raw_ticket_json=ticket_json,
        )

    @staticmethod
    def flatten_contact(contact_json: dict) -> IntercomContact:
        """Flattens Intercom contact JSON into flat fields to match the defined BQ table schema"""

        state_code = None
        # "custom_attributes" always exists as a key in the contact JSON, but is often empty
        if (
            CUSTOM_ATTRIBUTES in contact_json
            and STATE_CODE in contact_json[CUSTOM_ATTRIBUTES]
        ):
            try:
                state_code = StateCode(contact_json[CUSTOM_ATTRIBUTES][STATE_CODE])
            except ValueError:
                # Some contacts have value `RECIDIVIZ` for their state_code, in those cases
                # we can just leave it as None
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
    def flatten_conversation(conversation_json: dict[str, Any]) -> IntercomConversation:
        """Flattens Intercom conversation JSON into flat fields to match the defined BQ table schema"""

        created_at_timestamp = conversation_json[CREATED_AT]
        updated_at_timestamp = conversation_json[UPDATED_AT]

        # A conversation's topics are its issue types, nested as
        # conversation["topics"]["topics"]. Many conversations have none, in which
        # case Intercom sends "topics" as an empty object or omits it; the inner
        # list can also be absent or null. Dedupe and sort so the serialized value
        # is stable across exports of the same conversation.
        topics = conversation_json.get(TOPICS) or {}
        issue_topics = sorted(
            {topic[NAME] for topic in (topics.get(TOPICS) or []) if NAME in topic}
        )

        return IntercomConversation(
            conversation_id=conversation_json[ID],
            conversation_state=IntercomConversationState(conversation_json[STATE]),
            priority=conversation_json[PRIORITY],
            created_at=datetime.fromtimestamp(created_at_timestamp, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(updated_at_timestamp, tz=timezone.utc),
            contact_id=IntercomJsonToCsvConverter._first_contact_id(
                conversation_json,
                entity_label=CONVERSATION,
                entity_id=conversation_json[ID],
            ),
            issue_topics=issue_topics,
            raw_conversation_json=conversation_json,
        )

    @staticmethod
    def write_data_to_csv(
        data_list: list[dict[str, Any]],
        flatten_method: Callable[[dict[str, Any]], Any],
        output_dir: str,
        base_name: str,
        update_datetime: datetime,
    ) -> str:
        """Writes a list of JSON data to a CSV file, stamping each row with the
        export window's update_datetime.

        Args:
            data_list: list of JSON data
            flatten_method: method that flattens to JSON data to match the
                schema of the corresponding BQ table
            output_dir: output directory for the CSV
            base_name: the data name, used to construct the CSV filename
            update_datetime: the datetime of the export window, written to every row

        Returns:
            the filepath to the CSV
        """
        flattened_json: list[dict[str, Any]] = [
            {k: _csv_safe(v) for k, v in flatten_method(data_record).to_json().items()}
            for data_record in data_list
        ]

        df = pd.DataFrame(flattened_json)
        if data_list:
            df[UPDATE_DATETIME_COLUMN_NAME] = update_datetime

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
        conversations: list[dict[str, Any]],
        output_dir: str,
        update_datetime: datetime,
    ) -> dict[str, str]:
        """Creates CSV files for inbound Intercom tickets, contacts and conversations data

        Args:
            tickets: List of raw ticket JSON
            contacts: List of raw contact JSON
            conversations: List of raw conversation JSON
            output_dir: output directory for the CSVs
            update_datetime: the datetime of the export window, written to every row

        Returns:
            dictionary of output filepaths for each data type (tickets, contacts and conversations)

        """

        data_by_name: list[
            tuple[str, list[dict[str, Any]], Callable[[dict[str, Any]], Any]]
        ] = [
            (TICKETS, tickets, cls.flatten_ticket),
            (CONTACTS, contacts, cls.flatten_contact),
            (CONVERSATIONS, conversations, cls.flatten_conversation),
        ]

        return {
            base_name: cls.write_data_to_csv(
                data_list=data_list,
                flatten_method=flatten_method,
                output_dir=output_dir,
                base_name=base_name,
                update_datetime=update_datetime,
            )
            for base_name, data_list, flatten_method in data_by_name
        }
