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
"""Tests IntercomJsonToCsvConverter"""

import csv
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

from recidiviz.common.constants.states import StateCode
from recidiviz.intercom.intercom_export_columns import UPDATE_DATETIME_COLUMN_NAME
from recidiviz.intercom.json_to_csv_converter import (
    CATEGORY,
    CONTACTS,
    CONVERSATIONS,
    CREATED_AT,
    CUSTOM_ATTRIBUTES,
    DEFAULT_DESCRIPTION,
    DEFAULT_TITLE,
    EMAIL,
    EXTERNAL_ID,
    ID,
    NAME,
    PRIORITY,
    STATE,
    STATE_CODE,
    TICKET_ATTRIBUTES,
    TICKET_ID,
    TICKET_STATE,
    TICKETS,
    UPDATED_AT,
    IntercomJsonToCsvConverter,
)
from recidiviz.intercom.types import (
    IntercomContact,
    IntercomConversation,
    IntercomConversationState,
    IntercomSearchTicket,
    IntercomTicketCategory,
    IntercomTicketState,
)

RAW_TICKET_JSON = "raw_ticket_json"
RAW_CONVERSATION_JSON = "raw_conversation_json"


class TestIntercomSchemaTranslator(unittest.TestCase):
    """Test IntercomSchemaTranslator properly translates Intercom inbound data JSON and creates CSV files"""

    def setUp(self) -> None:
        self.json_to_csv_converter = IntercomJsonToCsvConverter()
        self.update_datetime = datetime(2026, 8, 25, tzinfo=timezone.utc)

        self.ticket_json: dict = {
            "type": "ticket",
            "id": "000000000000000",
            "ticket_id": "000000000",
            "ticket_attributes": {
                "_default_title_": "Test Title",
                "_default_description_": "Test description...",
            },
            "created_at": 1784775861,
            "updated_at": 1785956960,
            "ticket_state": "resolved",
            "category": "Customer",
            "contacts": {
                "type": "contact.list",
                "contacts": [
                    {"type": "contact", "id": "5ba682d23d7cf92bef87bfd4"},
                ],
            },
        }
        self.intercom_search_ticket = IntercomSearchTicket(
            ticket_id=self.ticket_json[TICKET_ID],
            ticket_category=IntercomTicketCategory(self.ticket_json[CATEGORY]),
            ticket_name=self.ticket_json[TICKET_ATTRIBUTES][DEFAULT_TITLE],
            description=self.ticket_json[TICKET_ATTRIBUTES][DEFAULT_DESCRIPTION],
            created_at=datetime.fromtimestamp(
                self.ticket_json[CREATED_AT], tz=timezone.utc
            ),
            updated_at=datetime.fromtimestamp(
                self.ticket_json[UPDATED_AT], tz=timezone.utc
            ),
            ticket_state=IntercomTicketState(self.ticket_json[TICKET_STATE]),
            contact_id="5ba682d23d7cf92bef87bfd4",
            raw_ticket_json=self.ticket_json,
        )

        self.contact_json: dict = {
            "type": "contact",
            "id": "5ba682d23d7cf92bef87bfd4",
            "external_id": "25",
            "role": "user",
            "email": "email@example.com",
            "name": "Joe Example",
            "custom_attributes": {
                "state_code": "US_TN",
            },
        }
        self.intercom_contact = IntercomContact(
            contact_id=self.contact_json[ID],
            intercom_external_id=self.contact_json[EXTERNAL_ID],
            state_code=StateCode(self.contact_json[CUSTOM_ATTRIBUTES][STATE_CODE]),
            name=self.contact_json[NAME],
            email=self.contact_json[EMAIL],
        )

        self.conversation_json: dict = {
            "type": "conversation",
            "id": "123456789",
            "created_at": 1784775861,
            "updated_at": 1785956960,
            "state": "closed",
            "priority": "high",
            "contacts": {
                "type": "contact.list",
                "contacts": [
                    {"type": "contact", "id": "5ba682d23d7cf92bef87bfd4"},
                ],
            },
            "topics": {
                "type": "topic.list",
                "topics": [
                    {"type": "topic", "id": 1, "name": "Data accuracy challenge"},
                    {"type": "topic", "id": 2, "name": "Access requests"},
                ],
                "total_count": 2,
            },
        }
        self.intercom_conversation = IntercomConversation(
            conversation_id=self.conversation_json[ID],
            conversation_state=IntercomConversationState(self.conversation_json[STATE]),
            priority=self.conversation_json[PRIORITY],
            created_at=datetime.fromtimestamp(
                self.conversation_json[CREATED_AT], tz=timezone.utc
            ),
            updated_at=datetime.fromtimestamp(
                self.conversation_json[UPDATED_AT], tz=timezone.utc
            ),
            contact_id="5ba682d23d7cf92bef87bfd4",
            # Sorted and deduped, so independent of Intercom's response order.
            issue_topics=["Access requests", "Data accuracy challenge"],
            raw_conversation_json=self.conversation_json,
        )

    def test_flatten_ticket(self) -> None:
        """Tests that flatten_ticket() properly parses the ticket JSON."""

        ticket = self.json_to_csv_converter.flatten_ticket(ticket_json=self.ticket_json)

        assert ticket == self.intercom_search_ticket

    def test_flatten_ticket_no_contacts(self) -> None:
        """flatten_ticket() raises when a ticket has no contacts."""

        ticket_json = {**self.ticket_json}
        ticket_json["contacts"] = {"type": "contact.list", "contacts": []}
        with self.assertRaisesRegex(
            ValueError, r"Expected at least one contact for ticket"
        ):
            self.json_to_csv_converter.flatten_ticket(ticket_json=ticket_json)

    def test_flatten_ticket_multiple_contacts_records_first(self) -> None:
        """flatten_ticket() records the first contact and warns when a ticket has several."""

        ticket_json = {**self.ticket_json}
        ticket_json["contacts"] = {
            "type": "contact.list",
            "contacts": [
                {"type": "contact", "id": "5ba682d23d7cf92bef87bfd4"},
                {"type": "contact", "id": "6cb793e34e8da03cfa98cae5"},
            ],
        }

        with self.assertLogs(level="WARNING") as logs:
            ticket = self.json_to_csv_converter.flatten_ticket(ticket_json=ticket_json)

        assert ticket.contact_id == "5ba682d23d7cf92bef87bfd4"
        # Both contacts stay recoverable from the raw JSON we persist.
        assert len(ticket.raw_ticket_json["contacts"]["contacts"]) == 2
        assert "has [2] contacts" in "".join(logs.output)

    def test_flatten_contact(self) -> None:
        """Tests that flatten_contact() properly parses the contact JSON"""

        contact = self.json_to_csv_converter.flatten_contact(
            contact_json=self.contact_json
        )

        assert contact == self.intercom_contact

    def test_flatten_contact_missing_state_code(self) -> None:
        """Tests that flatten_contact() properly parses the contact JSON"""

        contact_json: dict = {
            "type": "contact",
            "id": "5ba682d23d7cf92bef87bfd4",
            "external_id": "25",
            "role": "user",
            "email": "email@example.com",
            "name": "Joe Example",
            "custom_attributes": {},
        }

        contact = self.json_to_csv_converter.flatten_contact(contact_json=contact_json)

        assert contact == IntercomContact(
            contact_id=contact_json[ID],
            intercom_external_id=contact_json[EXTERNAL_ID],
            state_code=None,
            name=contact_json[NAME],
            email=contact_json[EMAIL],
        )

    def test_flatten_conversation(self) -> None:
        """Tests that flatten_conversation() properly parses the conversation JSON."""

        conversation = self.json_to_csv_converter.flatten_conversation(
            conversation_json=self.conversation_json
        )

        assert conversation == self.intercom_conversation

    def test_flatten_conversation_no_contacts(self) -> None:
        """flatten_conversation() raises when a conversation has no contacts."""

        conversation_json = {**self.conversation_json}
        conversation_json["contacts"] = {"type": "contact.list", "contacts": []}
        with self.assertRaisesRegex(
            ValueError, r"Expected at least one contact for conversation"
        ):
            self.json_to_csv_converter.flatten_conversation(
                conversation_json=conversation_json
            )

    def test_flatten_conversation_multiple_contacts_records_first(self) -> None:
        """flatten_conversation() records the first contact and warns when a conversation has several."""

        conversation_json = {**self.conversation_json}
        conversation_json["contacts"] = {
            "type": "contact.list",
            "contacts": [
                {"type": "contact", "id": "5ba682d23d7cf92bef87bfd4"},
                {"type": "contact", "id": "6cb793e34e8da03cfa98cae5"},
            ],
        }

        with self.assertLogs(level="WARNING") as logs:
            conversation = self.json_to_csv_converter.flatten_conversation(
                conversation_json=conversation_json
            )

        assert conversation.contact_id == "5ba682d23d7cf92bef87bfd4"
        assert len(conversation.raw_conversation_json["contacts"]["contacts"]) == 2
        assert "has [2] contacts" in "".join(logs.output)

    def _write_single_conversation_row(self, conversation_json: dict) -> dict[str, str]:
        """Writes one conversation to CSV via write_data_to_csv and returns its row."""
        with tempfile.TemporaryDirectory() as output_dir:
            file_path = self.json_to_csv_converter.write_data_to_csv(
                data_list=[conversation_json],
                flatten_method=self.json_to_csv_converter.flatten_conversation,
                output_dir=output_dir,
                base_name=CONVERSATIONS,
                update_datetime=self.update_datetime,
            )
            with open(file_path, "r", encoding="utf-8") as f:
                return list(csv.DictReader(f))[0]

    def test_conversation_raw_json_serialized_as_valid_json(self) -> None:
        """The raw conversation blob is written to the CSV as valid JSON (not a
        Python dict repr), so downstream views can JSON_EXTRACT a conversation's
        topics (issue type) and tags (state code) from it."""

        conversation_json = {
            **self.conversation_json,
            "topics": {
                "type": "topic.list",
                "topics": [{"type": "topic", "id": 1, "name": "Eligibility inquiry"}],
            },
            "tags": {
                "type": "tag.list",
                "tags": [{"type": "tag", "id": "1", "name": "TN"}],
            },
            # Intercom sends Python-incompatible JSON literals (false/true/null)
            # that a dict repr would mangle.
            "custom_attributes": {"Auto-translated": False},
        }

        raw_json = self._write_single_conversation_row(conversation_json)[
            RAW_CONVERSATION_JSON
        ]

        # Guard against the blob being written as a dict repr (invalid JSON with
        # single quotes / True / False / None).
        self.assertEqual(json.dumps(conversation_json), raw_json)
        self.assertEqual(conversation_json, json.loads(raw_json))

    def test_conversation_issue_topics_serialized_as_json_array(self) -> None:
        """A conversation's issue topics are flattened to a sorted, deduped list and
        written to the CSV as a JSON array string, so downstream views can
        JSON_EXTRACT_ARRAY them from the single column."""

        issue_topics_str = self._write_single_conversation_row(self.conversation_json)[
            "issue_topics"
        ]

        self.assertEqual(
            ["Access requests", "Data accuracy challenge"],
            json.loads(issue_topics_str),
        )

    def test_flatten_conversation_no_topics(self) -> None:
        """flatten_conversation() yields empty issue_topics when a conversation has
        no topics, whether the topics key is absent, an empty object, an empty
        inner list, or a null inner list."""

        without_topics_key = {
            k: v for k, v in self.conversation_json.items() if k != "topics"
        }
        empty_topics_object = {**self.conversation_json, "topics": {}}
        empty_topics_list = {
            **self.conversation_json,
            "topics": {"type": "topic.list", "topics": [], "total_count": 0},
        }
        null_topics_list = {
            **self.conversation_json,
            "topics": {"type": "topic.list", "topics": None},
        }

        for conversation_json in [
            without_topics_key,
            empty_topics_object,
            empty_topics_list,
            null_topics_list,
        ]:
            conversation = self.json_to_csv_converter.flatten_conversation(
                conversation_json=conversation_json
            )
            self.assertEqual([], conversation.issue_topics)

    def test_flatten_conversation_skips_topic_without_name(self) -> None:
        """A topic object missing a 'name' is skipped rather than crashing the row."""

        conversation_json = {
            **self.conversation_json,
            "topics": {
                "type": "topic.list",
                "topics": [
                    {"type": "topic", "id": 1, "name": "Access requests"},
                    {"type": "topic", "id": 2},
                ],
            },
        }

        conversation = self.json_to_csv_converter.flatten_conversation(
            conversation_json=conversation_json
        )

        self.assertEqual(["Access requests"], conversation.issue_topics)

    def test_write_data_to_csv(self) -> None:
        """Tests that write_data_to_csv properly writes JSON data to a CSV file"""

        with tempfile.TemporaryDirectory() as output_dir:
            file_path = self.json_to_csv_converter.write_data_to_csv(
                data_list=[self.ticket_json],
                flatten_method=self.json_to_csv_converter.flatten_ticket,
                output_dir=output_dir,
                base_name=TICKETS,
                update_datetime=self.update_datetime,
            )

            self.assertEqual(
                os.path.join(output_dir, f"{TICKETS}.csv"),
                file_path,
            )

            expected_data = self.intercom_search_ticket.to_json()
            # csv.DictReader will convert all values to strings
            expected_data[CREATED_AT] = str(expected_data[CREATED_AT])
            expected_data[UPDATED_AT] = str(expected_data[UPDATED_AT])
            # The raw blob is written as valid JSON, not a Python dict repr.
            expected_data[RAW_TICKET_JSON] = json.dumps(self.ticket_json)
            expected_data[UPDATE_DATETIME_COLUMN_NAME] = str(self.update_datetime)

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                actual_data = [dict(row) for row in reader]

                self.assertEqual(
                    actual_data,
                    [expected_data],
                    "CSV contents do not match the original ticket JSON",
                )

    def test_write_data_to_csv_no_data(self) -> None:
        """write_data_to_csv() writes an empty CSV (with no lone update_datetime column) when there is no data."""

        with tempfile.TemporaryDirectory() as output_dir:
            file_path = self.json_to_csv_converter.write_data_to_csv(
                data_list=[],
                flatten_method=self.json_to_csv_converter.flatten_ticket,
                output_dir=output_dir,
                base_name=TICKETS,
                update_datetime=self.update_datetime,
            )

            self.assertEqual(os.path.join(output_dir, f"{TICKETS}.csv"), file_path)

            with open(file_path, "r", encoding="utf-8") as f:
                self.assertEqual("\n", f.read())

    def test_create_inbound_data_csvs(self) -> None:
        """Tests that create_inbound_data_csvs() properly creates CSV files for tickets, contacts and conversations data."""

        with tempfile.TemporaryDirectory() as output_dir:
            file_paths = self.json_to_csv_converter.create_inbound_data_csvs(
                tickets=[self.ticket_json],
                contacts=[self.contact_json],
                conversations=[self.conversation_json],
                output_dir=output_dir,
                update_datetime=self.update_datetime,
            )

            self.assertEqual(
                {
                    TICKETS: os.path.join(output_dir, f"{TICKETS}.csv"),
                    CONTACTS: os.path.join(output_dir, f"{CONTACTS}.csv"),
                    CONVERSATIONS: os.path.join(output_dir, f"{CONVERSATIONS}.csv"),
                },
                file_paths,
            )

            for path in file_paths.values():
                with open(path, "r", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
                for row in rows:
                    self.assertEqual(
                        str(self.update_datetime), row[UPDATE_DATETIME_COLUMN_NAME]
                    )
