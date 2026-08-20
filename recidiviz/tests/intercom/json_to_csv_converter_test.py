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
import os
import tempfile
import unittest
from datetime import datetime, timezone

from recidiviz.common.constants.states import StateCode
from recidiviz.intercom.json_to_csv_converter import (
    CATEGORY,
    CONTACTS,
    CREATED_AT,
    CUSTOM_ATTRIBUTES,
    DEFAULT_DESCRIPTION,
    DEFAULT_TITLE,
    EMAIL,
    EXTERNAL_ID,
    ID,
    NAME,
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
    IntercomSearchTicket,
    IntercomTicketCategory,
    IntercomTicketState,
)

RAW_TICKET_JSON = "raw_ticket_json"


class TestIntercomSchemaTranslator(unittest.TestCase):
    """Test IntercomSchemaTranslator properly translates Intercom inbound data JSON and creates CSV files"""

    def setUp(self) -> None:
        self.json_to_csv_converter = IntercomJsonToCsvConverter()

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

    def test_flatten_ticket(self) -> None:
        """Tests that flatten_ticket() properly parses the ticket JSON."""

        ticket = self.json_to_csv_converter.flatten_ticket(ticket_json=self.ticket_json)

        assert ticket == self.intercom_search_ticket

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

    def test_write_data_to_csv(self) -> None:
        """Tests that write_data_to_csv properly writes JSON data to a CSV file"""

        with tempfile.TemporaryDirectory() as output_dir:
            file_path = self.json_to_csv_converter.write_data_to_csv(
                data_list=[self.ticket_json],
                flatten_method=self.json_to_csv_converter.flatten_ticket,
                output_dir=output_dir,
                base_name=TICKETS,
            )

            self.assertEqual(
                os.path.join(output_dir, f"{TICKETS}.csv"),
                file_path,
            )

            expected_data = self.intercom_search_ticket.to_json()
            # csv.DictReader will convert all values to strings
            expected_data[CREATED_AT] = str(expected_data[CREATED_AT])
            expected_data[UPDATED_AT] = str(expected_data[UPDATED_AT])
            expected_data[RAW_TICKET_JSON] = str(expected_data[RAW_TICKET_JSON])
            print("expected", expected_data)

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                actual_data = [dict(row) for row in reader]
                print("actual", actual_data)

                self.assertEqual(
                    actual_data,
                    [expected_data],
                    "CSV contents do not match the original ticket JSON",
                )

    def test_create_inbound_data_csvs(self) -> None:
        """Tests that create_inbound_data_csvs() properly creates CSV files for tickets and contacts data."""

        with tempfile.TemporaryDirectory() as output_dir:
            file_paths = self.json_to_csv_converter.create_inbound_data_csvs(
                tickets=[self.ticket_json],
                contacts=[self.contact_json],
                output_dir=output_dir,
            )

            self.assertEqual(
                {
                    TICKETS: os.path.join(output_dir, f"{TICKETS}.csv"),
                    CONTACTS: os.path.join(output_dir, f"{CONTACTS}.csv"),
                },
                file_paths,
            )
