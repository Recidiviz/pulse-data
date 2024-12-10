# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""
Tool for hydrating test data to the JII Pilots Firebase application.

Example usage:
python -m recidiviz.tools.jii.hydrate_test_data
"""

import datetime

from recidiviz.firestore.firestore_client import FirestoreClientImpl

TEST_COLLECTION_NAME = "TEST_twilio_messages"
TEST_COLLECTION_GROUP = "TEST_lsu_eligibility_messages"
TEST_BATCH_ID = "12_09_2024_11_11_11"
NUM_DOCUMENTS_TO_ADD = 1000
TEST_DATETIME_ADDED = datetime.datetime.fromisoformat("2024-12-01T00:00:00")
TEST_PHONE_NUMBER = "1234567890"


def generate_test_firestore_data() -> None:
    """
    Used to generate test Firestore data with the shape of the eligibility messages
    """
    firestore_client = FirestoreClientImpl(project_id="jii-pilots")

    # Reference to the collection
    collection_ref = firestore_client.get_collection(TEST_COLLECTION_NAME)

    # Empty collection
    firestore_client.delete_collection(TEST_COLLECTION_NAME)
    print(f"Cleared the {TEST_COLLECTION_NAME} collection")

    # Create a batch
    batch = firestore_client.batch()

    # Add documents to the batch
    for i in range(1, NUM_DOCUMENTS_TO_ADD + 1):
        external_id = str(i).zfill(4)

        # Add the top level document
        document_id = f"us_xx_{external_id}"
        batch.set(collection_ref.document(document_id), {})

        # Add a document for the initial text
        message_document_id = (
            f"{document_id}/{TEST_COLLECTION_GROUP}/eligibility_{TEST_BATCH_ID}"
        )
        batch.set(
            collection_ref.document(message_document_id),
            {
                "timestamp": TEST_DATETIME_ADDED,
                "message_sid": external_id,
                "body": "Hello world",
                "phone_number": TEST_PHONE_NUMBER,
                "message_type": "INITIAL_TEXT",
                "state_code": "us_xx",
                "external_id": external_id,
                "batch_id": TEST_BATCH_ID,
                "po_name": "John Doe",
                "district": "District X",
            },
        )

    batch.commit()
    print(
        f"Successfully added {NUM_DOCUMENTS_TO_ADD} documents to {TEST_COLLECTION_NAME} collection"
    )


if __name__ == "__main__":
    generate_test_firestore_data()
