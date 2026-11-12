#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for the Workflows ETL delegates."""
import io
from datetime import datetime, timezone
from typing import Any
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import MagicMock, call, patch

from freezegun import freeze_time

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import (
    WorkflowsETLDelegate,
    WorkflowsFirestoreETLDelegate,
)

SEED = "A123456"
FILENAME = "export_filename.json"


class TestETLDelegate(WorkflowsETLDelegate):
    async def run_etl(self) -> None:
        pass

    @classmethod
    def supports_file(cls, state_code: StateCode, filename: str) -> bool:
        return True


class TestETLDelegateWithPseudonymizedFields(TestETLDelegate):
    PSEUDONYMIZED_FIELDS = {
        "person_name": {
            "given_names": "first_name",
            "middle_names": "first_name",
            "surname": "last_name",
        },
        "address": "address",
        "current_physical_residence_address_unstructured": "address",
        "current_physical_residence_address_structured": {
            "address_line_1": "street_address",
            "address_line_2": "secondary_address",
            "address_city": "city",
            "address_zip": "zipcode",
        },
        "phone_number": "phone_number",
        "email_address": "email",
    }


class TestWorkflowsETLDelegate(TestCase):
    """Tests for the Workflows ETL delegate."""

    def test_get_filepath_uses_project_id(self) -> None:
        """Tests that get_filepath() incorporates the current project ID."""
        with local_project_id_override("test-project"):
            delegate = TestETLDelegate(StateCode.US_XX, FILENAME)
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="test-project-practices-etl-data",
                    blob_name="US_XX/export_filename.json",
                ),
                delegate.get_filepath(),
            )


class TestPseudonymizeRecord(TestCase):
    """Tests for WorkflowsETLDelegate.pseudonymize_record."""

    def test_returns_none_without_pseudonymized_fields(self) -> None:
        delegate = TestETLDelegate(StateCode.US_XX, FILENAME)
        record = {
            "person_external_id": "A123456",
            "state_code": "US_XX",
            "email_address": "jane.realperson@testdomain.com",
        }
        self.assertIsNone(delegate.pseudonymize_record(record, seed=SEED))

    def test_replaces_only_configured_fields(self) -> None:
        delegate = TestETLDelegateWithPseudonymizedFields(StateCode.US_XX, FILENAME)
        record = {
            "person_external_id": "A123456",
            "state_code": "US_XX",
            "person_name": {
                "given_names": "Jane",
                "surname": "Realperson",
                "name_suffix": "Jr",
            },
            "address": "123 Real St. Real City, XX",
            "current_physical_residence_address_unstructured": "123 Real St. Real City, XX",
            "current_physical_residence_address_structured": {
                "address_line_1": "123 Real St.",
                "address_line_2": "Apt. 1",
                "address_city": "Real City",
                "address_zip": "00000",
            },
            "phone_number": "1234567890",
            "email_address": "jane.realperson@testdomain.com",
        }
        self.assertEqual(
            {
                "person_external_id": "A123456",
                "state_code": "US_XX",
                "person_name": {
                    "given_names": "Sandra",
                    "surname": "Hall",
                    "name_suffix": "Jr",
                },
                "address": "1830 Williamson Union\nJefferyshire, IA 25024",
                "current_physical_residence_address_unstructured": "835 Jonathan Stravenue Suite 098\nJuantown, MI 79020",
                "current_physical_residence_address_structured": {
                    "address_line_1": "23373 Jones Drive Apt. 545",
                    "address_line_2": "Apt. 673",
                    "address_city": "East Kristyville",
                    "address_zip": "01662",
                },
                "phone_number": "390.602.2218x2934",
                "email_address": "luis76@example.com",
            },
            delegate.pseudonymize_record(record, seed=SEED),
        )

    def test_does_not_mutate_input_record(self) -> None:
        delegate = TestETLDelegateWithPseudonymizedFields(StateCode.US_XX, FILENAME)
        record = {
            "email_address": "jane.realperson@testdomain.com",
            "person_name": {"given_names": "Jane", "surname": "Realperson"},
        }
        delegate.pseudonymize_record(record, seed=SEED)
        self.assertEqual(
            {
                "email_address": "jane.realperson@testdomain.com",
                "person_name": {"given_names": "Jane", "surname": "Realperson"},
            },
            record,
        )

    def test_no_change_to_record_when_pseudonymized_field_missing_from_record(
        self,
    ) -> None:
        class TestETLDelegateWithEmailPseudonymizedField(TestETLDelegate):
            PSEUDONYMIZED_FIELDS = {"email_address": "email"}

        delegate = TestETLDelegateWithEmailPseudonymizedField(StateCode.US_XX, FILENAME)
        result = delegate.pseudonymize_record(record={"state_code": "US_XX"}, seed=SEED)
        self.assertEqual({"state_code": "US_XX"}, result)

    def test_returns_none_when_nested_field_is_not_a_dict(self) -> None:
        class TestETLDelegateWithAddressPseudonymizedField(TestETLDelegate):
            PSEUDONYMIZED_FIELDS = {"address": {"address_city": "city"}}

        delegate = TestETLDelegateWithAddressPseudonymizedField(
            StateCode.US_XX, FILENAME
        )
        with self.assertLogs(level="INFO") as logs:
            self.assertIsNone(
                delegate.pseudonymize_record(
                    record={"address": "123 Real St"}, seed=SEED
                )
            )
        self.assertEqual(1, len(logs.records))
        self.assertIn(
            "Failed to create the pseudonymized record. Expected a json dictionary "
            "string or dictionary for field: address on record {'address': '123 Real "
            "St'}.",
            logs.output[0],
        )

    def test_returns_none_when_nested_field_is_not_a_str(self) -> None:
        class TestETLDelegateWithAddressPseudonymizedField(TestETLDelegate):
            PSEUDONYMIZED_FIELDS = {"address": "address"}

        delegate = TestETLDelegateWithAddressPseudonymizedField(
            StateCode.US_XX, FILENAME
        )
        with self.assertLogs(level="INFO") as logs:
            self.assertIsNone(
                delegate.pseudonymize_record(
                    record={"address": {"address_city": "RealCity"}}, seed=SEED
                )
            )
        self.assertEqual(1, len(logs.records))
        self.assertIn(
            "Failed to create the pseudonymized record. Expected a string, integer, "
            "or double value for field: address on record: {'address': {'address_city': "
            "'RealCity'}}. Did not expect a dictionary.",
            logs.output[0],
        )

    def test_replaces_fields_nested_in_json_string(self) -> None:
        delegate = TestETLDelegateWithPseudonymizedFields(StateCode.US_XX, FILENAME)
        record = {
            "person_external_id": "A123456",
            "person_name": '{"given_names": "Jane", "surname": "Realperson", '
            '"name_suffix": "Jr"}',
        }
        self.assertEqual(
            {
                "person_external_id": "A123456",
                "person_name": '{"given_names": "Sandra", "surname": "Hall", '
                '"name_suffix": "Jr"}',
            },
            delegate.pseudonymize_record(record, seed=SEED),
        )

    def test_returns_none_when_json_string_field_is_not_expected_object(self) -> None:
        delegate = TestETLDelegateWithPseudonymizedFields(StateCode.US_XX, FILENAME)
        with self.assertLogs(level="INFO") as logs:
            self.assertIsNone(
                delegate.pseudonymize_record(
                    record={"person_name": '["Jane", "Realperson"]'}, seed=SEED
                )
            )
        self.assertEqual(1, len(logs.records))
        self.assertIn(
            "Failed to create the pseudonymized record. Expected a json dictionary "
            "string or dictionary for field: person_name on record {'person_name': "
            '\'["Jane", "Realperson"]\'}.',
            logs.output[0],
        )

    def test_returns_none_when_doubly_nested_field_does_not_match(self) -> None:
        class TestETLDelegateWithDoublyNestedPseudonymizedField(TestETLDelegate):
            PSEUDONYMIZED_FIELDS = {"metadata": {"address": {"address_city": "city"}}}

        delegate = TestETLDelegateWithDoublyNestedPseudonymizedField(
            StateCode.US_XX, FILENAME
        )
        with self.assertLogs(level="INFO") as logs:
            self.assertIsNone(
                delegate.pseudonymize_record(
                    record={"metadata": '{"address": "123 Real St"}'}, seed=SEED
                )
            )
        self.assertEqual(1, len(logs.records))
        self.assertIn(
            "Failed to create the pseudonymized record. Expected a json dictionary "
            "string or dictionary for field: address on record {'address': '123 Real "
            "St'}.",
            logs.output[0],
        )

    def test_raises_on_unknown_provider(self) -> None:
        class TestETLDelegateWithIncorrectPseudonymizedField(TestETLDelegate):
            PSEUDONYMIZED_FIELDS = {"person_name": "person.first_name"}

        delegate = TestETLDelegateWithIncorrectPseudonymizedField(
            StateCode.US_XX, FILENAME
        )
        with self.assertRaisesRegex(
            AttributeError,
            r"^'Generator' object has no attribute 'person\.first_name'$",
        ):
            delegate.pseudonymize_record(  # pylint: disable=protected-access
                record={"person_name": "Jane"},
                seed=SEED,
            )


class TestFirestoreETLDelegate(WorkflowsFirestoreETLDelegate):
    @classmethod
    def supports_file(cls, state_code: StateCode, filename: str) -> bool:
        return True

    def __init__(self, state_code: StateCode, filename: str):
        super().__init__(state_code, filename)
        self.collection_name = "testCollection"

    def transform_row(self, row: dict[str, Any]) -> tuple[str | None, dict | None]:
        return row["id"], row["document"]


class TestFirestoreETLDelegateWithPseudonymizedFields(TestFirestoreETLDelegate):
    PSEUDONYMIZED_FIELDS = {"document": {"name": "first_name"}}


class FakeAsyncBatch:
    """
    Fake Firestore batch that records the documents set on it and, on commit,
    appends them to committed_batches.
    """

    def __init__(self, committed_batches: list[dict[str, dict]]) -> None:
        self._documents: dict[str, dict] = {}
        self._committed_batches = committed_batches

    def set(self, document_id: str, document: dict) -> None:
        self._documents[document_id] = document

    async def commit(self) -> None:
        self._committed_batches.append(self._documents)

    def __len__(self) -> int:
        return len(self._documents)


class TestWorkflowsFirestoreETLDelegateRunETL(IsolatedAsyncioTestCase):
    """Tests for WorkflowsFirestoreETLDelegate.run_etl."""

    def setUp(self) -> None:
        self.committed_batches: list[dict[str, dict]] = []

        self.mock_firestore_client = self.enterContext(
            patch("recidiviz.workflows.etl.workflows_etl_delegate.FirestoreClientImpl")
        ).return_value
        self.mock_firestore_client.async_batch.side_effect = lambda: FakeAsyncBatch(
            self.committed_batches
        )
        # Return the document ID itself as the document reference so that committed
        # batches are keyed by document ID.
        self.mock_firestore_client.get_collection.return_value.document.side_effect = (
            lambda document_id: document_id
        )

        self.mock_get_file_stream = self.enterContext(
            patch.object(WorkflowsETLDelegate, "get_file_stream")
        )
        self.enterContext(local_project_id_override("test-project"))

        self.delegate = TestFirestoreETLDelegate(StateCode.US_XX, FILENAME)

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    async def test_run_etl_writes_transformed_documents(self) -> None:
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX", "name": "Ann"}}\n'
                '{"id": "B2", "document": {"stateCode": "US_XX", "name": "Bea"}}\n'
            )
        ]

        await self.delegate.run_etl()

        self.assertEqual(
            [
                {
                    "us_xx_a1": {
                        "stateCode": "US_XX",
                        "name": "Ann",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                    "us_xx_b2": {
                        "stateCode": "US_XX",
                        "name": "Bea",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                }
            ],
            self.committed_batches,
        )
        self.assertEqual(2, self.delegate.total_records_written)

        # Verify that a new index was created for the collection.
        self.assertEqual(
            [call("testCollection")],
            self.mock_firestore_client.index_exists_for_collection.call_args_list,
        )

        # Verify that old documents were deleted.
        self.assertEqual(
            [
                call(
                    "testCollection",
                    "US_XX",
                    "__loadedAt",
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                )
            ],
            self.mock_firestore_client.delete_old_documents.call_args_list,
        )

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    async def test_run_etl_skips_rows_that_are_not_valid_json(self) -> None:
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX"}}\nnot valid json\n'
            )
        ]

        with self.assertLogs(level="ERROR") as logs:
            await self.delegate.run_etl()

        self.assertEqual(
            [
                {
                    "us_xx_a1": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                }
            ],
            self.committed_batches,
        )
        self.assertEqual(1, self.delegate.total_records_written)
        self.assertEqual(1, len(logs.records))
        self.assertIn("Failed to parse row [not valid json", logs.output[0])

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    async def test_run_etl_skips_rows_that_fail_to_transform(self) -> None:
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX"}}\n'
                '{"document": {"stateCode": "US_XX"}}\n'
            )
        ]

        with self.assertLogs(level="ERROR") as logs:
            await self.delegate.run_etl()

        self.assertEqual(
            [
                {
                    "us_xx_a1": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                }
            ],
            self.committed_batches,
        )
        self.assertEqual(1, self.delegate.total_records_written)
        self.assertEqual(1, len(logs.records))
        self.assertIn(
            "Transform row failed on [{'document': {'stateCode': 'US_XX'}}]",
            logs.output[0],
        )

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    async def test_run_etl_skips_rows_without_id_or_document(self) -> None:
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX"}}\n'
                '{"id": null, "document": {"stateCode": "US_XX"}}\n'
                '{"id": "C3", "document": null}\n'
            )
        ]

        await self.delegate.run_etl()

        self.assertEqual(
            [
                {
                    "us_xx_a1": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                }
            ],
            self.committed_batches,
        )
        self.assertEqual(1, self.delegate.total_records_written)

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    async def test_run_etl_skips_rows_without_state_code(self) -> None:
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX", "name": "Ann"}}\n'
                '{"id": "B2", "document": {"name": "Bea"}}\n'
            )
        ]

        with self.assertLogs(level="ERROR") as logs:
            await self.delegate.run_etl()

        self.assertEqual(
            [
                {
                    "us_xx_a1": {
                        "stateCode": "US_XX",
                        "name": "Ann",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                }
            ],
            self.committed_batches,
        )
        self.assertEqual(1, self.delegate.total_records_written)

        self.assertEqual(1, len(logs.records))
        self.assertIn(
            "Skipping row [B2] in [export_filename.json]: document has no [stateCode]",
            logs.output[0],
        )

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    @patch(
        "recidiviz.workflows.etl.workflows_etl_delegate.MAX_FIRESTORE_RECORDS_PER_BATCH",
        2,
    )
    async def test_run_etl_splits_documents_into_batches(self) -> None:
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX"}}\n'
                '{"id": "B2", "document": {"stateCode": "US_XX"}}\n'
                '{"id": "C3", "document": {"stateCode": "US_XX"}}\n'
            )
        ]

        await self.delegate.run_etl()

        self.assertEqual(
            [
                {
                    "us_xx_a1": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                    "us_xx_b2": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                },
                {
                    "us_xx_c3": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                },
            ],
            self.committed_batches,
        )
        self.assertEqual(3, self.delegate.total_records_written)

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    @patch("recidiviz.workflows.etl.workflows_etl_delegate.INPUT_CHUNK_SIZE", 1)
    async def test_run_etl_commits_each_input_chunk_separately(self) -> None:
        # With a 1-byte chunk size, each readlines call returns a single line.
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX"}}\n'
                '{"id": "B2", "document": {"stateCode": "US_XX"}}\n'
            )
        ]

        await self.delegate.run_etl()

        self.assertEqual(
            [
                {
                    "us_xx_a1": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                },
                {
                    "us_xx_b2": {
                        "stateCode": "US_XX",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                },
            ],
            self.committed_batches,
        )
        self.assertEqual(2, self.delegate.total_records_written)

    @freeze_time(datetime(2026, 1, 1, tzinfo=timezone.utc))
    async def test_run_etl_writes_pseudonymized_documents(self) -> None:
        def build_collection(collection_name: str) -> MagicMock:
            collection = MagicMock()
            # Prefix document references with the collection name so that committed
            # batches show which collection each document was written to.
            collection.document.side_effect = (
                lambda document_id: f"{collection_name}/{document_id}"
            )
            return collection

        self.mock_firestore_client.get_collection.side_effect = build_collection
        self.mock_get_file_stream.return_value = [
            io.StringIO(
                '{"id": "A1", "document": {"stateCode": "US_XX", "name": "Ann"}}\n'
            )
        ]
        delegate = TestFirestoreETLDelegateWithPseudonymizedFields(
            StateCode.US_XX, FILENAME
        )

        await delegate.run_etl()

        self.assertEqual(
            [
                {
                    "testCollection/us_xx_a1": {
                        "stateCode": "US_XX",
                        "name": "Ann",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                    "PSEUDONYMIZED_testCollection/us_xx_a1": {
                        "stateCode": "US_XX",
                        "name": "Alyssa",
                        "__loadedAt": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    },
                }
            ],
            self.committed_batches,
        )
        self.assertEqual(2, delegate.total_records_written)

        # Verify both collection names are indexed.
        self.assertEqual(
            [call("testCollection"), call("PSEUDONYMIZED_testCollection")],
            self.mock_firestore_client.index_exists_for_collection.call_args_list,
        )

        # Verify old documents are deleted for both collection names.
        self.assertEqual(
            [
                call(
                    "testCollection",
                    "US_XX",
                    "__loadedAt",
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
                call(
                    "PSEUDONYMIZED_testCollection",
                    "US_XX",
                    "__loadedAt",
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
            ],
            self.mock_firestore_client.delete_old_documents.call_args_list,
        )
