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
from typing import List
from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsETLDelegate

SEED = "A123456"


class TestETLDelegate(WorkflowsETLDelegate):
    def get_supported_files(self) -> List[str]:
        return ["export_filename.json"]

    async def run_etl(self, _filename: str) -> None:
        pass


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

    def test_supports_file_matching(self) -> None:
        """Test that the supports_file returns true when delegate supports file."""
        delegate = TestETLDelegate(StateCode.US_XX)
        self.assertTrue(delegate.supports_file("export_filename.json"))
        self.assertFalse(delegate.supports_file("some_other_file.json"))

    def test_supports_file_extension(self) -> None:
        """Test that the supports_file matcher does not ignore the file format."""
        delegate = TestETLDelegate(StateCode.US_XX)
        self.assertTrue(delegate.supports_file("export_filename.json"))
        self.assertFalse(delegate.supports_file("export_filename.csv"))

    def test_get_filepath_uses_project_id(self) -> None:
        """Tests that get_filepath() incorporates the current project ID."""
        with local_project_id_override("test-project"):
            delegate = TestETLDelegate(StateCode.US_XX)
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="test-project-practices-etl-data",
                    blob_name="US_XX/export_filename.json",
                ),
                delegate.get_filepath("export_filename.json"),
            )


class TestPseudonymizeRecord(TestCase):
    """Tests for WorkflowsETLDelegate.pseudonymize_record."""

    def test_returns_none_without_pseudonymized_fields(self) -> None:
        delegate = TestETLDelegate(StateCode.US_XX)
        record = {
            "person_external_id": "A123456",
            "state_code": "US_XX",
            "email_address": "jane.realperson@testdomain.com",
        }
        self.assertIsNone(delegate.pseudonymize_record(record, seed=SEED))

    def test_replaces_only_configured_fields(self) -> None:
        delegate = TestETLDelegateWithPseudonymizedFields(StateCode.US_XX)
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
        delegate = TestETLDelegateWithPseudonymizedFields(StateCode.US_XX)
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

        delegate = TestETLDelegateWithEmailPseudonymizedField(StateCode.US_XX)
        result = delegate.pseudonymize_record(record={"state_code": "US_XX"}, seed=SEED)
        self.assertEqual({"state_code": "US_XX"}, result)

    def test_raises_when_nested_field_is_not_a_dict(self) -> None:
        class TestETLDelegateWithAddressPseudonymizedField(TestETLDelegate):
            PSEUDONYMIZED_FIELDS = {"address": {"address_city": "city"}}

        delegate = TestETLDelegateWithAddressPseudonymizedField(StateCode.US_XX)
        with self.assertRaisesRegex(
            ValueError,
            r"^Expected a dictionary, but got str for field: address on record: "
            r"\{'address': '123 Real St'\}$",
        ):
            delegate.pseudonymize_record(  # pylint: disable=protected-access
                record={"address": "123 Real St"},
                seed=SEED,
            )

    def test_raises_on_unknown_provider(self) -> None:
        class TestETLDelegateWithIncorrectPseudonymizedField(TestETLDelegate):
            PSEUDONYMIZED_FIELDS = {"person_name": "person.first_name"}

        delegate = TestETLDelegateWithIncorrectPseudonymizedField(StateCode.US_XX)
        with self.assertRaisesRegex(
            AttributeError,
            r"^'Generator' object has no attribute 'person\.first_name'$",
        ):
            delegate.pseudonymize_record(  # pylint: disable=protected-access
                record={"person_name": "Jane"},
                seed=SEED,
            )
