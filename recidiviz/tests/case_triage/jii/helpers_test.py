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
"""Implement tests for the helpers.py file"""

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.jii.helpers import (
    ALL_CLOSER,
    FULLY_ELIGIBLE_EXCEPT_FFR,
    FULLY_ELIGIBLE_TEXT,
    INITIAL_TEXT,
    LEARN_MORE,
    MISSING_INCOME,
    MISSING_NEGATIVE_DA,
    MISSING_NEGATIVE_DA_AND_INCOME,
    VISIT,
    generate_eligibility_text_messages_dict,
    generate_initial_text_messages_dict,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.utils.string import StrictStringFormatter

_DATASET_1 = "dataset_1"
_TABLE_1 = "table_1"


class TestSendJIITexts(BigQueryEmulatorTestCase):
    """Implements tests for helpers.py."""

    def setUp(self) -> None:
        super().setUp()
        self.test_table_address = BigQueryAddress(
            dataset_id=_DATASET_1,
            table_id=_TABLE_1,
        )
        self.create_mock_table(
            address=self.test_table_address,
            schema=[
                bigquery.SchemaField(
                    "state_code",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "external_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "person_id",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "person_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "phone_number",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "officer_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "po_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "district",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "launch_id",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "group_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            self.test_table_address,
            data=[
                {
                    "state_code": "US_IX",
                    "external_id": "0A",
                    "person_id": "0",
                    "person_name": '{"given_names": "TED", "middle_names": "", "name_suffix": "", "surname": "LASSO"}',
                    "phone_number": "1234567890",
                    "officer_id": "1A",
                    "po_name": "TEST PO 1",
                    "district": "District 2",
                    "launch_id": "1",
                    "group_id": "MISSING_INCOME_VERIFICATION",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "1B",
                    "person_id": "1",
                    "person_name": '{"given_names": "ROY", "middle_names": "", "name_suffix": "", "surname": "KENT"}',
                    "phone_number": "1111111111",
                    "officer_id": "1A",
                    "po_name": "TEST PO 1",
                    "district": "District 4",
                    "launch_id": "1",
                    "group_id": "MISSING_DA",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "2C",
                    "person_id": "2",
                    "person_name": '{"given_names": "KEELEY", "middle_names": "", "name_suffix": "", "surname": "JONES"}',
                    "phone_number": "2222222222",
                    "officer_id": "2B",
                    "po_name": "TEST PO 2",
                    "district": "District 6",
                    "launch_id": "1",
                    "group_id": "FULLY_ELIGIBLE",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "3D",
                    "person_id": "3",
                    "person_name": '{"given_names": "COACH", "middle_names": "", "name_suffix": "", "surname": "BEARD"}',
                    "phone_number": "3333333333",
                    "officer_id": "2B",
                    "po_name": "TEST PO 2",
                    "district": "District 3",
                    "launch_id": "1",
                    "group_id": "TWO_MISSING_CRITERIA",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "4E",
                    "person_id": "4",
                    "person_name": '{"given_names": "REBECCA", "middle_names": "", "name_suffix": "", "surname": "WELTON"}',
                    "phone_number": "4444444444",
                    "officer_id": "1A",
                    "po_name": "TEST PO 1",
                    "district": "District 1",
                    "launch_id": "1",
                    "group_id": "ELIGIBLE_MISSING_FINES_AND_FEES",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "5F",
                    "person_id": "5",
                    "person_name": '{"given_names": "JAMIE", "middle_names": "", "name_suffix": "", "surname": "TARTT"}',
                    "phone_number": "5555555555",
                    "officer_id": "1A",
                    "po_name": "TEST PO 1",
                    "district": "PCO",
                    "launch_id": "1",
                    "group_id": "FULLY_ELIGIBLE",
                },
            ],
        )

    def tearDown(self) -> None:
        super().tearDown()

    def test_generate_initial_text_messages_dict(self) -> None:
        query_job = BigQueryClientImpl.run_query_async(
            self.bq_client,
            query_str=f"SELECT * FROM {self.test_table_address.to_str()}",
            use_query_cache=False,
        )
        initial_text_dicts = generate_initial_text_messages_dict(bq_output=query_job)

        self.assertEqual(len(initial_text_dicts), 5)
        self.assertEqual(
            initial_text_dicts[0],
            {
                "external_id": "0A",
                "phone_num": "1234567890",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Ted", po_name="Test Po 1"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 2",
                "group_id": "MISSING_INCOME_VERIFICATION",
            },
        )
        self.assertEqual(
            initial_text_dicts[1],
            {
                "external_id": "1B",
                "phone_num": "1111111111",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Roy", po_name="Test Po 1"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 4",
                "group_id": "MISSING_DA",
            },
        )
        self.assertEqual(
            initial_text_dicts[2],
            {
                "external_id": "2C",
                "phone_num": "2222222222",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Keeley", po_name="Test Po 2"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 2",
                "district": "district 6",
                "group_id": "FULLY_ELIGIBLE",
            },
        )
        self.assertEqual(
            initial_text_dicts[3],
            {
                "external_id": "3D",
                "phone_num": "3333333333",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Coach", po_name="Test Po 2"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 2",
                "district": "district 3",
                "group_id": "TWO_MISSING_CRITERIA",
            },
        )
        self.assertEqual(
            initial_text_dicts[4],
            {
                "external_id": "4E",
                "phone_num": "4444444444",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Rebecca", po_name="Test Po 1"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 1",
                "group_id": "ELIGIBLE_MISSING_FINES_AND_FEES",
            },
        )

    def test_generate_eligibility_text_messages_dict(self) -> None:
        query_job = BigQueryClientImpl.run_query_async(
            self.bq_client,
            query_str=f"SELECT * FROM {self.test_table_address.to_str()}",
            use_query_cache=False,
        )
        eligibility_text_dicts = generate_eligibility_text_messages_dict(
            bq_output=query_job
        )
        self.assertEqual(len(eligibility_text_dicts), 5)

        self.assertEqual(
            eligibility_text_dicts[0],
            {
                "external_id": "0A",
                "phone_num": "1234567890",
                "text_body": StrictStringFormatter().format(
                    MISSING_INCOME,
                    given_name="Ted",
                    po_name="Test Po 1",
                    additional_contact=" or contact a specialist at district2Admin@idoc.idaho.gov",
                )
                + VISIT
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 2",
            },
        )
        self.assertEqual(
            eligibility_text_dicts[1],
            {
                "external_id": "1B",
                "phone_num": "1111111111",
                "text_body": StrictStringFormatter().format(
                    MISSING_NEGATIVE_DA,
                    given_name="Roy",
                    po_name="Test Po 1",
                    additional_contact=" or a specialist at d4ppspecialists@idoc.idaho.gov or 208-327-7008",
                )
                + VISIT
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 4",
            },
        )
        self.assertEqual(
            eligibility_text_dicts[2],
            {
                "external_id": "2C",
                "phone_num": "2222222222",
                "text_body": StrictStringFormatter().format(
                    FULLY_ELIGIBLE_TEXT,
                    given_name="Keeley",
                    po_name="Test Po 2",
                    additional_contact="",
                )
                + LEARN_MORE
                + ALL_CLOSER,
                "po_name": "Test Po 2",
                "district": "district 6",
            },
        )
        self.assertEqual(
            eligibility_text_dicts[3],
            {
                "external_id": "3D",
                "phone_num": "3333333333",
                "text_body": StrictStringFormatter().format(
                    MISSING_NEGATIVE_DA_AND_INCOME,
                    given_name="Coach",
                    po_name="Test Po 2",
                    additional_contact=" or a specialist at specialistsd3@idoc.idaho.gov or (208) 454-7601",
                )
                + VISIT
                + ALL_CLOSER,
                "po_name": "Test Po 2",
                "district": "district 3",
            },
        )
        self.assertEqual(
            eligibility_text_dicts[4],
            {
                "external_id": "4E",
                "phone_num": "4444444444",
                "text_body": StrictStringFormatter().format(
                    FULLY_ELIGIBLE_EXCEPT_FFR,
                    given_name="Rebecca",
                    po_name="Test Po 1",
                    additional_contact=" or email D1Connect@idoc.idaho.gov",
                )
                + LEARN_MORE
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 1",
            },
        )
