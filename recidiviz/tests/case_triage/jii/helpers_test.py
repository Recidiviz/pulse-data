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
    FULLY_ELIGIBLE_TEXT,
    INITIAL_TEXT,
    FULLY_ELIGIBLE_EXCEPT_FFR,
    MISSING_NEGATIVE_DA_OR_INCOME,
    MISSING_NEGATIVE_DA_DOCUMENTATION,
    MISSING_INCOME_DOCUMENTATION,
    MISSING_NEGATIVE_DA_AND_INCOME,
    LEARN_MORE,
    generate_eligibility_text_messages_dict,
    generate_initial_text_messages_dict,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.utils.string import StrictStringFormatter

_DATASET_1 = "dataset_1"
_TABLE_1 = "table_1"


class TestSendIDLSUTexts(BigQueryEmulatorTestCase):
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
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "person_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "supervision_type",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "supervision_level",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "po_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "phone_number",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "email_address",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "address",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "supervision_start_date",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "expiration_date",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "is_eligible",
                    field_type=bigquery.enums.SqlTypeNames.BOOLEAN.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "array_reasons",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REPEATED",
                ),
                bigquery.SchemaField(
                    "status",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "denied_reasons",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REPEATED",
                ),
                bigquery.SchemaField(
                    "lsir_level",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "ineligible_criteria_list",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "ineligible_criteria",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REPEATED",
                ),
                bigquery.SchemaField(
                    "district",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "fines_n_fees_denials",
                    field_type=bigquery.enums.SqlTypeNames.BOOLEAN.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            self.test_table_address,
            data=[
                {
                    "state_code": "US_IX",
                    "external_id": "0",
                    "person_name": '{"given_names": "TED", "middle_names": "", "name_suffix": "", "surname": "LASSO"}',
                    "supervision_type": "PAROLE",
                    "supervision_level": "MINIMUM",
                    "po_name": "TEST PO 1",
                    "phone_number": "1234567890",
                    "email_address": None,
                    "address": None,
                    "supervision_start_date": "2023-12-19",
                    "expiration_date": "2030-03-28",
                    "is_eligible": "false",
                    "array_reasons": "[]",
                    "status": None,
                    "denied_reasons": "[]",
                    "lsir_level": "LOW",
                    "ineligible_criteria_list": "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS",
                    "ineligible_criteria": ["US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS"],
                    "district": "District 2",
                    "fines_n_fees_denials": "false",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "1",
                    "person_name": '{"given_names": "ROY", "middle_names": "", "name_suffix": "", "surname": "KENT"}',
                    "supervision_type": "PAROLE",
                    "supervision_level": "MINIMUM",
                    "po_name": "TEST PO 1",
                    "phone_number": "1111111111",
                    "email_address": None,
                    "address": None,
                    "supervision_start_date": "2019-09-10",
                    "expiration_date": "2029-03-09",
                    "is_eligible": "false",
                    "array_reasons": "[]",
                    "status": None,
                    "denied_reasons": "[]",
                    "lsir_level": "LOW",
                    "ineligible_criteria_list": "NEGATIVE_DA_WITHIN_90_DAYS",
                    "ineligible_criteria": ["NEGATIVE_DA_WITHIN_90_DAYS"],
                    "district": "District 4",
                    "fines_n_fees_denials": "false",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "2",
                    "person_name": '{"given_names": "KEELEY", "middle_names": "", "name_suffix": "", "surname": "JONES"}',
                    "supervision_type": None,
                    "supervision_level": None,
                    "po_name": "TEST PO 2",
                    "phone_number": "2222222222",
                    "email_address": None,
                    "address": None,
                    "supervision_start_date": None,
                    "expiration_date": None,
                    "is_eligible": "true",
                    "array_reasons": None,
                    "status": None,
                    "denied_reasons": None,
                    "lsir_level": None,
                    "ineligible_criteria_list": None,
                    "ineligible_criteria": [],
                    "district": "District 6",
                    "fines_n_fees_denials": "false",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "3",
                    "person_name": '{"given_names": "COACH", "middle_names": "", "name_suffix": "", "surname": "BEARD"}',
                    "supervision_type": "PAROLE",
                    "supervision_level": "MEDIUM",
                    "po_name": "TEST PO 2",
                    "phone_number": "3333333333",
                    "email_address": None,
                    "address": None,
                    "supervision_start_date": "2020-11-27",
                    "expiration_date": "2027-06-27",
                    "is_eligible": "false",
                    "array_reasons": "[]",
                    "status": None,
                    "denied_reasons": "[]",
                    "lsir_level": "MODERATE",
                    "ineligible_criteria_list": "NEGATIVE_DA_WITHIN_90_DAYS, US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS",
                    "ineligible_criteria": [
                        "NEGATIVE_DA_WITHIN_90_DAYS",
                        "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS",
                    ],
                    "district": "District 3",
                    "fines_n_fees_denials": "false",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "4",
                    "person_name": '{"given_names": "REBECCA", "middle_names": "", "name_suffix": "", "surname": "WELTON"}',
                    "supervision_type": "PAROLE",
                    "supervision_level": "MINIMUM",
                    "po_name": "TEST PO 1",
                    "phone_number": "4444444444",
                    "email_address": None,
                    "address": None,
                    "supervision_start_date": "2023-12-19",
                    "expiration_date": "2030-03-28",
                    "is_eligible": "true",
                    "array_reasons": "[]",
                    "status": None,
                    "denied_reasons": "[]",
                    "lsir_level": "LOW",
                    "ineligible_criteria_list": None,
                    "ineligible_criteria": [],
                    "district": "District 1",
                    "fines_n_fees_denials": "true",
                },
                {
                    "state_code": "US_IX",
                    "external_id": "5",
                    "person_name": '{"given_names": "JAMIE", "middle_names": "", "name_suffix": "", "surname": "TARTT"}',
                    "supervision_type": "PAROLE",
                    "supervision_level": "MINIMUM",
                    "po_name": "TEST PO 1",
                    "phone_number": "5555555555",
                    "email_address": None,
                    "address": None,
                    "supervision_start_date": "2023-12-19",
                    "expiration_date": "2030-03-28",
                    "is_eligible": "false",
                    "array_reasons": "[]",
                    "status": None,
                    "denied_reasons": "[]",
                    "lsir_level": "LOW",
                    "ineligible_criteria_list": None,
                    "ineligible_criteria": [],
                    "district": "District 7",
                    "fines_n_fees_denials": "true",
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
                "external_id": "0",
                "phone_num": "1234567890",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Ted", po_name="Test Po 1"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 2",
            },
        )
        self.assertEqual(
            initial_text_dicts[1],
            {
                "external_id": "1",
                "phone_num": "1111111111",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Roy", po_name="Test Po 1"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 4",
            },
        )
        self.assertEqual(
            initial_text_dicts[2],
            {
                "external_id": "2",
                "phone_num": "2222222222",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Keeley", po_name="Test Po 2"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 2",
                "district": "district 6",
            },
        )
        self.assertEqual(
            initial_text_dicts[3],
            {
                "external_id": "3",
                "phone_num": "3333333333",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Coach", po_name="Test Po 2"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 2",
                "district": "district 3",
            },
        )
        self.assertEqual(
            initial_text_dicts[4],
            {
                "external_id": "4",
                "phone_num": "4444444444",
                "text_body": StrictStringFormatter().format(
                    INITIAL_TEXT, given_name="Rebecca", po_name="Test Po 1"
                )
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 1",
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
                "external_id": "0",
                "phone_num": "1234567890",
                "text_body": StrictStringFormatter().format(
                    MISSING_NEGATIVE_DA_OR_INCOME,
                    given_name="Ted",
                    missing_documentation=MISSING_INCOME_DOCUMENTATION,
                    po_name="Test Po 1",
                    additional_contact=" or contact a specialist at district2Admin@idoc.idaho.gov",
                )
                + LEARN_MORE
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 2",
            },
        )
        self.assertEqual(
            eligibility_text_dicts[1],
            {
                "external_id": "1",
                "phone_num": "1111111111",
                "text_body": StrictStringFormatter().format(
                    MISSING_NEGATIVE_DA_OR_INCOME,
                    given_name="Roy",
                    missing_documentation=MISSING_NEGATIVE_DA_DOCUMENTATION,
                    po_name="Test Po 1",
                    additional_contact=" or a specialist at d4ppspecialists@idoc.idaho.gov or 208-327-7008",
                )
                + LEARN_MORE
                + ALL_CLOSER,
                "po_name": "Test Po 1",
                "district": "district 4",
            },
        )
        self.assertEqual(
            eligibility_text_dicts[2],
            {
                "external_id": "2",
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
                "external_id": "3",
                "phone_num": "3333333333",
                "text_body": StrictStringFormatter().format(
                    MISSING_NEGATIVE_DA_AND_INCOME,
                    given_name="Coach",
                    po_name="Test Po 2",
                    additional_contact=" or a specialist at specialistsd3@idoc.idaho.gov or (208) 454-7601",
                )
                + LEARN_MORE
                + ALL_CLOSER,
                "po_name": "Test Po 2",
                "district": "district 3",
            },
        )
        self.assertEqual(
            eligibility_text_dicts[4],
            {
                "external_id": "4",
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
