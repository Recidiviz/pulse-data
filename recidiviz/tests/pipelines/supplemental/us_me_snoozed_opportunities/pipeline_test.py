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
"""Tests for UsMeSnoozedOpportunitiesPipeline."""
import unittest
from typing import Any, Dict, Iterable, Optional, Set
from unittest.mock import patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.pipelines.supplemental.us_me_snoozed_opportunities import pipeline
from recidiviz.pipelines.supplemental.us_me_snoozed_opportunities.pipeline import (
    ME_OPPORTUNITY_TYPES,
)
from recidiviz.pipelines.supplemental.us_me_snoozed_opportunities.us_me_snoozed_opportunity_notes_query_provider import (
    US_ME_SNOOZED_OPPORTUNITY_NOTES_QUERY_NAME,
)
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadFromBigQueryFactory,
    FakeWriteExactOutputToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    run_test_pipeline,
)


class TestUsMeSnoozedOpportunitiesPipeline(unittest.TestCase):
    """Tests for UsMeSnoozedOpportunitiesPipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteExactOutputToBigQuery
        )

        self.basic_input_columns: Dict[str, Any] = {
            "state_code": StateCode.US_ME.value,
            "person_id": "111",
            "Cis_100_Client_Id": "123123",
            "Note_Id": "111",
            "Note_Date": "2025-01-05",
        }

        self.basic_output_columns: Dict[str, Any] = {
            "state_code": StateCode.US_ME.value,
            "person_id": "111",
            "person_external_id": "123123",
            "Note_Id": "111",
            "Note_Date": "2025-01-05",
        }

        self.valid_snooze_note: str = """{
                    "is_recidiviz_snooze_note": true,
                    "person_external_id": "123123",
                    "opportunity_type": "usMeSCCP",
                    "officer_email": "example@fake.com",
                    "start_date": "2025-01-05",
                    "end_date": "2025-03-01",
                    "denial_reasons": ["example_1", "example_2"],
                    "other_text": "This is a test."
                }"""

        self.invalid_json_snooze_note: str = """extra text{
                    "is_recidiviz_snooze_note": true,
                    "person_external_id": "123123",
                    "opportunity_type": "usMeSCCP",
                    "officer_email": "example@fake.com",
                    "start_date": "2025-01-05",
                    "end_date": "2025-03-01",
                    "denial_reasons": ["example_1", "example_2"],
                    "other_text": "This is a test."
                }"""

        self.invalid_snooze_note: str = """{
                    "is_recidiviz_snooze_note": true,
                    "person_external_id": "123123",
                    "opportunity_type": "fakeOpportunity",
                    "officer_email": "example@fake.com",
                    "start_date": "2025-01-05",
                    "end_date": "2025-03-01",
                    "denial_reasons": ["example_1", "example_2"],
                    "other_text": "This is a test."
                }"""

        self.invalid_snooze_note_with_bad_end_date: str = """{
                    "is_recidiviz_snooze_note": true,
                    "person_external_id": "123123",
                    "opportunity_type": "fakeOpportunity",
                    "officer_email": "example@fake.com",
                    "start_date": "2025-01-05",
                    "end_date": false,
                    "denial_reasons": ["example_1", "example_2"],
                    "other_text": "This is a test."
                }"""

        self.initial_data: Iterable[dict[str, Any]] = [
            {**self.basic_input_columns, "Note_Tx": self.valid_snooze_note}
        ]

        self.final_data: Iterable[Dict[str, Any]] = [
            {
                **self.basic_output_columns,
                "is_valid_snooze_note": True,
                "note": self.valid_snooze_note,
                "error": None,
            }
        ]

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def run_test_pipeline(
        self,
        data_dict: Dict[str, Iterable[Dict]],
        root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
    ) -> None:
        """Runs a test version of the pipeline."""
        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                expected_entities_dataset=normalized_state_dataset_for_state_code(
                    StateCode.US_XX
                ),
                data_dict=data_dict,
                state_code=StateCode.US_ME,
            )
        )

        write_to_bq_constructor = (
            self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
                expected_dataset=BigQueryAddressOverrides.format_sandbox_dataset(
                    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
                    SUPPLEMENTAL_DATA_DATASET,
                ),
                expected_output=self.final_data,
            )
        )

        run_test_pipeline(
            pipeline_cls=pipeline.UsMeSnoozedOpportunitiesPipeline,
            state_code=StateCode.US_ME.value,
            project_id=self.project_id,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            root_entity_id_filter_set=root_entity_id_filter_set,
        )

    def testUsMeSnoozedOpportunitiesPipeline(self) -> None:
        data_dict = {US_ME_SNOOZED_OPPORTUNITY_NOTES_QUERY_NAME: self.initial_data}
        self.maxDiff = None
        self.run_test_pipeline(data_dict=data_dict)

    def test_invalid_json_is_marked_as_malformatted(self) -> None:
        expected_output_row = {
            **self.basic_output_columns,
            "is_valid_snooze_note": False,
            "note": self.invalid_json_snooze_note,
            "error": "JSONDecodeError: Expecting value: line 1 column 1 (char 0)",
        }
        self.maxDiff = None
        self.assertEqual(
            pipeline.UsMeSnoozedOpportunitiesPipeline.extract_snooze_event_from_case_note_row(
                {**self.basic_input_columns, "Note_Tx": self.invalid_json_snooze_note}
            ),
            expected_output_row,
        )

    def test_invalid_snooze_note_is_marked_as_malformatted(self) -> None:
        expected_output_row = {
            **self.basic_output_columns,
            "is_valid_snooze_note": False,
            "note": self.invalid_snooze_note,
            "error": "ValueError: Unexpected opportunity_type: [fakeOpportunity]",
        }
        self.maxDiff = None
        self.assertEqual(
            pipeline.UsMeSnoozedOpportunitiesPipeline.extract_snooze_event_from_case_note_row(
                {**self.basic_input_columns, "Note_Tx": self.invalid_snooze_note}
            ),
            expected_output_row,
        )

    def test_invalid_snooze_with_bad_iso_date_errors(self) -> None:
        expected_output_row = {
            **self.basic_output_columns,
            "is_valid_snooze_note": False,
            "note": self.invalid_snooze_note_with_bad_end_date,
            "error": "TypeError: Field [end_date] must be a string in ISO-format. Found [False], which is [<class 'bool'>].",
        }
        self.maxDiff = None
        self.assertEqual(
            pipeline.UsMeSnoozedOpportunitiesPipeline.extract_snooze_event_from_case_note_row(
                {
                    **self.basic_input_columns,
                    "Note_Tx": self.invalid_snooze_note_with_bad_end_date,
                }
            ),
            expected_output_row,
        )

    def test_opportunities_list_matches_workflows_configs(self) -> None:
        our_opportunities = set(ME_OPPORTUNITY_TYPES)

        maine_opportunities = set(
            cfg.opportunity_type
            for cfg in WORKFLOWS_OPPORTUNITY_CONFIGS
            if cfg.state_code == StateCode.US_ME
        )

        self.assertEqual(
            our_opportunities,
            maine_opportunities,
        )
