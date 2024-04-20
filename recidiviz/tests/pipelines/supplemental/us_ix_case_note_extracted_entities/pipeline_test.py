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
"""Tests the us_ix case notes entity extraction supplemental dataset pipeline."""
import unittest
from typing import Any, Dict, Iterable, Optional, Set
from unittest.mock import patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.calculator.query.state.views.reference.us_ix_case_update_info import (
    US_IX_CASE_UPDATE_INFO_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities import pipeline
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_title_text_analysis_configuration import (
    UsIxNoteTitleTextEntity,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadFromBigQueryFactory,
    FakeWriteExactOutputToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    run_test_pipeline,
)


# TODO(#16661) Rename US_IX -> US_ID in this file/code when we are ready to migrate the
# new ATLAS pipeline to run for US_ID
class TestUsIxCaseNoteExtractedEntitiesPipeline(unittest.TestCase):
    """Tests the us_ix case notes entity extraction supplemental dataset pipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteExactOutputToBigQuery
        )

        self.test_person_id = 123
        self.test_person_external_id = "111"
        self.test_note_id = "1"
        self.test_note_date = "2022-01-01 00:00:00"
        self.test_staff_id = "agent"

        self.initial_fields: Dict[str, Any] = {
            "person_id": self.test_person_id,
            "person_external_id": self.test_person_external_id,
            "state_code": "US_IX",
            "OffenderNoteId": self.test_note_id,
            "NoteDate": self.test_note_date,
            "StaffId": self.test_staff_id,
        }

        self.revocation = {
            **self.initial_fields,
            "Details": "{note_title: RX} {note: note}",
        }
        self.treatment_completion = {
            **self.initial_fields,
            "Details": "{note_title: TREATMENT COMPLETE} {note: note}",
        }
        self.not_a_revocation = {
            **self.initial_fields,
            "Details": "{note_title: REVOKE INTERNET} {note: note}",
        }
        self.not_treatment_completion = {
            **self.initial_fields,
            "Details": "{note_title: EVAL COMPLETE} {note: note}",
        }
        self.sanction = {
            **self.initial_fields,
            "Details": "{note_title: SANCTION} {note: note}",
        }

        self.initial_data: Iterable = [
            self.revocation,
            self.not_a_revocation,
            self.treatment_completion,
            self.not_treatment_completion,
            self.sanction,
        ]

        self.default_mappings = {
            entity.name.lower(): False
            for entity in UsIxNoteTitleTextEntity
            if entity != UsIxNoteTitleTextEntity.REVOCATION_INCLUDE
        }

        self.final_data: Iterable[Dict[str, Any]] = [
            {**self.revocation, **self.default_mappings, "revocation": True},
            {**self.not_a_revocation, **self.default_mappings},
            {
                **self.treatment_completion,
                **self.default_mappings,
                "any_treatment": True,
                "treatment_complete": True,
            },
            {**self.not_treatment_completion, **self.default_mappings},
            {**self.sanction, **self.default_mappings, "sanction": True},
        ]

        for final_data_point in self.final_data:
            final_data_point["NoteDate"] = "2022-01-01"

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def run_test_pipeline(
        self,
        data_dict: Dict[str, Iterable[Dict]],
        unifying_id_field_filter_set: Optional[Set[int]] = None,
    ) -> None:
        """Runs a test version of the pipeline."""
        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                expected_entities_dataset=NORMALIZED_STATE_DATASET,
                data_dict=data_dict,
                state_code=StateCode.US_IX,
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
            pipeline_cls=pipeline.UsIxCaseNoteExtractedEntitiesPipeline,
            state_code="US_IX",
            project_id=self.project_id,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            unifying_id_field_filter_set=unifying_id_field_filter_set,
        )

    def testUsIxCaseNoteExtractedEntities(self) -> None:
        data_dict = {US_IX_CASE_UPDATE_INFO_VIEW_NAME: self.initial_data}
        self.maxDiff = None
        self.run_test_pipeline(data_dict=data_dict)
