# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Unit tests for SupplementalPipelineParameters"""
import json
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.pipeline import (
    UsIxCaseNoteExtractedEntitiesPipeline,
)
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_all_reference_query_input_datasets_for_pipeline,
)


class TestSupplementalPipelineParameters(unittest.TestCase):
    """Unit tests for SupplementalPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = SupplementalPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="us_oz_case_note_extracted_entities_supplemental",
            region="us-west1",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "us_oz_case_note_extracted_entities_supplemental",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(
            pipeline_parameters.job_name, "us-oz-case-note-extracted-entities"
        )

        self.assertEqual(SUPPLEMENTAL_DATA_DATASET, pipeline_parameters.output)
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)

    def test_parameters_with_sandbox_prefix(self) -> None:
        pipeline_parameters = SupplementalPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="us_oz_case_note_extracted_entities_supplemental",
            region="us-west1",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "us_oz_case_note_extracted_entities_supplemental",
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(
            pipeline_parameters.job_name,
            "my-prefix-us-oz-case-note-extracted-entities-test",
        )
        self.assertEqual("my_prefix_supplemental_data", pipeline_parameters.output)
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    def test_check_for_valid_input_dataset_overrides(self) -> None:
        input_overrides_json = json.dumps(
            # This pipeline is for IX so doesn't read from us_yy_raw_data
            {"us_yy_raw_data": "some_other_raw_data_table"}
        )
        pipeline_parameters = SupplementalPipelineParameters(
            project="recidiviz-456",
            state_code="US_IX",
            pipeline="test_pipeline_name",
            region="us-west1",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            input_dataset_overrides_json=input_overrides_json,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found original dataset \[us_yy_raw_data\] in overrides which is not a "
            r"dataset this pipeline reads from. Datasets you can override: "
            r"\['us_ix_normalized_state', 'us_ix_raw_data_up_to_date_views'\].",
        ):
            pipeline_parameters.check_for_valid_input_dataset_overrides(
                get_all_reference_query_input_datasets_for_pipeline(
                    UsIxCaseNoteExtractedEntitiesPipeline,
                    StateCode(pipeline_parameters.state_code),
                )
            )

        input_overrides_json = json.dumps(
            # This override is valid
            {"us_ix_raw_data_up_to_date_views": "some_other_raw_data_table"}
        )
        pipeline_parameters = SupplementalPipelineParameters(
            project="recidiviz-456",
            state_code="US_IX",
            pipeline="test_pipeline_name",
            region="us-west1",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            input_dataset_overrides_json=input_overrides_json,
        )
        pipeline_parameters.check_for_valid_input_dataset_overrides(
            get_all_reference_query_input_datasets_for_pipeline(
                UsIxCaseNoteExtractedEntitiesPipeline,
                StateCode(pipeline_parameters.state_code),
            )
        )
