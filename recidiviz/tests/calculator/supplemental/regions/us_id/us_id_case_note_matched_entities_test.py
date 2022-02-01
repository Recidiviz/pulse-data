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
"""Unit tests for us_id_case_note_matched_entities.py"""
# pylint: disable=protected-access
import datetime
import unittest
from typing import Any, Dict, List

from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import transform_dict_to_bigquery_row
from recidiviz.calculator.supplemental.regions.us_id.us_id_case_note_matched_entities import (
    UsIdCaseNoteMatchedEntities,
)
from recidiviz.calculator.supplemental.regions.us_id.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)


class TestUsIdCaseNoteMatchedEntities(unittest.TestCase):
    """Unit tests for UsIdCaseNoteMatchedEntities"""

    def setUp(self) -> None:
        self.supplemental_dataset_table = UsIdCaseNoteMatchedEntities()

        self.test_person_id = 123
        self.test_person_external_id = "111"
        self.test_agnt_case_updt_id = "1"
        self.test_create_dt = datetime.date(2022, 1, 1)
        self.test_create_by_usr_id = "agent"

        self.initial_fields: Dict[str, Any] = {
            "person_id": self.test_person_id,
            "person_external_id": self.test_person_external_id,
            "agnt_case_updt_id": self.test_agnt_case_updt_id,
            "create_dt": self.test_create_dt,
            "create_by_usr_id": self.test_create_by_usr_id,
        }

        self.revocation = {**self.initial_fields, "agnt_note_title": "RX"}
        self.treatment_completion = {
            **self.initial_fields,
            "agnt_note_title": "TREATMENT COMPLETE",
        }
        self.not_a_revocation = {
            **self.initial_fields,
            "agnt_note_title": "REVOKE INTERNET",
        }
        self.not_treatment_completion = {
            **self.initial_fields,
            "agnt_note_title": "EVAL COMPLETE",
        }
        self.sanction = {**self.initial_fields, "agnt_note_title": "SANCTION"}

        self.initial_data = [
            self.revocation,
            self.not_a_revocation,
            self.treatment_completion,
            self.not_treatment_completion,
            self.sanction,
        ]

        self.initial_rows: List[bigquery.table.Row] = [
            transform_dict_to_bigquery_row(data_point)
            for data_point in self.initial_data
        ]

        self.default_mappings = {
            entity.name.lower(): False
            for entity in UsIdTextEntity
            if entity != UsIdTextEntity.REVOCATION_INCLUDE
        }

        self.final_data: List[Dict[str, Any]] = [
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
            final_data_point["create_dt"] = "2022-01-01 00:00:00"

    def test_process_row(self) -> None:
        result = [
            self.supplemental_dataset_table.process_row(row)
            for row in self.initial_rows
        ]
        self.assertListEqual(result, self.final_data)

    def test_query_from_dependent_views(self) -> None:
        self.assertEqual(
            "SELECT * FROM `test-project.reference_views.us_id_case_update_info_materialized`",
            self.supplemental_dataset_table.query_from_dependent_views(
                project_id="test-project"
            ),
        )
