# Recidiviz - a data platform for criminal justice reform
# Copyright (C) August 2025 Recidiviz, Inc.
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
"""Tests for batch_generation_helpers.py"""
import unittest
from textwrap import dedent

from recidiviz.research.utils.batch_generation_helpers import (
    random_clients_cte,
    select_notes,
)


class TestBatchGenerationHelpers(unittest.TestCase):
    """Tests for BatchGenerationHelpers SQL decomp subclasses
    Dedent and strip are used as indentation and white space don't
    actually matter for SQL queries"""

    def test_select_notes_basic(self) -> None:
        query = select_notes(
            client_source_sql='f"client_external_id IN ({client_external_ids_str})"',
            per_client_limit=10,
            table="`recidiviz-staging.bpacker_scratch.case_notes_with_info`",
        )
        expected_output = """
        SELECT *
        FROM `recidiviz-staging.bpacker_scratch.case_notes_with_info`
        WHERE f"client_external_id IN ({client_external_ids_str})"

        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY client_external_id ORDER BY note_timestamp DESC
        ) <= 10
        ORDER BY client_external_id, note_timestamp
        """
        self.assertEqual(
            dedent(expected_output).strip(),
            dedent(query).strip(),
        )

    def test_select_notes_with_extra_filter(self) -> None:
        query = select_notes(
            client_source_sql='f"client_external_id IN ({client_external_ids_str})"',
            per_client_limit=10,
            table="`recidiviz-staging.bpacker_scratch.case_notes_with_info`",
            extra_filter="AND extra filter",
        )
        expected_output = """
        SELECT *
        FROM `recidiviz-staging.bpacker_scratch.case_notes_with_info`
        WHERE f"client_external_id IN ({client_external_ids_str})"
        AND extra filter
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY client_external_id ORDER BY note_timestamp DESC
        ) <= 10
        ORDER BY client_external_id, note_timestamp
        """
        self.assertEqual(
            dedent(expected_output).strip(),
            dedent(query).strip(),
        )

    def test_random_clients_cte(self) -> None:
        query = random_clients_cte(limit_num=100, exclude_ids=["empty"])
        expected_output = """
                WITH RandomClients AS (
                    SELECT DISTINCT client_external_id
                    FROM `recidiviz-staging.bpacker_scratch.case_notes_with_info`
                    WHERE 'SUPERVISION' IN UNNEST(compartment_level_1)
                    AND compartment_level_1[OFFSET(0)] IN ("SUPERVISION", "PENDING_SUPERVISION")
                                
                    ORDER BY RAND()
                    LIMIT 100
                )"""
        self.assertEqual(
            dedent(expected_output).strip(),
            dedent(query).strip(),
        )

    def test_random_clients_cte_with_excluded_ids(self) -> None:
        query = random_clients_cte(limit_num=10, exclude_ids=["31", "45"])
        expected_output = """
                WITH RandomClients AS (
                    SELECT DISTINCT client_external_id
                    FROM `recidiviz-staging.bpacker_scratch.case_notes_with_info`
                    WHERE 'SUPERVISION' IN UNNEST(compartment_level_1)
                    AND compartment_level_1[OFFSET(0)] IN ("SUPERVISION", "PENDING_SUPERVISION")
                    AND case_note_id NOT IN ('31','45')
                    ORDER BY RAND()
                    LIMIT 10
                )"""
        self.assertEqual(
            dedent(expected_output).strip(),
            dedent(query).strip(),
        )
