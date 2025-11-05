# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""unit tests for query fragments for US_PA"""
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    StatueMatchingMode,
    statute_code_is_like,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestPAQueryFragments(BigQueryEmulatorTestCase):
    """unit tests for query fragments for US_PA"""

    def test_statute_like(self) -> None:
        query = f"""
            WITH parsed_statutes AS (
            SELECT
                '18.2702.A1' AS id,
                '18' AS title,
                '2702' AS section,
                'A1' AS subsection
            UNION ALL
                SELECT
                '18.2702.A' AS id,
                '18' AS title,
                '2702' AS section,
                'A' AS subsection
            UNION ALL
                SELECT
                '18.2702.A11' AS id,
                '18' AS title,
                '2702' AS section,
                'A11' AS subsection
            UNION ALL
                SELECT
                '18.2702.A2' AS id,
                '18' AS title,
                '2702' AS section,
                'A2' AS subsection
            UNION ALL
            SELECT
                '18.2702' AS id,
                '18' AS title,
                '2702' AS section,
                '' AS subsection 
            UNION ALL
            SELECT
                '19.2702' AS id,
                '19' AS title,
                '2702' AS section,
                '' AS subsection 
            UNION ALL
            SELECT
                '19.2702.A1' AS id,
                '19' AS title,
                '2702' AS section,
                'A1' AS subsection 
            )
        SELECT
            id as test_case_name,
            CASE 
                WHEN {statute_code_is_like('18', '2702', 'A1', StatueMatchingMode.MATCH_ON_TITLE_SECTION_SUBSECTION_STRICT)} 
                THEN "MATCH"
                ELSE "NO MATCH"
            END AS include_subsections_strict,
            CASE 
                WHEN {statute_code_is_like('18', '2702', 'A1', StatueMatchingMode.MATCH_ON_TITLE_SECTION_EXCLUDE_ON_SUBSECTION)} 
                THEN "MATCH"
                ELSE "NO MATCH"
            END AS exclude_subsections
        FROM parsed_statutes
        """

        print(query)

        expected_results = [
            {
                "test_case_name": "18.2702.A1",
                "include_subsections_strict": "MATCH",
                "exclude_subsections": "NO MATCH",
            },
            {
                "test_case_name": "18.2702.A",
                "include_subsections_strict": "NO MATCH",
                "exclude_subsections": "MATCH",
            },
            {
                "test_case_name": "18.2702.A11",
                "include_subsections_strict": "MATCH",
                "exclude_subsections": "NO MATCH",
            },
            {
                "test_case_name": "18.2702.A2",
                "include_subsections_strict": "NO MATCH",
                "exclude_subsections": "NO MATCH",
            },
            {
                "test_case_name": "18.2702",
                "include_subsections_strict": "NO MATCH",
                "exclude_subsections": "MATCH",
            },
            {
                "test_case_name": "19.2702",
                "include_subsections_strict": "NO MATCH",
                "exclude_subsections": "NO MATCH",
            },
            {
                "test_case_name": "19.2702.A1",
                "include_subsections_strict": "NO MATCH",
                "exclude_subsections": "NO MATCH",
            },
        ]

        self.run_query_test(query, expected_result=expected_results)
