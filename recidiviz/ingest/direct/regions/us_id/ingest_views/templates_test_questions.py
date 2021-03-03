# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Helper templates for the US_ID queries related to test questions."""


def question_numbers_with_descriptive_answers_view_fragment(test_id: str) -> str:
    return """
        qstn_nums_with_descriptive_answers AS (
          SELECT
            ofndr_num,
            body_loc_cd,
            ofndr_tst_id, 
            assess_tst_id,
            tst_dt,
            score_by_name,
            CONCAT(assess_qstn_num, '-', tst_sctn_num) AS qstn_num, 
            STRING_AGG(qstn_choice_desc ORDER BY qstn_choice_desc) AS qstn_answer
          FROM 
            {{ofndr_tst}}
          LEFT JOIN 
            {{tst_qstn_rspns}}
          USING 
            (ofndr_tst_id, assess_tst_id)
          LEFT JOIN 
            {{assess_qstn_choice}}
          USING 
            (assess_tst_id, tst_sctn_num, qstn_choice_num, assess_qstn_num)
          WHERE 
            assess_tst_id = '{test_id}'
          GROUP BY 
            ofndr_num, 
            body_loc_cd,
            ofndr_tst_id, 
            assess_tst_id,
            tst_dt,
            score_by_name,
            assess_qstn_num, 
            tst_sctn_num
        ) 
    """.format(
        test_id=test_id
    )
