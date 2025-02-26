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
"""Uses information on prioritized release dates, classes, and statutory requirements to
generate groups of people who may be eligible for prioritization for MOSOP (MO Sex Offender Program). 
The groups are categorized as follows.

1a: People with 18 months or less until their CR, board, or Max date, no past failures for MOSOP classes
1b: People with 18 months or less until their CR, board, or Max date, 1 past failure for MOSOP classes, and its been 6 months since the latest class terminated

2: People with a prioritized date in the past

3a: People whose CR date was "pulled" and their max discharge date is 18 months or less out
3b: People with 18 months or less until their ME date, no past failures for MOSOP classes, Phase I completed
3c: People with 18 months or less until their ME date, have not completed Phase I"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_MOSOP_PRIO_GROUPS_VIEW_NAME = "us_mo_mosop_prio_groups"

US_MO_MOSOP_PRIO_GROUPS_VIEW_DESCRIPTION = """Uses information on prioritized release dates, classes, and statutory requirements to
generate groups of people who may be eligible for prioritization for MOSOP (MO Sex Offender Program). 
The groups are categorized as follows.

1a: People with 18 months or less until their CR, board, or Max date, no past failures for MOSOP classes
1b: People with 18 months or less until their CR, board, or Max date, 1 past failure for MOSOP classes, and its been 6 months since the latest class terminated

2: People with a prioritized date in the past

3a: People whose CR date was "pulled" and their max discharge date is 18 months or less out
3b: People with 18 months or less until their ME date, no past failures for MOSOP classes, Phase I completed
3c: People with 18 months or less until their ME date, have not completed Phase I"""

US_MO_MOSOP_PRIO_GROUPS_QUERY_TEMPLATE = """
WITH pt_mosop AS (
  SELECT * FROM `{project_id}.{analyst_dataset}.us_mo_program_tracks_materialized`
  WHERE mosop_indicator AND NOT completed_flag AND NOT ongoing_flag AND uns_ct < 2
),

grp_1a AS (
    SELECT 
        *,
        "1a" AS eligibility_group,
        "CR/Board Date <18mo, no past MOSOP failures" AS group_desc,
        1 AS group_rank
    FROM pt_mosop
    WHERE
        (DATE_DIFF(conditional_release, CURRENT_DATE('US/Eastern'), MONTH) <= 18 OR 
        DATE_DIFF(board_determined_release_date, CURRENT_DATE('US/Eastern'), MONTH) <= 18 OR
        DATE_DIFF(max_discharge, CURRENT_DATE('US/Eastern'), MONTH) <= 18) 
        AND NOT HAS_UNS 
),
grp_1b AS (
    SELECT 
        *,
        "1b" AS eligibility_group,
        "CR/Board Date <18mo, 6 months since 1 MOSOP failure" AS group_desc,
        2 AS group_rank
    FROM pt_mosop
    WHERE
        (DATE_DIFF(conditional_release, CURRENT_DATE('US/Eastern'), MONTH) <= 18 OR 
        DATE_DIFF(board_determined_release_date, CURRENT_DATE('US/Eastern'), MONTH) <= 18 OR
        DATE_DIFF(max_discharge, CURRENT_DATE('US/Eastern'), MONTH) <= 18) 

        AND uns_ct = 1 AND DATE_DIFF(CURRENT_DATE('US/Eastern'), most_recent_failure, MONTH) >= 6
),
grp_2 AS (
    SELECT 
        *, 
        "2" AS eligibility_group,
        "Past prioritized date" AS group_desc,
        0 AS group_rank
    FROM pt_mosop
    WHERE prioritized_date < CURRENT_DATE('US/Eastern')
),
grp_3a AS (
    SELECT 
        *, 
        "3a" AS eligibility_group,
        "CR date pulled, max discharge <18 mo." AS group_desc,
        3 AS group_rank
    FROM pt_mosop
    WHERE board_determined_release_date = max_discharge
        AND DATE_DIFF(max_discharge, CURRENT_DATE('US/Eastern'), MONTH) <= 18
),
grp_3b AS (
    SELECT 
        *, 
        "3b" AS eligibility_group,
        "ME/MPT date <18mo, no past MOSOP failures" AS group_desc,
        4 AS group_rank
    FROM pt_mosop
    WHERE DATE_DIFF(
        GREATEST(IFNULL(minimum_eligibility_date, minimum_mandatory_release_date), IFNULL(minimum_mandatory_release_date, minimum_eligibility_date)
        ), CURRENT_DATE('US/Eastern'), MONTH) <= 18 
        AND NOT has_uns
        AND completed_p1
),
grp_3c AS (
    SELECT 
        *, 
        "3c" AS eligibility_group,
        "ME/MPT date <18 mo, havent completed Phase 1" AS group_desc,
        5 AS group_rank
    FROM pt_mosop
    WHERE DATE_DIFF(
        GREATEST(IFNULL(minimum_eligibility_date, minimum_mandatory_release_date), IFNULL(minimum_mandatory_release_date, minimum_eligibility_date)
        ), CURRENT_DATE('US/Eastern'), MONTH) <= 18 
    AND NOT completed_p1
)

SELECT * FROM (
    SELECT * FROM grp_1a
    UNION ALL
    SELECT * FROM grp_1b
    UNION ALL (
    SELECT * FROM grp_2)
    UNION ALL (
    SELECT * FROM grp_3a)
    UNION ALL (
    SELECT * FROM grp_3b)
    UNION ALL (
    SELECT * FROM grp_3c)
) 
QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY group_rank) = 1
"""

US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_MO_MOSOP_PRIO_GROUPS_VIEW_NAME,
    view_query_template=US_MO_MOSOP_PRIO_GROUPS_QUERY_TEMPLATE,
    description=US_MO_MOSOP_PRIO_GROUPS_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER.build_and_print()
