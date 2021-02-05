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

"""Query containing incarceration incident information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """WITH
inmate_number_time_spans AS (
  SELECT
      mov_cnt_num,
      mov_cur_inmt_num,
      mov_move_date AS inmate_num_lower_bound_date_inclusive,
      LEAD(mov_move_date) OVER (PARTITION BY mov_cnt_num ORDER BY mov_seq_num) AS inmate_num_upper_bound_date_exclusive
  FROM (
    SELECT 
      mov_cnt_num,
      mov_cur_inmt_num,
      mov_seq_num,
      mov_move_date,
      mov_move_time,
      ROW_NUMBER() OVER (PARTITION BY mov_cnt_num, mov_cur_inmt_num ORDER BY mov_seq_num) AS seq_rank_among_inmate_numbers
    FROM {dbo_Movrec}
  )
  WHERE seq_rank_among_inmate_numbers = 1
)
SELECT 
    COALESCE(ids.control_number, m.control_number) AS control_number,  
    spans.mov_cur_inmt_num AS inmate_number, m.* EXCEPT (control_number)
FROM 
  {dbo_Miscon} m
LEFT OUTER JOIN
  inmate_number_time_spans spans
ON 
  m.control_number = spans.mov_cnt_num 
  AND m.misconduct_date >= spans.inmate_num_lower_bound_date_inclusive 
  AND (spans.inmate_num_upper_bound_date_exclusive IS NULL OR m.misconduct_date < spans.inmate_num_upper_bound_date_exclusive)
LEFT OUTER JOIN
  -- In 20-ish cases, the control_number in dbo_Miscon does not correspond to any control number in 
  -- dbo_tblSearchInmateInfo. We generally want to rely on dbo_tblSearchInmateInfo, since that's the file we use to 
  -- ingest person id links.
  (SELECT DISTINCT control_number, inmate_number FROM {dbo_tblSearchInmateInfo}) ids
ON spans.mov_cur_inmt_num = ids.inmate_number
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='dbo_Miscon',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='control_number ASC, misconduct_number ASC'
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
