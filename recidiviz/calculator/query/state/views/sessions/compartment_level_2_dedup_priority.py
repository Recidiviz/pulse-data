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
"""Dedup priority for compartment_level_2 values in population metrics"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_NAME = "compartment_level_2_dedup_priority"

COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_DESCRIPTION = (
    """Dedup priority for compartment_level_2 values in population metrics"""
)

COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      'INCARCERATION' AS compartment_level_1,
      *
    FROM
      UNNEST([ 'GENERAL', 'TREATMENT_IN_PRISON', 'PAROLE_BOARD_HOLD', 'COMMUNITY_PLACEMENT_PROGRAM', 'INTERNAL_UNKNOWN']) AS compartment_level_2
    WITH
    OFFSET
      AS priority
    UNION ALL
    SELECT
      'SUPERVISION' AS compartment_level_1,
      *
    FROM
      UNNEST([ 'DUAL', 'PAROLE', 'PROBATION', 'INFORMAL_PROBATION', 'BENCH_WARRANT', 'ABSCONSION', 'INTERNAL_UNKNOWN']) AS compartment_level_2
    WITH
    OFFSET
      AS priority
    UNION ALL
    SELECT
      'SUPERVISION_OUT_OF_STATE' AS compartment_level_1,
      *
    FROM
      UNNEST([ 'DUAL', 'PAROLE', 'PROBATION',  'INFORMAL_PROBATION', 'INTERNAL_UNKNOWN']) AS compartment_level_2
    WITH
    OFFSET
      AS priority 
    """

COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()
