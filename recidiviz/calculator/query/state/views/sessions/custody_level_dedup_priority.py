# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Dedup priority for custody levels"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodCustodyLevel,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_NAME = "custody_level_dedup_priority"

CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_DESCRIPTION = """
This view defines a prioritized ranking for custody levels. This view is ultimately used to deduplicate 
incarceration periods so that there is only one level per person per day
"""

CUSTODY_LEVEL_ORDERED_PRIORITY = [
    StateIncarcerationPeriodCustodyLevel.SOLITARY_CONFINEMENT,
    StateIncarcerationPeriodCustodyLevel.MAXIMUM,
    StateIncarcerationPeriodCustodyLevel.CLOSE,
    StateIncarcerationPeriodCustodyLevel.MEDIUM,
    StateIncarcerationPeriodCustodyLevel.RESTRICTIVE_MINIMUM,
    StateIncarcerationPeriodCustodyLevel.MINIMUM,
    StateIncarcerationPeriodCustodyLevel.INTAKE,
    StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
    StateIncarcerationPeriodCustodyLevel.EXTERNAL_UNKNOWN,
]

CUSTODY_LEVEL_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    SELECT 
        custody_level,
        custody_level_priority,
        -- Indicator for whether custody level can be assigned based on risk level/CO discretion, to determine inclusion in downgrade/upgrade counts
        custody_level IN ('SOLITARY_CONFINEMENT','MAXIMUM', 'CLOSE', 'MEDIUM', 'RESTRICTIVE_MINIMUM', 'MINIMUM', 'INTAKE') AS is_discretionary_level
    FROM UNNEST([{prioritized_custody_levels}]) AS custody_level
    WITH OFFSET AS custody_level_priority    
    """

CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=CUSTODY_LEVEL_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
    prioritized_custody_levels=(
        "\n,".join([f"'{level.value}'" for level in CUSTODY_LEVEL_ORDERED_PRIORITY])
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()
