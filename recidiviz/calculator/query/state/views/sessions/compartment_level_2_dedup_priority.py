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
"""Dedup priority for compartment_level_2 values in compartment_sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_NAME = "compartment_level_2_dedup_priority"

COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_DESCRIPTION = """
This view defines a prioritized ranking for compartment level 2 values (supervision type and specialized purpose for incarceration).
This view is ultimately used to deduplicate incarceration compartments and supervision compartments so that there is only one
compartment_level_2 value per person per day. Prioritization and deduplication is done within incarceration and within supervision
meaning that a person could in theory have both a supervision compartment and incarceration compartment on the same day.
Deduplication across incarceration and supervision is handled based on a join condition to deduplicated population metrics
"""

# TODO(#7912): Add sessions compartment_level_2 enums to the state schema
SUPERVISION_TYPE_ORDERED_PRIORITY = [
    StateSupervisionPeriodSupervisionType.DUAL,
    StateSupervisionPeriodSupervisionType.PAROLE,
    StateSupervisionPeriodSupervisionType.PROBATION,
    StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
    StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
    StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
    StateSupervisionPeriodAdmissionReason.ABSCONSION,
    StateSupervisionPeriodSupervisionType.INVESTIGATION,
    StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
    StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
]

SPECIALIZED_PURPOSE_FOR_INCARCERATION_ORDERED_PRIORITY = [
    StateSpecializedPurposeForIncarceration.GENERAL,
    StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
    StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
    StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
    StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
    # Used for ND's CPP sessions
    StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
    StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT,
    StateSupervisionPeriodAdmissionReason.ABSCONSION,
    StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN,
    StateSpecializedPurposeForIncarceration.EXTERNAL_UNKNOWN,
]


COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        *
    FROM UNNEST(["INCARCERATION", "INCARCERATION_OUT_OF_STATE"]) AS compartment_level_1,
        UNNEST([{prioritized_specialized_purpose_for_incarceration}]) AS compartment_level_2
    WITH OFFSET AS priority

    UNION ALL

    SELECT
        *
    FROM UNNEST(["SUPERVISION", "SUPERVISION_OUT_OF_STATE"]) AS compartment_level_1,
        UNNEST([{prioritized_supervision_type}]) AS compartment_level_2
    WITH OFFSET AS priority
    """

COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
    prioritized_supervision_type=(
        "\n,".join([f"'{type.value}'" for type in SUPERVISION_TYPE_ORDERED_PRIORITY])
    ),
    prioritized_specialized_purpose_for_incarceration=(
        "\n,".join(
            [
                f"'{type.value}'"
                for type in SPECIALIZED_PURPOSE_FOR_INCARCERATION_ORDERED_PRIORITY
            ]
        )
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()
