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
"""Dedup priority for session end reasons"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_NAME = (
    "release_termination_reason_dedup_priority"
)

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION = """
This view defines a prioritized ranking for supervision termination reasons and incarceration release reasons. This view is
ultimately used to deduplicate incarceration releases and supervision terminations so that there is only one event per person
per day. Prioritization and deduplication is done within incarceration and within supervision meaning that a person
could in theory have both a supervision termination and incarceration release on the same day. Deduplication across incarceration
and supervision is handled based on a join condition to deduplicated population metrics
"""

INCARCERATION_RELEASE_REASON_ORDERED_PRIORITY = [
    StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
    StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
    StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
    StateIncarcerationPeriodReleaseReason.ESCAPE,
    StateIncarcerationPeriodReleaseReason.EXECUTION,
    StateIncarcerationPeriodReleaseReason.DEATH,
    StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
    StateIncarcerationPeriodReleaseReason.COURT_ORDER,
    StateIncarcerationPeriodReleaseReason.COMMUTED,
    StateIncarcerationPeriodReleaseReason.PARDONED,
    StateIncarcerationPeriodReleaseReason.COMPASSIONATE,
    StateIncarcerationPeriodReleaseReason.VACATED,
    StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
    StateIncarcerationPeriodReleaseReason.TRANSFER,
    StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
    StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
    StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN,
]

SUPERVISION_TERMINATION_REASON_ORDERED_PRIORITY = [
    StateSupervisionPeriodTerminationReason.DISCHARGE,
    StateSupervisionPeriodTerminationReason.EXPIRATION,
    StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
    StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
    StateSupervisionPeriodTerminationReason.REVOCATION,
    StateSupervisionPeriodTerminationReason.ABSCONSION,
    StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION,
    StateSupervisionPeriodTerminationReason.DEATH,
    StateSupervisionPeriodTerminationReason.SUSPENSION,
    StateSupervisionPeriodTerminationReason.PARDONED,
    StateSupervisionPeriodTerminationReason.COMMUTED,
    StateSupervisionPeriodTerminationReason.VACATED,
    StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
    StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
    StateSupervisionPeriodTerminationReason.INVESTIGATION,
    StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
    StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN,
]

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        'INCARCERATION_RELEASE' AS metric_source,
        * 
    FROM UNNEST([{prioritized_incarceration_release_reasons}]) AS end_reason
    WITH OFFSET AS priority

    UNION ALL 
    SELECT 
        'SUPERVISION_TERMINATION' AS metric_source,
        * 
    FROM UNNEST([{prioritized_supervision_termination_reasons}]) AS end_reason
    WITH OFFSET AS priority

    """

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
    prioritized_incarceration_release_reasons=(
        "\n,".join(
            [
                f"'{end_reason.value}'"
                for end_reason in INCARCERATION_RELEASE_REASON_ORDERED_PRIORITY
            ]
        )
    ),
    prioritized_supervision_termination_reasons=(
        "\n,".join(
            [
                f"'{end_reason.value}'"
                for end_reason in SUPERVISION_TERMINATION_REASON_ORDERED_PRIORITY
            ]
        )
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()
