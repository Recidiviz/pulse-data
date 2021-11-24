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
"""Dedup priority for session start reasons"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_NAME = (
    "admission_start_reason_dedup_priority"
)

ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION = """
This view defines a prioritized ranking for supervision start reasons and incarceration admission reasons. This view is
ultimately used to deduplicate incarceration admissions and supervision starts so that there is only one event per person
per day. Prioritization and deduplication is done within incarceration and within supervision meaning that a person
could in theory have both a supervision start and incarceration admission on the same day. Deduplication across incarceration
and supervision is handled based on a join condition to deduplicated population metrics
"""

SUPERVISION_START_REASON_ORDERED_PRIORITY = [
    StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
    StateSupervisionPeriodAdmissionReason.ABSCONSION,
    StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
    StateSupervisionPeriodAdmissionReason.INVESTIGATION,
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
    StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
    StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
    StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
    StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN,
]

INCARCERATION_START_REASON_ORDERED_PRIORITY = [
    StateIncarcerationPeriodAdmissionReason.REVOCATION,
    # TODO(#9865): Delete `PAROLE_REVOCATION`, `DUAL_REVOCATION`, and `PROBATION_REVOCATION` once
    #  collapsed to `REVOCATION`.
    StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
    StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
    StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
    StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
    StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
    StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
    StateIncarcerationPeriodAdmissionReason.TRANSFER,
    StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
    StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
    StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
    StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
]

ADMISSION_START_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        'SUPERVISION_START' AS metric_source,
        * 
    FROM UNNEST([{prioritized_supervision_start_reasons}]) AS start_reason
    WITH OFFSET AS priority

    UNION ALL

    SELECT 
        'INCARCERATION_ADMISSION' AS metric_source,
        *
    FROM UNNEST([{prioritized_incarceration_start_reasons}]) AS 
start_reason
    WITH OFFSET AS priority  
    """

ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=ADMISSION_START_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
    prioritized_supervision_start_reasons=(
        "\n,".join(
            [
                f"'{start_reason.value}'"
                for start_reason in SUPERVISION_START_REASON_ORDERED_PRIORITY
            ]
        )
    ),
    prioritized_incarceration_start_reasons=(
        "\n,".join(
            [
                f"'{start_reason.value}'"
                for start_reason in INCARCERATION_START_REASON_ORDERED_PRIORITY
            ]
        )
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()
