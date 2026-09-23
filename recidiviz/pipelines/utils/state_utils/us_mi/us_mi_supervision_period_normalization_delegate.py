# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""US_MI implementation of the StateSpecificSupervisionNormalizationDelegate."""
from copy import copy
from typing import Dict, List, Optional

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.date import CriticalRangesBuilder
from recidiviz.ingest.direct.regions.us_mi.constants import COMS_MIGRATION_DATE
from recidiviz.persistence.entity.activity.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.persistence.entity.activity.normalized_entities_utils import (
    update_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.pipelines.ingest.activity.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.supervision_period_utils import SUCCESSFUL_TERMINATIONS

# Prefixes used to preserve the overlapping incarceration period's own admission/
# release reason as context on inferred IN_CUSTODY periods, since neither
# StateSupervisionPeriodAdmissionReason nor StateSupervisionPeriodTerminationReason
# has a value describing "became in-custody while remaining nominally supervised".
_INFERRED_ADMISSION_REASON_RAW_TEXT_PREFIX = (
    "INFERRED_FROM_INCARCERATION_ADMISSION_REASON"
)
_INFERRED_TERMINATION_REASON_RAW_TEXT_PREFIX = (
    "INFERRED_FROM_INCARCERATION_RELEASE_REASON"
)


class UsMiSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_MI implementation of the StateSpecificSupervisionNormalizationDelegate."""

    # TODO(#30367): Revisit and see if we can omit investigation periods in ingest directly
    def drop_bad_periods(
        self, sorted_supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        periods_to_keep = []

        for sp in sorted_supervision_periods:
            # If supervision type = INVESTIGATION, let's drop
            if (
                sp.supervision_type
                == StateSupervisionPeriodSupervisionType.INVESTIGATION
            ):
                continue

            periods_to_keep.append(sp)

        return periods_to_keep

    def supervision_level_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionLevel]:
        """
        For pre-COMS supervision periods (start date before 8/14/23) where supervision level is missing and the
        period doesn't end in a successful termination, override the supervision level and set it to IN_CUSTODY.
        We've validated with trusted testers that for pre-COMS data, a missing supervision level indicates the
        person is in custody. Post-COMS, supervision levels are supposed to be consistently populated and this inference is
        no longer needed.
        """

        sp = sorted_supervision_periods[supervision_period_list_index]

        if (
            sp.supervision_level_raw_text is None
            and sp.supervision_level is None
            and sp.termination_reason not in SUCCESSFUL_TERMINATIONS
            and sp.start_date < COMS_MIGRATION_DATE
        ):
            return StateSupervisionLevel.IN_CUSTODY

        return sp.supervision_level

    def infer_additional_periods(
        self,
        person_id: int,
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateSupervisionPeriod]:
        """Infers additional supervision periods with a supervision_level of
        IN_CUSTODY for any span of time where a supervision period overlaps with an
        incarceration period, so that a client who is simultaneously supervised and
        incarcerated shows an IN_CUSTODY supervision level for that span. The
        original supervision periods are left unmodified; each inferred period is a
        new, separate period covering just the overlapping sub-span."""
        return supervision_periods + self._infer_in_custody_periods(
            person_id, supervision_periods, incarceration_periods
        )

    @staticmethod
    def _infer_in_custody_periods(
        person_id: int,
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateSupervisionPeriod]:
        """Returns a new, separate StateSupervisionPeriod with a supervision_level of
        IN_CUSTODY for each critical range where a supervision period and an
        incarceration period overlap. Uses the CriticalRangesBuilder to create a set
        of key spans that either have no periods, an SP, an IP, or overlapping
        SPs/IPs, then infers an IN_CUSTODY period for any span with both an
        overlapping SP and an overlapping IP."""
        if not supervision_periods or not incarceration_periods:
            return []

        critical_range_builder = CriticalRangesBuilder(
            [*supervision_periods, *incarceration_periods]
        )

        inferred_periods: List[StateSupervisionPeriod] = []

        # The number of periods that have been inferred so far from the supervision
        # period with external_id=key, used to build a unique external_id suffix.
        inferred_period_count_by_sp_external_id: Dict[str, int] = {}

        for critical_range in critical_range_builder.get_sorted_critical_ranges():
            overlapping_sps = (
                critical_range_builder.get_objects_overlapping_with_critical_range(
                    critical_range, StateSupervisionPeriod
                )
            )
            overlapping_ips = (
                critical_range_builder.get_objects_overlapping_with_critical_range(
                    critical_range, StateIncarcerationPeriod
                )
            )
            if not overlapping_sps or not overlapping_ips:
                continue

            # It's rare, but a person could have more than one overlapping
            # supervision or incarceration period for the same span (e.g. dual
            # supervision types). Arbitrarily use the first of each, mirroring the
            # same simplification made in infer_incarceration_periods_from_in_custody_sps.
            source_sp = overlapping_sps[0]
            source_ip = overlapping_ips[0]

            if source_sp.supervision_level == StateSupervisionLevel.IN_CUSTODY:
                continue

            inferred_period_count = inferred_period_count_by_sp_external_id.get(
                source_sp.external_id, 0
            )
            inferred_period_count_by_sp_external_id[source_sp.external_id] = (
                inferred_period_count + 1
            )

            is_open = critical_range.upper_bound_exclusive_date is None

            # Note: this is a plain shallow copy, not copy_entities_and_add_unique_ids
            # - the unique id is generated further down, once the segment's own
            # distinguishing fields (external_id, dates) are set. Generating it here,
            # from the still-source_sp-identical copy, would give every segment split
            # off of the same source_sp an identical (non-unique) id.
            segment_sp = copy(source_sp)
            segment_sp = deep_entity_update(
                segment_sp,
                external_id=f"{source_sp.external_id}-{inferred_period_count}-IN-CUSTODY",
                start_date=critical_range.lower_bound_inclusive_date,
                termination_date=critical_range.upper_bound_exclusive_date,
                supervision_level=StateSupervisionLevel.IN_CUSTODY,
                supervision_level_raw_text=None,
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                admission_reason_raw_text=(
                    f"{_INFERRED_ADMISSION_REASON_RAW_TEXT_PREFIX}_"
                    f"{source_ip.admission_reason.value if source_ip.admission_reason else 'UNKNOWN'}"
                ),
                termination_reason=(
                    None
                    if is_open
                    else StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
                ),
                termination_reason_raw_text=(
                    None
                    if is_open
                    else (
                        f"{_INFERRED_TERMINATION_REASON_RAW_TEXT_PREFIX}_"
                        f"{source_ip.release_reason.value if source_ip.release_reason else 'UNKNOWN'}"
                    )
                ),
                case_type_entries=[],
            )
            update_entity_with_globally_unique_id(
                root_entity_id=person_id, entity=segment_sp
            )

            inferred_periods.append(segment_sp)

        return inferred_periods
