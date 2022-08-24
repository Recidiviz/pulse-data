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
"""Identifier class for events related to incarceration."""
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import attr

from recidiviz.calculator.pipeline.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContext,
)
from recidiviz.calculator.pipeline.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
    SupervisionPopulationSpan,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.identifier_models import Span
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    identify_most_severe_case_type,
    supervision_period_is_out_of_state,
)
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import (
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_judicial_district_association import (
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.date import (
    DateRange,
    DateRangeDiff,
    NonNegativeDateRange,
    PotentiallyOpenDateRange,
    convert_critical_dates_to_time_spans,
    merge_sorted_date_ranges,
    tomorrow,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StatePerson


class PopulationSpanIdentifier(BaseIdentifier[List[Span]]):
    """Identifier class for events related to incarceration."""

    def __init__(self) -> None:
        self.identifier_result_class = Span
        self.field_index = CoreEntityFieldIndex()

    def identify(
        self, _person: StatePerson, identifier_context: IdentifierContext
    ) -> List[Span]:

        return self._find_incarceration_spans(
            incarceration_delegate=identifier_context[
                StateSpecificIncarcerationDelegate.__name__
            ],
            incarceration_periods=identifier_context[
                NormalizedStateIncarcerationPeriod.base_class_name()
            ],
            incarceration_period_judicial_district_association=identifier_context[
                INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME
            ],
        ) + self._find_supervision_spans(
            supervision_delegate=identifier_context[
                StateSpecificSupervisionDelegate.__name__
            ],
            supervision_periods=identifier_context[
                NormalizedStateSupervisionPeriod.base_class_name()
            ],
            supervision_period_to_agent_association=identifier_context[
                SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME
            ],
            supervision_period_judicial_district_association=identifier_context[
                SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME
            ],
            incarceration_periods=identifier_context[
                NormalizedStateIncarcerationPeriod.base_class_name()
            ],
            incarceration_delegate=identifier_context[
                StateSpecificIncarcerationDelegate.__name__
            ],
        )

    def _find_incarceration_spans(
        self,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        incarceration_period_judicial_district_association: List[Dict[str, Any]],
    ) -> List[Span]:
        """Finds instances of various events related to incarceration.
        Transforms the person's StateIncarcerationPeriods into IncarcerationPopulationSpans.

        Returns:
            A list of IncarcerationPopulationSpans for the person.
        """
        incarceration_spans: List[IncarcerationPopulationSpan] = []

        if not incarceration_periods:
            return cast(List[Span], incarceration_spans)

        # Convert the list of dictionaries into one dictionary where the keys are the
        # incarceration_period_id values
        incarceration_period_to_judicial_district: Dict[
            int, Dict[str, Any]
        ] = list_of_dicts_to_dict_with_keys(
            incarceration_period_judicial_district_association,
            key=NormalizedStateIncarcerationPeriod.get_class_id_name(),
        )

        ip_index = NormalizedIncarcerationPeriodIndex(
            sorted_incarceration_periods=incarceration_periods,
            incarceration_delegate=incarceration_delegate,
        )

        for incarceration_period in ip_index.sorted_incarceration_periods:
            if incarceration_period.admission_date is None:
                raise ValueError("Unexpected supervision period without start_date")

            if not incarceration_period.incarceration_period_id:
                raise ValueError(
                    "Unexpected incarceration period without an incarceration_period_id."
                )
            incarceration_spans.append(
                IncarcerationPopulationSpan(
                    state_code=incarceration_period.state_code,
                    start_date_inclusive=incarceration_period.admission_date,
                    end_date_exclusive=incarceration_period.release_date,
                    included_in_state_population=incarceration_delegate.is_period_included_in_state_population(
                        incarceration_period
                    ),
                    facility=incarceration_period.facility,
                    purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                    custodial_authority=incarceration_period.custodial_authority,
                    judicial_district_code=self._get_judicial_district_code_for_period(
                        incarceration_period,
                        incarceration_period_to_judicial_district,
                    ),
                )
            )

        return cast(List[Span], incarceration_spans)

    def _find_supervision_spans(
        self,
        supervision_delegate: StateSpecificSupervisionDelegate,
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        supervision_period_to_agent_association: List[Dict[str, Any]],
        supervision_period_judicial_district_association: List[Dict[str, Any]],
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        incarceration_delegate: StateSpecificIncarcerationDelegate,
    ) -> List[Span]:
        """Finds instances of various events related to incarceration.
        Transforms the person's StateSupervisionPeriods into SupervisionPopulationSpans.

        Returns:
            A list of SupervisionPopulationSpans for the person.
        """
        supervision_spans: List[SupervisionPopulationSpan] = []

        if not supervision_periods:
            return cast(List[Span], supervision_spans)

        # Convert the list of dictionaries into one dictionary where the keys are the
        # incarceration_period_id values
        supervision_period_to_judicial_district: Dict[
            int, Dict[str, Any]
        ] = list_of_dicts_to_dict_with_keys(
            supervision_period_judicial_district_association,
            key=NormalizedStateSupervisionPeriod.get_class_id_name(),
        )

        supervision_period_to_agent: Dict[
            int, Dict[str, Any]
        ] = list_of_dicts_to_dict_with_keys(
            supervision_period_to_agent_association,
            key=NormalizedStateSupervisionPeriod.get_class_id_name(),
        )

        sp_index = NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=supervision_periods
        )

        ip_index = NormalizedIncarcerationPeriodIndex(
            sorted_incarceration_periods=incarceration_periods,
            incarceration_delegate=incarceration_delegate,
        )

        # We need to split the spans based on the durations that a person is incarcerated
        # as well in order to determine if a person is to be counted towards the state's
        # supervision population.
        durations_incarcerated: List[NonNegativeDateRange] = merge_sorted_date_ranges(
            [
                ip.duration
                for ip in ip_index.incarceration_periods_that_exclude_person_from_supervision_population
            ]
        )

        next_relevant_ip_index = 0

        for supervision_period in sp_index.sorted_supervision_periods:
            if supervision_period.start_date is None:
                raise ValueError("Unexpected supervision period without start_date")
            if not supervision_period.supervision_period_id:
                raise ValueError(
                    "Unexpected supervision period without a supervision_period_id."
                )

            # Find all relevant IPs that overlap with the SP
            relevant_ip_durations = []
            while next_relevant_ip_index < len(durations_incarcerated):
                next_relevant_ip_duration = durations_incarcerated[
                    next_relevant_ip_index
                ]
                if DateRangeDiff(
                    next_relevant_ip_duration, supervision_period.duration
                ).overlapping_range:
                    relevant_ip_durations.append(next_relevant_ip_duration)
                if (
                    next_relevant_ip_duration.upper_bound_exclusive_date
                    > supervision_period.duration.upper_bound_exclusive_date
                ):
                    # Found an IP that extends past where our SP ends, stop here.
                    # Inspect this IP first when we get to the next SP.
                    break
                next_relevant_ip_index += 1

            # Break up the SP into pieces based on relevant overlapping IPs
            sub_supervision_period_durations: List[Tuple[DateRange, bool]] = []
            sp_remaining_duration: Optional[DateRange] = supervision_period.duration
            for ip_duration in relevant_ip_durations:
                if not sp_remaining_duration:
                    break
                range_diff = DateRangeDiff(ip_duration, sp_remaining_duration)
                before_part = range_diff.range_2_non_overlapping_before_part
                if before_part:
                    sub_supervision_period_durations.append((before_part, False))
                overlapping_part = range_diff.overlapping_range
                if overlapping_part:
                    sub_supervision_period_durations.append((overlapping_part, True))
                sp_remaining_duration = range_diff.range_2_non_overlapping_after_part

            if sp_remaining_duration:
                sub_supervision_period_durations.append((sp_remaining_duration, False))

            # Build SPs from the SP subduration
            (
                level_1_supervision_location,
                level_2_supervision_location,
            ) = supervision_delegate.supervision_location_from_supervision_site(
                supervision_period.supervision_site
            )
            deprecated_supervising_district_external_id = (
                supervision_delegate.get_deprecated_supervising_district_external_id(
                    level_1_supervision_location, level_2_supervision_location
                )
            )
            supervising_officer_external_id = supervision_delegate.get_supervising_officer_external_id_for_supervision_period(
                supervision_period, supervision_period_to_agent
            )
            case_type = identify_most_severe_case_type(supervision_period)
            judicial_district_code = self._get_judicial_district_code_for_period(
                supervision_period,
                supervision_period_to_judicial_district,
            )
            sp_in_state_population_based_on_metadata = supervision_delegate.supervision_period_in_supervision_population_in_non_excluded_date_range(
                supervision_period, supervising_officer_external_id
            )
            supervision_type = (
                supervision_period.supervision_type
                if supervision_period.supervision_type
                else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
            )

            for sp_duration, overlaps_with_ip in sub_supervision_period_durations:
                included_in_state_population = (
                    not overlaps_with_ip and sp_in_state_population_based_on_metadata
                )
                end_date_exclusive = (
                    sp_duration.upper_bound_exclusive_date
                    if sp_duration.upper_bound_exclusive_date != tomorrow()
                    else None
                )
                span = SupervisionPopulationSpan(
                    state_code=supervision_period.state_code,
                    start_date_inclusive=sp_duration.lower_bound_inclusive_date,
                    end_date_exclusive=end_date_exclusive,
                    supervision_level=supervision_period.supervision_level,
                    supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                    supervision_type=supervision_type,
                    case_type=case_type,
                    custodial_authority=supervision_period.custodial_authority,
                    judicial_district_code=judicial_district_code,
                    supervising_officer_external_id=supervising_officer_external_id,
                    supervising_district_external_id=deprecated_supervising_district_external_id,
                    level_1_supervision_location_external_id=level_1_supervision_location,
                    level_2_supervision_location_external_id=level_2_supervision_location,
                    included_in_state_population=included_in_state_population,
                )
                span = attr.evolve(
                    span,
                    included_in_state_population=(
                        span.included_in_state_population
                        and not supervision_period_is_out_of_state(
                            span, supervision_delegate
                        )
                    ),
                )
                supervision_spans.append(span)

        supervision_spans = (
            self._convert_spans_to_dual(supervision_spans)
            if supervision_delegate.supervision_types_mutually_exclusive()
            else self._expand_dual_supervision_spans(supervision_spans)
        )

        return cast(List[Span], supervision_spans)

    def _convert_spans_to_dual(
        self, supervision_spans: List[SupervisionPopulationSpan]
    ) -> List[SupervisionPopulationSpan]:
        """For some states, we want to track DUAL supervision as distinct from both
        PAROLE and PROBATION. For these states, if someone has two spans on the same day
        that will contribute to the same type of metric, and these events are of different
        supervision types (one is PAROLE and one is PROBATION, or one is DUAL and the other
        is something other than DUAL), then we want that person to only contribute to metrics
        with a supervision type of DUAL. All events of that type on that overlapping span
        are then replaced with ones that have DUAL as the set supervision_type.

        Returns an updated list of SupervisionPopulationSpans."""
        if not supervision_spans:
            return supervision_spans

        revised_supervision_spans: List[SupervisionPopulationSpan] = []

        # First, generate a set of critical dates from all of the spans
        time_spans: List[PotentiallyOpenDateRange] = self._get_new_spans(
            supervision_spans
        )

        start_dates: List[date] = sorted(
            time_span.lower_bound_inclusive_date for time_span in time_spans
        )
        original_spans_overlapping_start_date: Dict[
            date, List[SupervisionPopulationSpan]
        ] = self._get_spans_by_critical_date(supervision_spans, start_dates)

        for time_span in time_spans:
            start_date = time_span.lower_bound_inclusive_date
            end_date = time_span.upper_bound_exclusive_date
            spans = original_spans_overlapping_start_date[start_date]

            if not spans:
                continue

            supervision_types = {
                span.supervision_type for span in spans if span.supervision_type
            }

            overwrite_supervision_types_with_dual = (
                StateSupervisionPeriodSupervisionType.PAROLE in supervision_types
                and StateSupervisionPeriodSupervisionType.PROBATION in supervision_types
            ) or (
                StateSupervisionPeriodSupervisionType.DUAL in supervision_types
                and len(supervision_types) > 1
            )

            for span in spans:
                revised_supervision_spans.append(
                    attr.evolve(
                        span,
                        start_date_inclusive=start_date,
                        end_date_exclusive=end_date,
                        supervision_type=StateSupervisionPeriodSupervisionType.DUAL
                        if overwrite_supervision_types_with_dual
                        else span.supervision_type,
                    )
                )

        return revised_supervision_spans

    def _get_spans_by_critical_date(
        self,
        supervision_spans: List[SupervisionPopulationSpan],
        critical_dates: List[date],
    ) -> Dict[date, List[SupervisionPopulationSpan]]:
        """Obtain the supervision spans that overlap with the critical date as a dictionary
        keyed by date."""
        result = defaultdict(list)
        for critical_date in critical_dates:
            for span in supervision_spans:
                if NonNegativeDateRange.from_maybe_open_range(
                    span.start_date_inclusive, span.end_date_exclusive
                ).contains_day(critical_date):
                    result[critical_date].append(span)
        return result

    def _get_new_spans(
        self, supervision_spans: List[SupervisionPopulationSpan]
    ) -> List[PotentiallyOpenDateRange]:
        """Obtain all critical dates from all spans that may overlap with each other.
        Then return all of the time spans of those critical dates."""
        has_null_end_date = False
        critical_date_set: Set[date] = set()
        for span in supervision_spans:
            critical_date_set.add(span.start_date_inclusive)
            if span.end_date_exclusive:
                critical_date_set.add(span.end_date_exclusive)
            else:
                has_null_end_date = True

        return convert_critical_dates_to_time_spans(
            critical_date_set, has_null_end_date
        )

    # TODO(#14800) Revisit this logic once downstream views don't need it.
    def _expand_dual_supervision_spans(
        self, supervision_spans: List[SupervisionPopulationSpan]
    ) -> List[SupervisionPopulationSpan]:
        """For any SupervisionPopulationSpans that are of DUAL supervision type, makes a
        copy of the event that has a PAROLE supervision type and a copy of the event that
        has a PROBATION supervision type. Returns all events, including the duplicated
        events for each of the DUAL supervision events, because we want these events to
        contribute to PAROLE, PROBATION, and DUAL breakdowns of any metric."""
        if not supervision_spans:
            return supervision_spans

        additional_supervision_spans: List[SupervisionPopulationSpan] = []

        for supervision_span in supervision_spans:
            if (
                supervision_span.supervision_type
                == StateSupervisionPeriodSupervisionType.DUAL
            ):
                parole_copy = attr.evolve(
                    supervision_span,
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                )
                additional_supervision_spans.append(parole_copy)
                probation_copy = attr.evolve(
                    supervision_span,
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                )
                additional_supervision_spans.append(probation_copy)

        supervision_spans.extend(additional_supervision_spans)
        return supervision_spans

    def _get_judicial_district_code_for_period(
        self,
        period: Union[
            NormalizedStateIncarcerationPeriod, NormalizedStateSupervisionPeriod
        ],
        period_to_judicial_district: Dict[int, Dict[str, Any]],
    ) -> Optional[str]:
        """Retrieves the judicial_district_code corresponding to the period, if one exists."""
        period_id: Optional[int] = period.get_id()

        if period_id is None:
            raise ValueError(f"Missing primary key on period of type [{type(period)}].")

        info: Optional[Dict[str, Any]] = period_to_judicial_district.get(period_id)

        if info is not None:
            return info["judicial_district_code"]

        return None
