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
"""US_MO implementation of the supervision normalization delegate"""
import logging
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Set

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import (
    DurationMixinT,
    PotentiallyOpenDateRange,
    convert_critical_dates_to_time_spans,
)
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.direct.regions.us_mo.us_mo_custom_enum_parsers import (
    parse_supervision_period_admission_reason,
    parse_supervision_period_termination_reason,
)
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.normalized_entities_utils import (
    copy_entities_and_add_unique_ids,
    update_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import (
    StateSentence,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
    UsMoSentenceStatus,
)
from recidiviz.pipelines.utils.supervision_type_identification import (
    sentence_supervision_types_to_supervision_period_supervision_type,
)
from recidiviz.utils.types import assert_type


class UsMoSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_MO implementation of the supervision normalization delegate"""

    def __init__(self, sentences: List[StateSentence]) -> None:
        self._sentence_statuses_by_sentence: Dict[
            str, List[UsMoSentenceStatus]
        ] = self._get_sentence_statuses_by_sentence(sentences)
        self._supervision_type_spans = self._get_supervision_type_spans_by_sentence()
        self._validate_supervision_type_spans()

    @staticmethod
    def _get_sentence_statuses_by_sentence(
        sentences: List[StateSentence],
    ) -> Dict[str, List[UsMoSentenceStatus]]:
        sentence_status_dict = defaultdict(list)
        for sentence in sentences:
            if not sentence.sentence_status_snapshots:
                logging.warning(
                    "Found sentence [%s] with no sentence statuses",
                    sentence.external_id,
                )
            for status_snapshot in sentence.sentence_status_snapshots:
                sentence_status_dict[sentence.external_id].append(
                    UsMoSentenceStatus.from_sentence_status_snapshot(status_snapshot)
                )
        return sentence_status_dict

    def _get_supervision_type_spans_by_sentence(
        self,
    ) -> Dict[str, List[SupervisionTypeSpan]]:
        """Generates the time span objects representing the supervision type for this
        sentence between certain critical dates where the type may have changed for all
        sentences."""
        supervision_type_spans: Dict[str, List[SupervisionTypeSpan]] = defaultdict(list)
        for (
            sentence_external_id,
            sentence_statuses,
        ) in self._sentence_statuses_by_sentence.items():
            all_critical_statuses = [
                status
                for status in sentence_statuses
                if status.is_supervision_type_critical_status
            ]
            critical_statuses_by_day = defaultdict(list)

            for s in all_critical_statuses:
                if s.status_date is not None:
                    critical_statuses_by_day[s.status_date].append(s)

            critical_days = sorted(critical_statuses_by_day.keys())

            for i, critical_day in enumerate(critical_days):
                start_date = critical_day
                end_date = critical_days[i + 1] if i < len(critical_days) - 1 else None

                supervision_type = (
                    self._get_sentence_supervision_type_from_critical_day_statuses(
                        critical_statuses_by_day[critical_day]
                    )
                )
                supervision_type_spans[sentence_external_id].append(
                    SupervisionTypeSpan(
                        sentence_external_id=sentence_external_id,
                        start_date=start_date,
                        end_date=end_date,
                        supervision_type=supervision_type,
                    )
                )

        return supervision_type_spans

    @staticmethod
    def _get_sentence_supervision_type_from_critical_day_statuses(
        critical_day_statuses: List[UsMoSentenceStatus],
    ) -> Optional[StateSupervisionSentenceSupervisionType]:
        """Given a set of 'supervision type critical' statuses, returns the supervision
        type for the SupervisionTypeSpan starting on that day."""

        # Sort statuses by sequence number in reverse order
        critical_day_statuses.sort(key=lambda s: s.sequence_num, reverse=True)
        supervision_type = critical_day_statuses[
            0
        ].supervision_type_status_classification

        if (
            supervision_type is None
            or supervision_type
            != StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
        ):
            return supervision_type

        # If the most recent status in a day does not give us enough information to tell
        # the supervision type, look to other statuses on that day.
        for status in critical_day_statuses:
            if (
                status.supervision_type_status_classification is not None
                and status.supervision_type_status_classification
                != StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
            ):
                return status.supervision_type_status_classification

        return supervision_type

    def _validate_supervision_type_spans(self) -> None:
        """Validate that the supervision type spans are generated correctly."""
        if self._supervision_type_spans is None:
            raise ValueError("Spans dictionary should not be None")

        for (
            sentence_external_id,
            supervision_type_spans,
        ) in self._supervision_type_spans.items():
            if not supervision_type_spans:
                continue

            last_span = supervision_type_spans[-1]
            if last_span.end_date is not None:
                raise ValueError(
                    f"Must end span list with an open span for sentence_external_id: {sentence_external_id}"
                )

            for not_last_span in supervision_type_spans[:-1]:
                if not_last_span.end_date is None:
                    raise ValueError(
                        f"Intermediate span must not have None end date for sentence_external_id: {sentence_external_id}"
                    )

    def split_periods_based_on_sentences(
        self, person_id: int, supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        """Generates supervision periods based on sentences and critical statuses
        that denote changes in supervision type.

        This assumes that the input supervision periods are already chronologically
        sorted."""
        supervision_type_spans: List[SupervisionTypeSpan] = []
        all_statuses_by_date: Dict[date, Set[UsMoSentenceStatus]] = defaultdict(set)
        for (
            sentence_external_id,
            sentence_statuses,
        ) in self._sentence_statuses_by_sentence.items():
            type_spans: List[SupervisionTypeSpan] = self._supervision_type_spans[
                sentence_external_id
            ]
            for status in sentence_statuses:
                all_statuses_by_date[assert_type(status.status_date, date)].add(status)
            for supervision_type_span in type_spans:
                supervision_type_spans.append(supervision_type_span)

        time_spans: List[PotentiallyOpenDateRange] = self._get_new_period_time_spans(
            supervision_periods, supervision_type_spans
        )

        start_dates: List[date] = sorted(
            time_span.lower_bound_inclusive_date for time_span in time_spans
        )
        periods_by_start_date: Dict[
            date, List[StateSupervisionPeriod]
        ] = self._get_periods_by_critical_date(
            time_periods=supervision_periods, critical_dates=start_dates
        )
        type_spans_by_start_date: Dict[
            date, List[SupervisionTypeSpan]
        ] = self._get_periods_by_critical_date(
            time_periods=supervision_type_spans, critical_dates=start_dates
        )

        new_supervision_periods: List[StateSupervisionPeriod] = []
        for i, time_span in enumerate(time_spans):
            start_date = time_span.lower_bound_inclusive_date
            end_date = time_span.upper_bound_exclusive_date
            period_supervision_type = self._get_period_supervision_type(
                time_span, type_spans_by_start_date
            )
            # If the supervision type is None, this person should not be counted towards
            # the supervision population and we skip this period.
            if period_supervision_type is None:
                continue

            admission_reason_raw_text = self._get_statuses_raw_text_for_date(
                all_statuses_by_date, start_date
            )
            admission_reason = parse_supervision_period_admission_reason(
                normalize(admission_reason_raw_text)
            )
            termination_reason_raw_text = (
                self._get_statuses_raw_text_for_date(all_statuses_by_date, end_date)
                if end_date
                else None
            )
            termination_reason = (
                parse_supervision_period_termination_reason(
                    normalize(termination_reason_raw_text)
                )
                if termination_reason_raw_text
                else None
            )

            relevant_periods = periods_by_start_date[start_date]
            # We take the last relevant period in case there are overlapping supervision
            # periods that match this time span.
            relevant_period = None if not relevant_periods else relevant_periods[-1]

            supervision_site = (
                relevant_period.supervision_site if relevant_period else None
            )
            supervision_level = (
                relevant_period.supervision_level if relevant_period else None
            )
            supervision_level_raw_text = (
                relevant_period.supervision_level_raw_text if relevant_period else None
            )

            supervising_officer_staff_external_id = (
                relevant_period.supervising_officer_staff_external_id
                if relevant_period
                else None
            )
            supervising_officer_staff_external_id_type = (
                relevant_period.supervising_officer_staff_external_id_type
                if relevant_period
                else None
            )

            supervision_period_metadata = (
                relevant_period.supervision_period_metadata if relevant_period else None
            )

            case_type_entries: List[StateSupervisionCaseTypeEntry] = []

            new_supervision_period = StateSupervisionPeriod(
                state_code=StateCode.US_MO.value,
                external_id=f"{person_id}-{i}-NORMALIZED",
                start_date=start_date,
                termination_date=end_date,
                supervision_type=period_supervision_type,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                termination_reason=termination_reason,
                termination_reason_raw_text=termination_reason_raw_text,
                supervision_site=supervision_site,
                supervision_level=supervision_level,
                supervision_level_raw_text=supervision_level_raw_text,
                supervising_officer_staff_external_id=supervising_officer_staff_external_id,
                supervising_officer_staff_external_id_type=supervising_officer_staff_external_id_type,
                supervision_period_metadata=supervision_period_metadata,
            )

            # Add a unique id to the new SP
            update_entity_with_globally_unique_id(
                root_entity_id=person_id, entity=new_supervision_period
            )

            if relevant_period:
                for case_type_entry in relevant_period.case_type_entries:
                    case_type_entry = deep_entity_update(
                        original_entity=case_type_entry,
                        supervision_period=new_supervision_period,
                    )
                    case_type_entries.append(case_type_entry)
                case_type_entries = copy_entities_and_add_unique_ids(
                    person_id=person_id, entities=case_type_entries
                )

            new_supervision_period = deep_entity_update(
                original_entity=new_supervision_period,
                case_type_entries=case_type_entries,
            )

            new_supervision_periods.append(new_supervision_period)

        return new_supervision_periods

    def supervision_level_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionLevel]:
        if self._infer_absconsion_for_existing_period(
            supervision_period_list_index, sorted_supervision_periods
        ):
            return StateSupervisionLevel.ABSCONSION
        return sorted_supervision_periods[
            supervision_period_list_index
        ].supervision_level

    def supervision_type_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        if self._infer_absconsion_for_existing_period(
            supervision_period_list_index, sorted_supervision_periods
        ):
            return StateSupervisionPeriodSupervisionType.ABSCONSION
        return sorted_supervision_periods[
            supervision_period_list_index
        ].supervision_type

    def _infer_absconsion_for_existing_period(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> bool:
        """Identifies if the supervision type and level for a given periodcan be inferred
        as ABSCONSION, based on whether or not it falls between a ABSCONSION termination
        reason and a RETURN_FROM_ABSCONSION admission reason, with no ABSCONSION terminations
        between the period itself and the future RETURN_FROM_ABSCONSION admission."""

        if (
            sorted_supervision_periods[supervision_period_list_index].admission_reason
            == StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
            or sorted_supervision_periods[
                supervision_period_list_index
            ].termination_reason
            == StateSupervisionPeriodTerminationReason.ABSCONSION
        ):
            # We never want to apply the override to periods ending in absconsion, since
            # if the period doesn't follow an earlier absconsion then only future periods
            # should receive the override, and if it does follow an earlier absconsion then
            # it's an ambiguous case where the prior absconsion can't be matched to a return.
            return False

        for period in sorted_supervision_periods[supervision_period_list_index + 1 :]:
            # Iterate through future periods to see if a RETURN_FROM_ABSCONSION occurs
            # before an ABSCONSION.
            if (
                period.admission_reason
                == StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
            ):
                break
            if (
                period.termination_reason
                == StateSupervisionPeriodTerminationReason.ABSCONSION
            ):
                return False
        else:
            # Did not find a RETURN_FROM_ABSCONSION
            return False

        for i in reversed(range(supervision_period_list_index)):
            # Upon finding a future RETURN_FROM_ABSCONSION (if no ABSCONSION
            # periods are reached first), iterate backwards through past periods
            # to see if there is an ABSCONSION that is more recent than a
            # RETURN_FROM_ABSCONSION.
            if (
                sorted_supervision_periods[i].termination_reason
                == StateSupervisionPeriodTerminationReason.ABSCONSION
            ):
                return True
            if (
                sorted_supervision_periods[i].admission_reason
                == StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
            ):
                return False

        # If we reach this point, there is a future RETURN_FROM_ABSCONSION (which occurs
        # before a future ABSCONSION), but no ABSCONSION in the past.
        return False

    def _get_new_period_time_spans(
        self,
        supervision_periods: List[StateSupervisionPeriod],
        sentence_supervision_type_spans: List[SupervisionTypeSpan],
    ) -> List[PotentiallyOpenDateRange]:
        """From both the periods and the supervision type spans, obtains all of the
        critical dates and therefore the time spans that will form the new periods."""
        # First find all of the critical dates from all of the sentences and all of the
        # periods.
        has_null_end_date = False
        critical_date_set: Set[date] = set()
        for period in supervision_periods:
            if period.start_date_inclusive:
                critical_date_set.add(period.start_date_inclusive)
            if period.end_date_exclusive:
                critical_date_set.add(period.end_date_exclusive)
            else:
                has_null_end_date = True

        for supervision_type_span in sentence_supervision_type_spans:
            critical_date_set.add(supervision_type_span.start_date)
            if supervision_type_span.end_date:
                critical_date_set.add(supervision_type_span.end_date)
            else:
                has_null_end_date = True

        return convert_critical_dates_to_time_spans(
            critical_date_set, has_null_end_date
        )

    def _get_periods_by_critical_date(
        self, time_periods: List[DurationMixinT], critical_dates: List[date]
    ) -> Dict[date, List[DurationMixinT]]:
        """Obtain the supervision periods that overlap with the critical date as
        a dictionary keyed by date."""
        result = defaultdict(list)
        for critical_date in critical_dates:
            for period in time_periods:
                if period.duration.contains_day(critical_date):
                    result[critical_date].append(period)
        return result

    def _get_period_supervision_type(
        self,
        new_time_span: PotentiallyOpenDateRange,
        supervision_type_spans_for_start_date: Dict[date, List[SupervisionTypeSpan]],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Obtain the supervision period type given the relevant type spans."""
        relevant_type_spans = supervision_type_spans_for_start_date[
            new_time_span.lower_bound_inclusive_date
        ]

        # There is no set supervision type for the period if there are no relevant
        # supervision type spans associated with the start date.
        if not relevant_type_spans:
            return None

        return sentence_supervision_types_to_supervision_period_supervision_type(
            {
                supervision_type_span.supervision_type
                for supervision_type_span in relevant_type_spans
            }
        )

    def _get_statuses_raw_text_for_date(
        self,
        all_statuses_by_date: Dict[date, Set[UsMoSentenceStatus]],
        status_date: date,
    ) -> str:
        """Obtain the ingest-style raw text with the statuses to populate the
        admission/termination reason raw text fields."""
        statuses = sorted(
            set(
                day_status.status_code
                for day_status in all_statuses_by_date[status_date]
            )
        )
        return (
            ",".join(statuses)
            if statuses
            else StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE.value
        )
