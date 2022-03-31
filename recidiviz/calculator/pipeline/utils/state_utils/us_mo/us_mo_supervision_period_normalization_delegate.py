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
import itertools
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Set, Tuple

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    copy_entities_and_add_unique_ids,
    update_normalized_entity_with_globally_unique_id,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
    UsMoSentenceMixin,
    UsMoSentenceStatus,
)
from recidiviz.calculator.pipeline.utils.supervision_type_identification import (
    sentence_supervision_types_to_supervision_period_supervision_type,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DurationMixinT
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.direct.regions.us_mo.us_mo_legacy_enum_helpers import (
    supervision_period_admission_reason_mapper,
    supervision_period_termination_reason_mapper,
)
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


class UsMoSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_MO implementation of the supervision normalization delegate"""

    def normalization_relies_on_sentences(self) -> bool:
        """In US_MO, sentences are used to pre-process StateSupervisionPeriods."""
        return True

    def split_periods_based_on_sentences(
        self,
        person_id: int,
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_sentences: Optional[List[StateIncarcerationSentence]],
        supervision_sentences: Optional[List[StateSupervisionSentence]],
    ) -> List[StateSupervisionPeriod]:
        """Generates supervision periods based on sentences and critical statuses.
        This assumes that the input supervision periods are already chronologically
        sorted."""
        if incarceration_sentences is None or supervision_sentences is None:
            raise ValueError(
                f"Sentences are required for US_MO SP normalization."
                f"Found incarceration_sentences: {incarceration_sentences}, "
                f"supervision_sentences: {supervision_sentences}"
            )

        sentences = itertools.chain(incarceration_sentences, supervision_sentences)
        supervision_type_spans: List[SupervisionTypeSpan] = []
        for sentence in sentences:
            if not isinstance(sentence, UsMoSentenceMixin):
                raise ValueError(
                    f"Unexpected type for sentence: {type(sentence)}. "
                    f"Found sentence: {sentence}."
                )
            for supervision_type_span in sentence.supervision_type_spans:
                supervision_type_spans.append(supervision_type_span)

        time_spans: List[Tuple[date, Optional[date]]] = self._get_new_period_time_spans(
            supervision_periods, supervision_type_spans
        )

        start_dates: List[date] = sorted(start_date for start_date, _ in time_spans)
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
        critical_statuses_by_date: Dict[
            date, Set[UsMoSentenceStatus]
        ] = self._get_critical_statuses_by_date(supervision_type_spans)

        new_supervision_periods: List[StateSupervisionPeriod] = []
        for time_span in time_spans:
            start_date, end_date = time_span
            period_supervision_type = self._get_period_supervision_type(
                time_span, type_spans_by_start_date
            )
            # If the supervision type is None, this person should not be counted towards
            # the supervision population and we skip this period.
            if period_supervision_type is None:
                continue

            admission_reason_raw_text = self._get_statuses_raw_text_for_date(
                critical_statuses_by_date, start_date
            )
            admission_reason = supervision_period_admission_reason_mapper(
                normalize(admission_reason_raw_text, remove_punctuation=True)
            )
            termination_reason_raw_text = (
                self._get_statuses_raw_text_for_date(
                    critical_statuses_by_date, end_date
                )
                if end_date
                else None
            )
            termination_reason = (
                supervision_period_termination_reason_mapper(
                    normalize(termination_reason_raw_text, remove_punctuation=True)
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
            case_type_entries: List[StateSupervisionCaseTypeEntry] = []

            if relevant_period:
                case_type_entries = copy_entities_and_add_unique_ids(
                    person_id=person_id, entities=relevant_period.case_type_entries
                )

            new_supervision_period = StateSupervisionPeriod(
                state_code=StateCode.US_MO.value,
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
            )

            # Add a unique id to the new SP
            update_normalized_entity_with_globally_unique_id(
                person_id, new_supervision_period
            )

            new_supervision_period = deep_entity_update(
                original_entity=new_supervision_period,
                case_type_entries=case_type_entries,
            )

            new_supervision_periods.append(new_supervision_period)

        return new_supervision_periods

    def _get_new_period_time_spans(
        self,
        supervision_periods: List[StateSupervisionPeriod],
        sentence_supervision_type_spans: List[SupervisionTypeSpan],
    ) -> List[Tuple[date, Optional[date]]]:
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

        critical_dates = sorted(list(critical_date_set))

        # Next, generate all time spans relevant to all critical dates
        time_spans: List[Tuple[date, Optional[date]]] = []
        for index, _ in enumerate(critical_dates):
            if index == len(critical_dates) - 1 and has_null_end_date:
                time_spans.append((critical_dates[index], None))
            elif index < len(critical_dates) - 1:
                time_spans.append((critical_dates[index], critical_dates[index + 1]))

        return time_spans

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

    def _get_critical_statuses_by_date(
        self,
        supervision_type_spans: List[SupervisionTypeSpan],
    ) -> Dict[date, Set[UsMoSentenceStatus]]:
        """Obtain both the start and optional end critical statuses and build a
        dictionary of statuses keyed by date."""
        critical_statuses_by_date: Dict[date, Set[UsMoSentenceStatus]] = defaultdict(
            set
        )
        for supervision_type_span in supervision_type_spans:
            critical_statuses_by_date[supervision_type_span.start_date].update(
                supervision_type_span.start_critical_statuses
            )
            if supervision_type_span.end_date:
                if not supervision_type_span.end_critical_statuses:
                    raise ValueError(
                        "Expected nonnull end_critical_statuses for type span with nonnull end_date"
                    )
                critical_statuses_by_date[supervision_type_span.end_date].update(
                    supervision_type_span.end_critical_statuses
                )
        return critical_statuses_by_date

    def _get_period_supervision_type(
        self,
        new_time_span: Tuple[date, Optional[date]],
        supervision_type_spans_for_start_date: Dict[date, List[SupervisionTypeSpan]],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Obtain the supervision period type given the relevant type spans."""
        start_date, _ = new_time_span
        relevant_type_spans = supervision_type_spans_for_start_date[start_date]

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
        critical_statuses_by_date: Dict[date, Set[UsMoSentenceStatus]],
        status_date: date,
    ) -> str:
        """Obtain the ingest-style raw text with the critical statuses to populate the
        admission/termination reason raw text fields."""
        critical_statuses = sorted(
            set(
                critical_day_status.status_code
                for critical_day_status in critical_statuses_by_date[status_date]
            )
        )
        return (
            ",".join(critical_statuses)
            if critical_statuses
            else StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE.value
        )
