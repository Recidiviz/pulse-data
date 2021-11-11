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
"""Missouri-specific code for modeling sentence based on sentence statuses from table TAK026."""

import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, Generic, List, Optional, Tuple

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.date import DateRange, DurationMixin
from recidiviz.persistence.entity.state.entities import (
    SentenceType,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)


@attr.s(frozen=True)
class UsMoSentenceStatus(BuildableAttr):
    """Representation of MO statuses in the TAK026 table, with helpers to provide more info about this status."""

    # Unique id for this sentence status object
    sentence_status_external_id: str = attr.ib()

    # External id for the sentence associated with this status
    sentence_external_id: str = attr.ib()

    # Status date
    status_date: Optional[date] = attr.ib()

    # Status code like 15O1000
    status_code: str = attr.ib()

    # Human-readable status code description
    status_description: str = attr.ib()

    # MO DOC id for person associated with this sentence
    person_external_id: str = attr.ib()

    @person_external_id.default
    def _get_person_external_id(self) -> str:
        return self.sentence_external_id.split("-")[0]

    # If True, this is a status that denotes a start to some period of supervision related to this sentence.
    # Note: these are not always present when a period of supervision starts - sometimes only an 'incarceration out'
    # status is the only status present to indicate a transition from incarceration to supervision.
    is_supervision_in_status: bool = attr.ib()

    @is_supervision_in_status.default
    def _get_is_supervision_in_status(self) -> bool:
        return "5I" in self.status_code

    # If True, this is a status that denotes an end to some period of supervision related to this sentence.
    # Note: these are not always present when a period of supervision ends - sometimes only an 'incarceration in'
    # status is the only status present to indicate a transition from supervision to incarceration.
    is_supervision_out_status: bool = attr.ib()

    @is_supervision_out_status.default
    def _get_is_supervision_out_status(self) -> bool:
        return "5O" in self.status_code

    # If True, this is a status that denotes a start to some period of incarceration related to this sentence.
    # Note: these are not always present when a period of incarceration starts - sometimes only a 'supervision out'
    # status is the only status present to indicate a transition from supervision to incarceration.
    is_incarceration_in_status: bool = attr.ib()

    @is_incarceration_in_status.default
    def _get_is_incarceration_in_status(self) -> bool:
        return "0I" in self.status_code

    # If True, this is a status that denotes an end to some period of incarceration related to this sentence.
    # Note: these are not always present when a period of incarceration ends - sometimes only a 'supervision in'
    # status is the only status present to indicate a transition from incarceration to supervision.
    is_incarceration_out_status: bool = attr.ib()

    @is_incarceration_out_status.default
    def _get_is_incarceration_out_status(self) -> bool:
        return "0O" in self.status_code

    # Indicates whether the status is related to the start or end of a sentencing investigation/assessment
    is_investigation_status: bool = attr.ib()

    @is_investigation_status.default
    def _get_is_investigation_status(self) -> bool:
        return (
            self.status_code.startswith("05I5")
            or self.status_code.startswith("25I5")
            or self.status_code.startswith("35I5")
            or self.status_code.startswith("95O5")
        )

    # Indicates that this is a status that marks the beginning of a period of lifetime supervision (usually implemented
    # as electronic monitoring).
    is_lifetime_supervision_start_status: bool = attr.ib()

    @is_lifetime_supervision_start_status.default
    def _get_is_lifetime_supervision_start_status(self) -> bool:
        return self.status_code in (
            "35I6010",  # Release from DMH for SVP Supv
            "35I6020",  # Lifetime Supervision Revisit
            "40O6010",  # Release for SVP Commit Hearing
            "40O6020",  # Release for Lifetime Supv
            # These indicate a transition to lifetime supervision (electronic monitoring). They are still serving
            # the sentence in this case.
            "90O1070",  # Director's Rel Comp-Life Supv
            "95O1020",  # Court Prob Comp-Lifetime Supv
            "95O2060",  # Parole / CR Comp-Lifetime Supv
        )

    # Statuses that start with '9' are often though not exclusively used to indicate that a sentence has been
    # terminated. See |is_sentence_termimination_status| for statuses that actually count towards a sentence
    # termination.
    is_sentence_termination_status_candidate: bool = attr.ib()

    @is_sentence_termination_status_candidate.default
    def _get_is_sentence_termination_status_candidate(self) -> bool:
        return self.status_code.startswith("9")

    # If True, the presence of this status in association with a sentence means the sentence has been terminated and
    # this person is no longer serving it. This includes both successful completions and unsuccessful completions (e.g.
    # a probation revocation that starts a different sentence).
    is_sentence_termimination_status: bool = attr.ib()

    @is_sentence_termimination_status.default
    def _get_is_sentence_termination_status(self) -> bool:
        return (
            self.is_sentence_termination_status_candidate
            and not self.is_investigation_status
            and not self.is_lifetime_supervision_start_status
            and self.status_code
            not in (
                # These are usually paired with a Court Probation - Revisit. Do not mean the sentence has completed.
                "95O1040",  # Resentenced
                "95O2120",  # Prob Rev-Codes Not Applicable
            )
        )

    # Indicates that this status is critical for determining the supervision type associated with this sentence on a
    # given date.
    is_supervision_type_critical_status: bool = attr.ib()

    @is_supervision_type_critical_status.default
    def _get_is_supervision_type_critical_status(self) -> bool:
        result = (
            self.is_supervision_in_status
            or self.is_supervision_out_status
            or self.is_incarceration_out_status
            or self.is_sentence_termimination_status
        ) and not self.is_investigation_status
        return result

    # For a status that is found to be the most recent critical status when determining supervision type, this tells us
    # what supervision type should be associated with this sentence.
    supervision_type_status_classification: Optional[
        StateSupervisionSentenceSupervisionType
    ] = attr.ib()

    @supervision_type_status_classification.default
    def _supervision_type_status_classification(
        self,
    ) -> Optional[StateSupervisionSentenceSupervisionType]:
        """Calculates what supervision type should be associated with this sentence if this status that is found to be
        the most recent critical status for determining supervision type.
        """
        if (
            self.is_incarceration_in_status
            or self.is_supervision_out_status
            or self.is_sentence_termimination_status
        ):
            return None

        if self.status_code in (
            # Called parole, but MO classifies interstate 'Parole' as probation
            "35I4100",  # IS Compact-Parole-Revisit
            "40O7400",  # IS Compact Parole to Missouri
            # The term 'Field' does not always exclusively mean probation, but in the case of interstate transfer
            # statuses, it does.
            "75I3000",  # MO Field-Interstate Returned
        ):
            return StateSupervisionSentenceSupervisionType.PROBATION

        if self.status_code in (
            # In July 2008, MO transitioned people in CRC transitional facilities from the control of the DAI
            # (incarceration) to the parole board. If we see this status it means someone is on parole.
            "40O6000"  # Converted-CRC DAI to CRC Field
        ):
            return StateSupervisionSentenceSupervisionType.PAROLE

        if self.is_lifetime_supervision_start_status:
            return StateSupervisionSentenceSupervisionType.PAROLE

        if "Prob" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PROBATION
        if "Court Parole" in self.status_description:
            # Confirmed from MO that 'Court Parole' should be treated as a probation sentence
            return StateSupervisionSentenceSupervisionType.PROBATION
        if "Diversion Sup" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PROBATION
        if "Parole" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PAROLE
        if "Board" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PAROLE
        if "Conditional Release" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PAROLE
        if "CR " in self.status_description:
            # CR stands for Conditional Release
            return StateSupervisionSentenceSupervisionType.PAROLE

        return StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN


@attr.s(frozen=True)
class SupervisionTypeSpan(DurationMixin):
    """Represents a duration in which there is a specific supervision type associated
    derived from critical statuses and sentences."""

    # Sentence supervision type to associate with this time span, or None if the person was not on supervision at this
    # time.
    supervision_type: Optional[StateSupervisionSentenceSupervisionType] = attr.ib()

    # First day where the sentence has the given supervision type, inclusive
    start_date: date = attr.ib()

    # Last day where the sentence has the given supervision type, exclusive. None if the sentence has that status up
    # until present day.
    end_date: Optional[date] = attr.ib()

    # Critical sentence statuses associated with the supervision_type start date
    start_critical_statuses: List[UsMoSentenceStatus] = attr.ib()

    # Critical sentence statuses associated with the supervision_type end date
    end_critical_statuses: Optional[List[UsMoSentenceStatus]] = attr.ib()

    @property
    def duration(self) -> DateRange:
        return DateRange.from_maybe_open_range(
            start_date=self.start_date, end_date=self.end_date
        )

    @property
    def start_date_inclusive(self) -> Optional[date]:
        return self.start_date

    @property
    def end_date_exclusive(self) -> Optional[date]:
        return self.end_date


@attr.s
class UsMoSentenceMixin(Generic[SentenceType]):
    """State-specific extension of sentence classes for MO which allows us to calculate additional info based on the
    MO sentence statuses.
    """

    base_sentence: SentenceType = attr.ib()

    @base_sentence.default
    def _base_sentence(self) -> SentenceType:
        raise ValueError("Must set base_sentence")

    sentence_statuses: List[UsMoSentenceStatus] = attr.ib()

    @sentence_statuses.default
    def _sentence_statuses(self) -> List[UsMoSentenceStatus]:
        raise ValueError("Must set sentence_statuses")

    # Time span objects that represent time spans where a sentence has a given supervision type
    supervision_type_spans: List[SupervisionTypeSpan] = attr.ib()

    @supervision_type_spans.default
    def _get_supervision_type_spans(self) -> List[SupervisionTypeSpan]:
        """Generates the time span objects representing the supervision type for this sentence between certain critical
        dates where the type may have changed.
        """
        if not self.base_sentence.external_id:
            return []

        if not self.sentence_statuses:
            logging.warning(
                "No sentence statuses in the reftable for sentence [%s]",
                self.base_sentence.external_id,
            )
            return []

        all_critical_statuses = [
            status
            for status in self.sentence_statuses
            if status.is_supervision_type_critical_status
        ]
        critical_statuses_by_day = defaultdict(list)

        for s in all_critical_statuses:
            if s.status_date is not None:
                critical_statuses_by_day[s.status_date].append(s)

        critical_days = sorted(critical_statuses_by_day.keys())

        supervision_type_spans = []
        for i, critical_day in enumerate(critical_days):
            start_date = critical_day
            end_date = critical_days[i + 1] if i < len(critical_days) - 1 else None

            supervision_type = (
                self._get_sentence_supervision_type_from_critical_day_statuses(
                    critical_statuses_by_day[critical_day]
                )
            )
            supervision_type_spans.append(
                SupervisionTypeSpan(
                    start_date=start_date,
                    end_date=end_date,
                    supervision_type=supervision_type,
                    start_critical_statuses=critical_statuses_by_day[start_date],
                    end_critical_statuses=critical_statuses_by_day[end_date]
                    if end_date
                    else None,
                )
            )

        return supervision_type_spans

    @supervision_type_spans.validator
    def _supervision_type_spans_validator(
        self,
        _attribute: attr.Attribute,
        supervision_type_spans: List[SupervisionTypeSpan],
    ) -> None:
        if supervision_type_spans is None:
            raise ValueError("Spans list should not be None")

        if not supervision_type_spans:
            return

        last_span = supervision_type_spans[-1]
        if last_span.end_date is not None:
            raise ValueError("Must end span list with an open span")

        for not_last_span in supervision_type_spans[:-1]:
            if not_last_span.end_date is None:
                raise ValueError("Intermediate span must not have None end date")

    @staticmethod
    def _get_sentence_supervision_type_from_critical_day_statuses(
        critical_day_statuses: List[UsMoSentenceStatus],
    ) -> Optional[StateSupervisionSentenceSupervisionType]:
        """Given a set of 'supervision type critical' statuses, returns the supervision type for the
        SupervisionTypeSpan starting on that day."""

        # Status external ids are the sentence id with the status sequence number appended - larger sequence numbers
        # should be given precedence.
        critical_day_statuses.sort(
            key=lambda s: s.sentence_status_external_id, reverse=True
        )
        supervision_type = critical_day_statuses[
            0
        ].supervision_type_status_classification

        if (
            supervision_type is None
            or supervision_type
            != StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
        ):
            return supervision_type

        # If the most recent status in a day does not give us enough information to tell the supervision type, look to
        # other statuses on that day.
        for status in critical_day_statuses:
            if (
                status.supervision_type_status_classification is not None
                and status.supervision_type_status_classification
                != StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
            ):
                return status.supervision_type_status_classification

        return supervision_type

    def _get_overlapping_supervision_type_span_index(
        self, supervision_type_day: date
    ) -> Optional[int]:
        """Returns the index of the span in this sentence's supervision_type_spans list that overlaps in time with the
        provided date, or None if there are no overlapping spans."""
        filtered_spans = [
            (i, span)
            for i, span in enumerate(self.supervision_type_spans)
            if span.start_date <= supervision_type_day
            and (span.end_date is None or supervision_type_day < span.end_date)
        ]

        if not filtered_spans:
            return None

        if len(filtered_spans) > 1:
            raise ValueError("Should have non-overlapping supervision type spans")

        return filtered_spans[0][0]

    def get_sentence_supervision_type_on_day(
        self, supervision_type_day: date
    ) -> Optional[StateSupervisionSentenceSupervisionType]:
        """Calculates the supervision type to be associated with this sentence on a given day, or None if the sentence
        has been completed/terminated, if the person is incarcerated on this date, or if there are no statuses for this
        person on/before a given date.
        """

        overlapping_span_index = self._get_overlapping_supervision_type_span_index(
            supervision_type_day
        )

        if overlapping_span_index is None:
            return None

        if self.supervision_type_spans[overlapping_span_index].supervision_type is None:
            return None

        while overlapping_span_index >= 0:
            span = self.supervision_type_spans[overlapping_span_index]
            if (
                span.supervision_type is not None
                and span.supervision_type
                != StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
            ):
                return span.supervision_type

            # If the most recent status status is INTERNAL_UNKNOWN, we look back at previous statuses until we can
            # find a status that is not INTERNAL_UNKNOWN
            overlapping_span_index -= 1

        return self.supervision_type_spans[overlapping_span_index].supervision_type

    def get_most_recent_supervision_type_before_upper_bound_day(
        self,
        upper_bound_exclusive_date: date,
        lower_bound_inclusive_date: Optional[date],
    ) -> Optional[Tuple[date, StateSupervisionSentenceSupervisionType]]:
        """Finds the most recent nonnull type associated this sentence, preceding the provided date. An optional lower
        bound may be provided to limit the lookback window.

        Returns a tuple (last valid date of that supervision type span, supervision type).
        """
        upper_bound_inclusive_day = upper_bound_exclusive_date - timedelta(days=1)
        overlapping_span_index = self._get_overlapping_supervision_type_span_index(
            upper_bound_inclusive_day
        )

        if overlapping_span_index is None:
            return None

        while overlapping_span_index >= 0:
            span = self.supervision_type_spans[overlapping_span_index]

            last_supervision_type_day_exclusive = (
                min(upper_bound_exclusive_date, span.end_date)
                if span.end_date
                else upper_bound_exclusive_date
            )
            last_supervision_type_day_inclusive = (
                last_supervision_type_day_exclusive - timedelta(days=1)
            )

            if (
                lower_bound_inclusive_date
                and last_supervision_type_day_inclusive < lower_bound_inclusive_date
            ):
                return None

            supervision_type = self.get_sentence_supervision_type_on_day(
                span.start_date
            )

            if supervision_type is not None:
                return last_supervision_type_day_inclusive, supervision_type

            overlapping_span_index -= 1

        return None


@attr.s
class UsMoIncarcerationSentence(
    StateIncarcerationSentence, UsMoSentenceMixin[StateIncarcerationSentence]
):
    @classmethod
    def from_incarceration_sentence(
        cls,
        sentence: StateIncarcerationSentence,
        sentence_statuses_raw: List[Dict[str, Any]],
        subclass_args: Optional[Dict[str, Any]] = None,
    ) -> "UsMoIncarcerationSentence":
        subclass_args = subclass_args if subclass_args else {}
        sentence_statuses_converted = [
            UsMoSentenceStatus.build_from_dictionary(status_dict_raw)
            for status_dict_raw in sentence_statuses_raw
        ]

        sentence_dict = {
            **sentence.__dict__,
            "base_sentence": sentence,
            "sentence_statuses": sentence_statuses_converted,
            **subclass_args,
        }

        return cls(**sentence_dict)  # type: ignore


@attr.s
class UsMoSupervisionSentence(
    StateSupervisionSentence, UsMoSentenceMixin[StateSupervisionSentence]
):
    @classmethod
    def from_supervision_sentence(
        cls,
        sentence: StateSupervisionSentence,
        sentence_statuses_raw: List[Dict[str, Any]],
        subclass_args: Optional[Dict[str, Any]] = None,
    ) -> "UsMoSupervisionSentence":
        subclass_args = subclass_args if subclass_args else {}
        sentence_statuses_converted = [
            UsMoSentenceStatus.build_from_dictionary(status_dict_raw)
            for status_dict_raw in sentence_statuses_raw
        ]
        sentence_dict = {
            **sentence.__dict__,
            "base_sentence": sentence,
            "sentence_statuses": sentence_statuses_converted,
            **subclass_args,
        }

        return cls(**sentence_dict)  # type: ignore
