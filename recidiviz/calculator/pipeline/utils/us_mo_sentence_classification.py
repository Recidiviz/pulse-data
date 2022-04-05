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
from datetime import date
from enum import Enum, auto
from typing import Optional, Dict, List, Any, Generic

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence, StateSupervisionSentence, \
    SentenceType


class SupervisionTypeCriticalStatusCategory(Enum):
    SUPERVISION_START = auto()
    SUPERVISION_END = auto()
    # For certain charges (sex offenders), people are sentenced to lifetime supervision via electronic monitoring. These
    # statuses indicate a start to that period of supervision once the other (often incarceration) portions of the
    # sentence have been served.
    LIFETIME_SUPERVISION_START = auto()
    SENTENCE_COMPLETION = auto()


@attr.s(frozen=True)
class UsMoSentenceStatus(BuildableAttr):
    """Representation of MO statuses in the TAK026 table, with helpers to provide more info about this status."""

    # Unique id for this sentence status object
    sentence_status_external_id: str = attr.ib()

    # External id for the sentence associated with this status
    sentence_external_id = attr.ib()

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
        return self.sentence_external_id.split('-')[0]

    # If True, this is a status that denotes a start to some period of supervision related to this sentence.
    # Note: these are not always present when a period of supervision starts - sometimes only an 'incarceration out'
    # status is the only status present to indicate a transition from incarceration to supervision.
    is_supervision_in_status: bool = attr.ib()

    @is_supervision_in_status.default
    def _get_is_supervision_in_status(self):
        return '5I' in self.status_code

    # If True, this is a status that denotes an end to some period of supervision related to this sentence.
    # Note: these are not always present when a period of supervision ends - sometimes only an 'incarceration in'
    # status is the only status present to indicate a transition from supervision to incarceration.
    is_supervision_out_status: bool = attr.ib()

    @is_supervision_out_status.default
    def _get_is_supervision_out_status(self):
        return '5O' in self.status_code

    # If True, this is a status that denotes a start to some period of incarceration related to this sentence.
    # Note: these are not always present when a period of incarceration starts - sometimes only a 'supervision out'
    # status is the only status present to indicate a transition from supervision to incarceration.
    is_incarceration_in_status: bool = attr.ib()

    @is_incarceration_in_status.default
    def _get_is_incarceration_in_status(self):
        return '0I' in self.status_code

    # If True, this is a status that denotes an end to some period of incarceration related to this sentence.
    # Note: these are not always present when a period of incarceration ends - sometimes only a 'supervision in'
    # status is the only status present to indicate a transition from incarceration to supervision.
    is_incarceration_out_status: bool = attr.ib()

    @is_incarceration_out_status.default
    def _get_is_incarceration_out_status(self):
        return '0O' in self.status_code

    # Indicates whether the status is related to the start or end of a sentencing investigation/assessment
    is_investigation_status: bool = attr.ib()

    @is_investigation_status.default
    def _get_is_investigation_status(self) -> bool:
        return self.status_code.startswith('05I5') or \
               self.status_code.startswith('25I5') or \
               self.status_code.startswith('35I5') or \
               self.status_code.startswith('95O5')

    # Indicates that this is a status that marks the beginning of a period of lifetime supervision (usually implemented
    # as electronic monitoring).
    is_lifetime_supervision_start_status: bool = attr.ib()

    @is_lifetime_supervision_start_status.default
    def _get_is_lifetime_supervision_start_status(self):
        return self.status_code in (
            '35I6010',  # Release from DMH for SVP Supv
            '35I6020',  # Lifetime Supervision Revisit
            '40O6010',  # Release for SVP Commit Hearing
            '40O6020',  # Release for Lifetime Supv

            # These indicate a transition to lifetime supervision (electronic monitoring). They are still serving
            # the sentence in this case.
            '90O1070',  # Director's Rel Comp-Life Supv
            '95O1020',  # Court Prob Comp-Lifetime Supv
            '95O2060'   # Parole / CR Comp-Lifetime Supv
        )

    # Statuses that start with '9' are often though not exclusively used to indicate that a sentence has been
    # terminated. See |is_sentence_termimination_status| for statuses that actually count towards a sentence
    # termination.
    is_sentence_termination_status_candidate: bool = attr.ib()

    @is_sentence_termination_status_candidate.default
    def _get_is_sentence_termination_status_candidate(self) -> bool:
        return self.status_code.startswith('9')

    # If True, the presence of this status in association with a sentence means the sentence has been terminated and
    # this person is no longer serving it. This includes both successful completions and unsuccessful completions (e.g.
    # a probation revocation that starts a different sentence).
    is_sentence_termimination_status: bool = attr.ib()

    @is_sentence_termimination_status.default
    def _get_is_sentence_termination_status(self) -> bool:
        return \
            self.is_sentence_termination_status_candidate and \
            not self.is_investigation_status and \
            not self.is_lifetime_supervision_start_status and \
            self.status_code not in (
                # These are usually paired with a Court Probation - Revisit. Do not mean the sentence has completed.
                '95O1040',  # Resentenced
                '95O2120'   # Prob Rev-Codes Not Applicable
            )

    # Indicates that this status is critical for determining the supervision type associated with this sentence on a
    # given date.
    is_supervision_type_critical_status: bool = attr.ib()

    @is_supervision_type_critical_status.default
    def _get_is_supervision_type_critical_status(self) -> bool:
        result = (
            self.is_supervision_in_status or
            self.is_supervision_out_status or
            self.is_incarceration_out_status or
            self.is_sentence_termimination_status
        ) and not self.is_investigation_status
        return result

    # A classification that helps us understand the supervision type at a given point in time, or None if
    # |is_supervision_type_critical_status| is False.
    supervision_type_critical_status_category: Optional[SupervisionTypeCriticalStatusCategory] = attr.ib()

    @supervision_type_critical_status_category.default
    def _get_supervision_type_critical_status_category(self) -> Optional[SupervisionTypeCriticalStatusCategory]:
        if not self.is_supervision_type_critical_status:
            return None

        if self.is_sentence_termimination_status:
            return SupervisionTypeCriticalStatusCategory.SENTENCE_COMPLETION
        if self.is_lifetime_supervision_start_status:
            return SupervisionTypeCriticalStatusCategory.LIFETIME_SUPERVISION_START
        if self.is_supervision_out_status:
            return SupervisionTypeCriticalStatusCategory.SUPERVISION_END
        if self.is_supervision_in_status or self.is_incarceration_out_status:
            return SupervisionTypeCriticalStatusCategory.SUPERVISION_START

        raise ValueError(f'Unexpected critical sentence type status [{self.status_code} - {self.status_description}]')

    # For a status that is found to be the most recent critical status when determining supervision type, this tells us
    # what supervision type should be associated with this sentence.
    supervision_type_status_classification: Optional[StateSupervisionType] = attr.ib()

    @supervision_type_status_classification.default
    def _supervision_type_status_classification(self) -> StateSupervisionType:
        """Calculates what supervision type should be associated with this sentence if this status that is found to be
        the most recent critical status for determining supervision type.
        """
        if self.status_code in (
                # Called parole, but MO classifies interstate 'Parole' as probation
                '35I4100',  # IS Compact-Parole-Revisit

                # The term 'Field' does not always exclusively mean probation, but in the case of interstate transfer
                # statuses, it does.
                '75I3000',  # MO Field-Interstate Returned

                # TODO(2647): Unsure if this can also happen for a parole sentence - needs confirmation with rerun
                '40O7000',   # Rel to Field-DAI Other Sent

                # TODO(2647): Might need to implement some sort of lookback to determine whether this was
                #  probation/parole
                '95O1040',  # Resentenced
        ):
            return StateSupervisionType.PROBATION

        if self.status_code in (
                # TODO(2647): For some reason MO treats this as Parole even though it's clearly a release that happens
                #  after a probation commitment for treatment - ask if this is correct.
                '40O2000',  # Prob Rev-Rel to Field-Spc Cir
        ):
            return StateSupervisionType.PAROLE

        if self.is_lifetime_supervision_start_status:
            return StateSupervisionType.PAROLE

        if 'Prob' in self.status_description:
            return StateSupervisionType.PROBATION
        if 'Court Parole' in self.status_description:
            # Confirmed from MO that 'Court Parole' should be treated as a probation sentence
            return StateSupervisionType.PROBATION
        if 'Diversion Sup' in self.status_description:
            return StateSupervisionType.PROBATION
        if 'Parole' in self.status_description:
            return StateSupervisionType.PAROLE
        if 'Board' in self.status_description:
            return StateSupervisionType.PAROLE
        if 'Conditional Release' in self.status_description:
            return StateSupervisionType.PAROLE

        return StateSupervisionType.INTERNAL_UNKNOWN


@attr.s(frozen=True)
class _SupervisionTypeSpan:
    # Sentence supervision type to associate with this time span
    supervision_type: StateSupervisionType = attr.ib()

    # First day where the sentence has the given supervision type, inclusive
    start_date: date = attr.ib()

    # Last day where the sentence has the given supervision type, exclusive. None if the sentence has that status up
    # until present day.
    end_date: Optional[date] = attr.ib()


@attr.s
class UsMoSentenceMixin(Generic[SentenceType]):
    """State-specific extension of sentence classes for MO which allows us to calculate additional info based on the
    MO sentence statuses.
    """

    base_sentence: SentenceType = attr.ib()

    @base_sentence.default
    def _base_sentence(self):
        raise ValueError('Must set base_sentence')

    sentence_statuses: List[UsMoSentenceStatus] = attr.ib()

    @sentence_statuses.default
    def _sentence_statuses(self):
        raise ValueError('Must set sentence_statuses')

    # Time span objects that represent time spans where a sentence has a given supervision type
    supervision_type_spans: List[_SupervisionTypeSpan] = attr.ib()

    @supervision_type_spans.default
    def _get_supervision_type_spans(self) -> List[_SupervisionTypeSpan]:
        """Generates the time span objects representing the supervision type for this sentence between certain critical
        dates where the type may have changed.
        """
        if not self.base_sentence.external_id:
            return []

        if not self.sentence_statuses:
            logging.warning('No sentence statuses in the reftable for sentence [%s]', self.base_sentence.external_id)
            return []

        all_critical_statuses = [status for status in self.sentence_statuses
                                 if status.is_supervision_type_critical_status]
        critical_statuses_by_day = defaultdict(list)

        for s in all_critical_statuses:
            critical_statuses_by_day[s.status_date].append(s)

        critical_days = sorted(critical_statuses_by_day.keys())

        supervision_type_spans = []
        for i, critical_day in enumerate(critical_days):
            if critical_day is None:
                continue
            start_date = critical_day
            end_date = critical_days[i+1] if i < len(critical_days) - 1 else None

            supervision_type = \
                self._get_sentence_supervision_type_from_critical_day_statuses(critical_statuses_by_day[critical_day])
            supervision_type_spans.append(_SupervisionTypeSpan(start_date=start_date,
                                                               end_date=end_date,
                                                               supervision_type=supervision_type))

        return supervision_type_spans

    @staticmethod
    def _get_sentence_supervision_type_from_critical_day_statuses(critical_day_statuses: List[UsMoSentenceStatus]):
        """Given a set of 'supervision type critical' statuses, returns the supervision type for the
        _SupervisionTypeSpan starting on that day."""
        status_categories_on_day = {
            status.supervision_type_critical_status_category for status in critical_day_statuses}

        has_supv_start_status = \
            SupervisionTypeCriticalStatusCategory.SUPERVISION_START in status_categories_on_day
        has_supv_end_status = \
            SupervisionTypeCriticalStatusCategory.SUPERVISION_END in status_categories_on_day
        has_sentence_completion_status = \
            SupervisionTypeCriticalStatusCategory.SENTENCE_COMPLETION in status_categories_on_day
        has_lifetime_supervision_start_status = \
            SupervisionTypeCriticalStatusCategory.LIFETIME_SUPERVISION_START in status_categories_on_day

        if has_sentence_completion_status and not has_lifetime_supervision_start_status:
            # This sentence has been completed by this date
            return None

        if has_supv_end_status and not has_supv_start_status:
            # If we can assume that the date we're inspecting is a date that the person is not incarcerated,
            # this seems to happen when a sentence is revoked and ended completely (a new sentence will be
            # started in this case) or when a probation sentence is suspended.
            return None

        if not has_supv_start_status and not has_lifetime_supervision_start_status:
            raise ValueError(f'Expect only supervision start statuses at this point. '
                             f'Person: {critical_day_statuses[0].person_external_id},'
                             f'Sentence: {critical_day_statuses[0].sentence_external_id},'
                             f'Critical status date: {critical_day_statuses[0].status_date}')

        critical_day_statuses_filtered = \
            [s for s in critical_day_statuses if not s.is_sentence_termination_status_candidate]

        if not critical_day_statuses_filtered:
            logging.warning('Found no most_recent_critical_day_statuses_filtered, most_recent_critical_day_statuses: '
                            '[%s]. Person: [%s], Sentence: [%s], Critical status date: [%s]}',
                            critical_day_statuses,
                            critical_day_statuses[0].person_external_id,
                            critical_day_statuses[0].sentence_external_id,
                            critical_day_statuses[0].status_date)
            return critical_day_statuses[0].supervision_type_status_classification

        if len(critical_day_statuses_filtered) > 1:
            logging.warning('Should only have one status, found [%s]. '
                            'Person: [%s], Sentence: [%s], Critical status date: [%s]}',
                            len(critical_day_statuses_filtered),
                            critical_day_statuses_filtered[0].person_external_id,
                            critical_day_statuses_filtered[0].sentence_external_id,
                            critical_day_statuses_filtered[0].status_date)

        return critical_day_statuses_filtered[0].supervision_type_status_classification

    def get_sentence_supervision_type_on_day(
            self,
            supervision_type_day: date
    ) -> Optional[StateSupervisionType]:
        """Calculates the supervision type to be associated with this sentence on a given day, or None if the sentence
        has been completed/terminated, if the person is incarcerated on this date, or if there are no statuses for this
        person on/before a given date.
        """

        filtered_spans = [span for span in self.supervision_type_spans
                          if span.start_date <= supervision_type_day and
                          (span.end_date is None or supervision_type_day < span.end_date)]

        if not filtered_spans:
            return None

        if len(filtered_spans) > 1:
            raise ValueError("Should have non-overlapping supervision type spans")

        return filtered_spans[0].supervision_type


@attr.s
class UsMoIncarcerationSentence(StateIncarcerationSentence, UsMoSentenceMixin[StateIncarcerationSentence]):

    @classmethod
    def from_incarceration_sentence(cls,
                                    sentence: StateIncarcerationSentence,
                                    sentence_statuses_raw: List[Dict[str, Any]],
                                    subclass_args: Optional[Dict[str, Any]] = None) -> 'UsMoIncarcerationSentence':
        subclass_args = subclass_args if subclass_args else {}
        sentence_statuses_converted = [UsMoSentenceStatus.build_from_dictionary(status_dict_raw)
                                       for status_dict_raw in sentence_statuses_raw]

        sentence_dict = {
            **sentence.__dict__,
            'base_sentence': sentence,
            'sentence_statuses': sentence_statuses_converted,
            **subclass_args
        }

        return cls(
            **sentence_dict  # type: ignore
        )


@attr.s
class UsMoSupervisionSentence(StateSupervisionSentence, UsMoSentenceMixin[StateSupervisionSentence]):

    @classmethod
    def from_supervision_sentence(cls,
                                  sentence: StateSupervisionSentence,
                                  sentence_statuses_raw: List[Dict[str, Any]],
                                  subclass_args: Optional[Dict[str, Any]] = None) -> 'UsMoSupervisionSentence':
        subclass_args = subclass_args if subclass_args else {}
        sentence_statuses_converted = [UsMoSentenceStatus.build_from_dictionary(status_dict_raw)
                                       for status_dict_raw in sentence_statuses_raw]
        sentence_dict = {
            **sentence.__dict__,
            'base_sentence': sentence,
            'sentence_statuses': sentence_statuses_converted,
            **subclass_args
        }

        return cls(
            **sentence_dict  # type: ignore
        )
