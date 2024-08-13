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
"""Missouri-specific classes for modeling sentences based on sentence statuses from table TAK026."""
from datetime import date
from typing import Optional

import attr

from recidiviz.common.attr_converters import optional_str_to_uppercase_str
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.date import DateRange, DurationMixin


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
    # Will be an uppercase or empty str
    status_description: str = attr.ib(converter=optional_str_to_uppercase_str)

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

    # Indicates that this status is related to starting / stopping a period of
    # absconsion.
    is_absconsion_status: bool = attr.ib()

    @is_absconsion_status.default
    def _is_absconsion_status(self) -> bool:
        return self.status_code.startswith("65N95") or self.status_code in {
            "65O1010",  # Offender declared absconder - from TAK026 BW$SCD
            "65O1020",  # Offender declared absconder - from TAK026 BW$SCD
            "65O1030",  # Offender declared absconder - from TAK026 BW$SCD
            "65L9100",  # Offender declared absconder - from TAK026 BW$SCD
        }

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

    # Indicates that this status marks the end of a cycle, which is a continuous period
    # of interaction with the MODOC. This status essentially means that the person is at
    # liberty or has died.
    is_cycle_termination_status: bool = attr.ib()

    @is_cycle_termination_status.default
    def _is_cycle_termination_status(self) -> bool:
        return self.status_code.startswith("99")

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

    # If True, we believe this status is a temporary status that is added as a hint to
    # what happened, but we're not completely certain.
    # These statuses have a description that ends in '- MUST VERIFY'
    is_must_verify_status: bool = attr.ib()

    @is_must_verify_status.default
    def _get_is_must_verify_status(self) -> bool:
        return self.status_code.endswith("Z")

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

        if not self.status_description:
            return StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN

        if "PROB" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PROBATION
        if "COURT PAROLE" in self.status_description:
            # Confirmed from MO that 'Court Parole' should be treated as a probation sentence
            return StateSupervisionSentenceSupervisionType.PROBATION
        if "DIVERSION SUP" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PROBATION
        if "PAROLE" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PAROLE
        if "BOARD" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PAROLE
        if "CONDITIONAL RELEASE" in self.status_description:
            return StateSupervisionSentenceSupervisionType.PAROLE
        if "CR " in self.status_description:
            # CR stands for Conditional Release
            return StateSupervisionSentenceSupervisionType.PAROLE

        return StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN

    # These statuses designate the original sentence is to treatment
    is_treatment_status: bool = attr.ib()

    @is_treatment_status.default
    def _is_treatment_status(self) -> bool:
        """These statuses designate the original sentence is to treatment."""
        return self.status_code in {
            "10I1020",  # New Court Comm-Long Term Treat
            "10I1040",  # New Court Comm-120 Day Treat
            "10I7060",  # New Prob-Post Conv-Treat Prog
            "20I1020",  # Court Comm-Lng Tm Trt-Addl Chg
            "20I1040",  # Court Comm-120 Day Treat-Addl
            "30I1020",  # Court Comm-Lng Trm Trt-Revisit
            "30I1040",  # Court Comm-120 Day Trt-Revisit
        }

    # This is to identify the type of supervision a sentence has
    # based on the status at imposition.
    state_sentence_type: StateSentenceType = attr.ib()

    # TODO(#28189): Expand status code knowledge/coverage for supervision type classification
    @state_sentence_type.default
    def _state_sentence_type(self) -> StateSentenceType:
        """
        Determines the sentence type at imposition based on the
        earliest valid status code and status description.
        """
        if self.is_incarceration_in_status or self.is_incarceration_out_status:
            return StateSentenceType.STATE_PRISON
        if self.is_treatment_status:
            return StateSentenceType.TREATMENT
        if (
            self.supervision_type_status_classification
            == StateSupervisionSentenceSupervisionType.PROBATION
        ):
            return StateSentenceType.PROBATION
        if (
            self.supervision_type_status_classification
            == StateSupervisionSentenceSupervisionType.PAROLE
        ):
            return StateSentenceType.PAROLE
        return StateSentenceType.INTERNAL_UNKNOWN

    # This is to identify the type of SentenceStatus at its own
    # point in time in a ledger of StateSentenceStatusSnapshots
    state_sentence_status: StateSentenceStatus = attr.ib()

    @state_sentence_status.default
    def _state_sentence_status(
        self,
    ) -> StateSentenceStatus:
        """Returns the StateSentenceStatus based on the given sentence and status code."""

        if self.status_code in {
            "35I3500",  # Bond Supv-Pb Suspended-Revisit
            "65O2015",  # Court Probation Suspension
            "65O3015",  # Court Parole Suspension
            "95O3500",  # Bond Supv-Pb Susp-Completion
            "95O3505",  # Bond Supv-Pb Susp-Bond Forfeit
            "95O3600",  # Bond Supv-Pb Susp-Trm-Tech
            "95O7145",  # DATA ERROR-Suspended
        }:
            return StateSentenceStatus.SUSPENDED

        if self.status_code in {
            "90O1020",  # Institutional Commutation Comp
            "95O1025",  # Field Commutation
            "99O1020",  # Institutional Commutation
            "99O1025",  # Field Commutation
        }:
            return StateSentenceStatus.COMMUTED

        if self.is_sentence_termimination_status:
            return StateSentenceStatus.COMPLETED

        return StateSentenceStatus.SERVING

    # TODO(#30199): Remove this field once we set sequence_num directly as a param
    #  on this class.
    @property
    def sequence_num(self) -> int:
        """Parses the status sequence number from the external id."""
        parts = self.sentence_status_external_id.split("-")
        if len(parts) < 2:
            raise ValueError(
                f"Unexpected sentence status external id format: "
                f"{self.sentence_status_external_id}"
            )
        return int(parts[-1])


@attr.s(frozen=True)
class SupervisionTypeSpan(DurationMixin):
    """Represents a duration in which there is a specific supervision type associated
    derived from critical statuses and sentences."""

    # External id for the sentence associated with this status
    sentence_external_id: str = attr.ib()

    # Sentence supervision type to associate with this time span, or None if the person was not on supervision at this
    # time.
    supervision_type: Optional[StateSupervisionSentenceSupervisionType] = attr.ib()

    # First day where the sentence has the given supervision type, inclusive
    start_date: date = attr.ib()

    # Last day where the sentence has the given supervision type, exclusive. None if the sentence has that status up
    # until present day.
    end_date: Optional[date] = attr.ib()

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
