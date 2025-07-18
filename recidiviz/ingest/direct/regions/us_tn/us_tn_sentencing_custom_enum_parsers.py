#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""
This module includes helpers and utilities to ingest
StateChargeV2, StateSentence, etc. in TN

# TODO(#41687): Ingest Interstate Compact sentences!!!
"""
from enum import Enum
from typing import Optional

from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)


class TnSentenceStatus(Enum):
    """
    Enumerates the possible sentence statuses in TN.

    See TnInactiveSentenceStatusReason for the reasons a sentence can be inactive.
    """

    # The values of this enum are the string literals found in Sentence.SentenceStatus
    ACTIVE = "AC"
    COMMUNITY_CORRECTIONS = "CC"
    PROBATION = "PB"
    INACTIVE = "IN"


class TnSentencedTo(Enum):
    """
    Enumerates the possible values of SentencedTo in TN.
    Which is where a person is *serving* their sentence
    and *CHANGES* over time.
    """

    PRISON = "TD"  # TDOC
    COMMUNITY_CORRECTIONS = "CC"
    LOCAL_JAIL = "LJ"
    WORKHOUSE = "WK"  # Workhouse is a jail for short sentences.

    @classmethod
    def from_raw_text(cls, raw_text: str) -> Optional["TnSentencedTo"]:
        if raw_text.upper() == "NONE":
            return None
        return cls(raw_text.upper())


def infer_imposed_sentence_type_from_raw_text(raw_text: str) -> StateSentenceType:
    """
    Infers the imposed sentence type from the raw text.

    The raw text is a string with the following format:
    "<initial_status_str>@@<sentenced_to_str>@@<suspended_to_probation_str>"

    Where:
        - initial_status_str: The initial status of the sentence. (changes over time)
        - initial_sentenced_to_str: The intitial modality of the the sentence. (changes over time)
        - suspended_to_probation_str: A string indicating if the sentence is suspended to probation (does not change over time).
    """
    (
        inital_status_str,
        initial_sentenced_to_str,
        suspended_to_probation_str,
    ) = raw_text.split("@@")

    # We want everything to break here if we get a new value in the data.
    initial_status: TnSentenceStatus = TnSentenceStatus(inital_status_str)
    initial_sentenced_to: Optional[TnSentencedTo] = TnSentencedTo.from_raw_text(
        initial_sentenced_to_str
    )
    suspended_to_probation: bool = suspended_to_probation_str == "S"

    # The ordering of this logic reflects our level of confidence in the data.
    # We're more confident in supsended_to_probation than sentenced_to
    # and initial status given how they change over time.

    if suspended_to_probation or (initial_status == TnSentenceStatus.PROBATION):
        return StateSentenceType.PROBATION

    if (
        initial_status == TnSentenceStatus.COMMUNITY_CORRECTIONS
        or initial_sentenced_to == TnSentencedTo.COMMUNITY_CORRECTIONS
    ):
        return StateSentenceType.COMMUNITY_CORRECTIONS

    if (
        initial_status == TnSentenceStatus.ACTIVE
        or initial_sentenced_to == TnSentencedTo.PRISON
    ):
        return StateSentenceType.STATE_PRISON

    # At this point, this individual was not sentenced to prison, probation, or community corrections.
    # They also did not have a sentence suspended to probation.
    # The only remaining option is to have an "INACTIVE" earliest known status and an earliest known SentencedTo
    # value was jail or workhouse. This is most likely due to an individual completing their sentence before we
    # began ingesting data in TN.
    if initial_status == TnSentenceStatus.INACTIVE and initial_sentenced_to in {
        TnSentencedTo.LOCAL_JAIL,
        TnSentencedTo.WORKHOUSE,
    }:
        return StateSentenceType.INTERNAL_UNKNOWN

    raise ValueError(
        f"Could not determine initial sentence type from : {initial_status=}, "
        f"{initial_sentenced_to=}, "
        f"{suspended_to_probation=}"
    )


def infer_sentence_status(raw_text: str) -> StateSentenceStatus:
    """
    This function encapsulates everything we know about sentence
    statuses from TN raw data.

    It checks a few assumptions we have about the data and
    returns the appropriate StateSentenceStatus.
    """
    sentence_status, *flags = raw_text.split("@@")
    (
        death,
        pardoned,
        dismissed,
        commuted,
        expired,
        awaiting_retrial,
        court_order,
    ) = tuple(map(lambda x: x == "Y", flags))

    if death:
        return StateSentenceStatus.DEATH

    if sentence_status in {"EFFECTIVE", "AC", "PB", "CC"}:
        return StateSentenceStatus.SERVING

    if pardoned:
        return StateSentenceStatus.PARDONED

    # It seems that a court order is for terminating a
    # sentence and not suspending it. The pipeline will
    # break if a serving status follows a COMPLETED status.
    # (BUT, if we ever turn on correct_early_completed_statuses
    # in the normalization manager, this will not be the case.)
    if dismissed or expired or court_order:
        return StateSentenceStatus.COMPLETED

    if commuted:
        return StateSentenceStatus.COMMUTED

    if awaiting_retrial:
        return StateSentenceStatus.SUSPENDED

    # TODO(#44222) About 100 people have no specific Invalid reason
    # and do not have a death date.
    # Next step is to look more closely into movement reasons
    # We mark them as COMPLETED but should verify appropriate action
    # with TN
    return StateSentenceStatus.COMPLETED
