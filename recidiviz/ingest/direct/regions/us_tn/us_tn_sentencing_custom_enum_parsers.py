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

from recidiviz.common.constants.state.state_sentence import StateSentenceType


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

    See types of release associated with a sentence here:
    https://www.tn.gov/correction/cs/types-of-release.html

    The raw text is a string with the following format:
    "<initial_status_str>@@<sentenced_to_str>@@<split_confinement_str>@@<suspended_to_probation_str>"

    Where:
        - initial_status_str: The initial status of the sentence. (changes over time)
        - initial_sentenced_to_str: The intitial modality of the the sentence. (changes over time)
        - split_confinement_str: A string indicating the type of split confinement (does not change over time).
        - suspended_to_probation_str: A string indicating if the sentence is suspended to probation (does not change over time).
    """
    (
        inital_status_str,
        initial_sentenced_to_str,
        split_confinement_str,
        suspended_to_probation_str,
    ) = raw_text.split("@@")

    # We want everything to break here if we get a new value in the data.
    initial_status: TnSentenceStatus = TnSentenceStatus(inital_status_str)
    initial_sentenced_to: Optional[TnSentencedTo] = TnSentencedTo.from_raw_text(
        initial_sentenced_to_str
    )
    suspended_to_probation: bool = suspended_to_probation_str == "S"
    split_confinement: bool = split_confinement_str in {"PB", "CC"}

    # The ordering of this logic reflects our level of confidence in the data.
    # Data from Judgement Orders tend to be more reliable.

    if split_confinement:
        return StateSentenceType.SPLIT

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

    # Read about being sentenced to labor in a county workhouse:
    #     - https://www.ctas.tennessee.edu/eli/sentence-county-workhouse
    # Read about situations where a person may be sentenced to or residing in a jail:
    #     - https://www.ctas.tennessee.edu/eli/persons-confined-jail
    #     - Tenn. Code Ann. § 40-20-109
    if initial_sentenced_to in {
        TnSentencedTo.LOCAL_JAIL,
        TnSentencedTo.WORKHOUSE,
    }:
        return StateSentenceType.COUNTY_JAIL

    raise ValueError(
        f"Could not determine initial sentence type from : {initial_status=}, "
        f"{initial_sentenced_to=}, "
        f"{suspended_to_probation=}"
    )
