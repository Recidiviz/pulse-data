#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""Custom parser functions for US_TN. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_tn_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)


def parse_supervision_type(raw_text: str) -> StateSupervisionSentenceSupervisionType:
    """
    Returns the supervision type of a supervision sentence.
    """
    # TODO(#10923): Remove custom parser once multiple columns can be used to determine enum value.
    sentence_status, suspended_to_probation, sentenced_to = raw_text.split("-")
    if suspended_to_probation == "S" or sentence_status == "PB":
        return StateSupervisionSentenceSupervisionType.PROBATION
    if sentence_status == "CC" or (sentence_status == "IN" and sentenced_to == "CC"):
        return StateSupervisionSentenceSupervisionType.COMMUNITY_CORRECTIONS

    return StateSupervisionSentenceSupervisionType.EXTERNAL_UNKNOWN


def parse_sentence_status(raw_text: str) -> StateSentenceStatus:
    """
    Returns the StateSentenceStatus associated with the sentence action and sentence status columns.
    """
    # TODO(#10923): Remove custom parser once multiple columns can be used to determine enum value.
    sentence_action, sentence_status = raw_text.split("-")
    if sentence_action == "CMTA":
        return StateSentenceStatus.COMMUTED
    if sentence_action == "PARA":
        return StateSentenceStatus.PARDONED
    if sentence_action == "RLSD":
        return StateSentenceStatus.SUSPENDED
    if sentence_action in ("VRVC", "VRVP", "JRPR", "JRCC"):
        return StateSentenceStatus.REVOKED
    if sentence_status in ("AC", "CC", "PB"):
        return StateSentenceStatus.SERVING
    if sentence_status == "IN":
        return StateSentenceStatus.COMPLETED

    return StateSentenceStatus.EXTERNAL_UNKNOWN
