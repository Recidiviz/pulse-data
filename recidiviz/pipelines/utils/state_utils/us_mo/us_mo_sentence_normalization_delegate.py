# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Contains US_MO implementation of the StateSpecificSentenceNormalizationDelegate."""
from more_itertools import first

from recidiviz.common.constants.state.state_charge import (
    StateChargeV2ClassificationType,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceType
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
)
from recidiviz.pipelines.ingest.activity.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.ingest.activity.state.normalization.utils import (
    get_min_max_fields,
)
from recidiviz.utils.types import assert_type

# Note: the way MO sentences are ingested means that each sentence can only ever have
# a single charge associated with it. Therefore, sentence.charges[0] is often used (without
# regard for the ordering of sentence.charges) in this file to get the single charge associated with a sentence.


def _get_candidates_by_classification_type(
    sentences: list[NormalizedStateSentence],
) -> list[NormalizedStateSentence]:
    """Given a list of sentences, returns the subset of those sentences with the most severe
    classification type in that list. Therefore, if the list contains any sentences with
    felony charges, then only those sentences will be returned. This is useful when ranking
    charge severity within sentence groups containing both misdemeanors and felonies, since
    (for example) B felonies are more severe than A misdemeanors."""
    felony_sentences = [
        sentence
        for sentence in sentences
        if sentence.charges[0].classification_type
        == StateChargeV2ClassificationType.FELONY
    ]
    if len(felony_sentences) > 0:
        return felony_sentences
    misd_sentences = [
        sentence
        for sentence in sentences
        if sentence.charges[0].classification_type
        == StateChargeV2ClassificationType.MISDEMEANOR
    ]
    if len(misd_sentences) > 0:
        return misd_sentences
    return sentences


def _classification_subtype_severity(
    sentence: NormalizedStateSentence,
) -> int:
    """Assign an integer severity ranking to each charge classification subtype."""
    charge = sentence.charges[0]

    # Classification subtypes should always be a single letter or null. Charges with a classification
    # subtype of null, U, O, and N are all ranked at the same severity level, since they don't have
    # a known classification subtype.
    classification_subtype_letter = (
        charge.classification_subtype[0]
        if charge.classification_subtype
        and charge.classification_subtype not in ["O", "N"]
        else "U"
    )

    # This will return -65 for A, -66 for B, and so on, so that the severity ranking places A > B > C > D > E > U.
    return 0 - ord(classification_subtype_letter)


def _sort_sentences_by_severity(
    sentences: list[NormalizedStateSentence],
) -> list[NormalizedStateSentence]:
    """
    Sorts the list of sentences by their severity (in priority order):
    ------------------------------------------------------------------------------------
    THIS IS THE ONLY STEP ADDED IN THE MO IMPLEMENTATION OF get_most_severe_charge
    ------------------------------------------------------------------------------------
        - A sentence with the most severe OR UNCLASSIFIED charge classification subtype is more severe than other sentences.
    ------------------------------------------------------------------------------------
    THE REST OF THIS SORTING PROCEDURE IS IDENTICAL TO THE STATE-AGNOSTIC IMPLEMENTATION OF get_most_severe_charge,
    EXCEPT THAT 1) THE CONTROLLING CHARGE STEP IS SKIPPED SINCE IT DOESN'T APPLY IN MO AND 2) THE VIOLENT/SEX OFFENSE
    STEP IS SLIGHTLY MODIFIED TO ACCOUNT FOR CHARGES WITH BOTH OFFENSE TYPES
    ------------------------------------------------------------------------------------
        - A state prison sentence is more severe than other types
        - A life sentence is more severe than non-life sentences
        - The sentence with the longest initial sentence length (at imposition) is more severe than other sentences,
          with sentence length determined by the first non-null date field in this order:
              - sentence length days max
              - sentence length days min
              - projected completion date max external (calculated from sentence serving start date)
              - projected completion date min external (calculated from sentence serving start date)
        - A sentence linked to at least 1 charge classified as a violent offense OR a sex offense is more severe (modified to rank charges with both offense types the highest)
        - If all prior attributes are equal (or null) than the sentence with the highest sentence_id is the most severe
    """
    most_severe_subtype_in_group = max(
        _classification_subtype_severity(s) for s in sentences
    )

    def _sort_key(
        sentence: NormalizedStateSentence,
    ) -> tuple[int, int, int, int, int, int]:
        sentence_charge = sentence.charges[0]

        # This will be True if the sentence has the highest-severity classification subtype in its group.
        # Note that we'll use _get_candidates_by_classification_type before sorting, so if any felonies
        # are in the group, this will only look at the classification subtype for other felonies in the
        # group, ignoring misdemeanors.
        has_most_severe_subtype_in_group = (
            _classification_subtype_severity(sentence) == most_severe_subtype_in_group
        )

        # Rank sentences with has_most_severe_subtype_in_group == True or classification_subtype == "U"
        # above other sentences. This means that if a sentence group has A, B, and U felonies, the B
        # felony will never be ranked the most severe, and either the A or U felony will be ranked
        # most severe, depending on the other details of those sentences.
        charge_classification_subtype_priority = (
            0
            if has_most_severe_subtype_in_group
            or sentence_charge.classification_subtype == "U"
            else -1
        )

        # The rest of this is taken directly from the sentencing normalization manager's _sort_sentences_by_severity function.
        sentence_type_priority = (
            0 if sentence.sentence_type == StateSentenceType.STATE_PRISON else -1
        )
        life_sentence_priority = 0 if sentence.is_life else -1

        # Rank charges that are both violent and sex offenses the highest, then charges that are
        # either violent or sex offenses, then charges that are neither.
        if sentence_charge.is_violent and sentence_charge.is_sex_offense:
            violent_or_sex_offense_priority = 1
        elif sentence_charge.is_violent or sentence_charge.is_sex_offense:
            violent_or_sex_offense_priority = 0
        else:
            violent_or_sex_offense_priority = -1

        first_sentence_length = first(
            sorted(sentence.sentence_lengths, key=lambda sl: sl.length_update_datetime),
            default=None,
        )
        # Return -1 for the length priority value if there is no sentence length data for this sentence
        if not first_sentence_length:
            return (
                charge_classification_subtype_priority,
                sentence_type_priority,
                life_sentence_priority,
                -1,
                violent_or_sex_offense_priority,
                sentence.sentence_id,
            )
        min_length_days, max_length_days = get_min_max_fields(
            first_sentence_length.sentence_length_days_min,
            first_sentence_length.sentence_length_days_max,
        )
        # Try to compute a sentence length days value from the projected completion dates
        # and sentence serving start date if there is no min/max sentence length days data
        if (min_length_days is None) & (max_length_days is None):
            # Get the sentence start date from the first SERVING status
            sentence_serving_start_date = first(
                sorted(
                    [
                        sentence_status
                        for sentence_status in sentence.sentence_status_snapshots
                        if sentence_status.status.is_considered_serving_status
                    ],
                    key=lambda s: s.status_update_datetime,
                ),
                default=None,
            )
            projected_completion_date = (
                first_sentence_length.projected_completion_date_max_external
                or first_sentence_length.projected_completion_date_min_external
            )
            if (sentence_serving_start_date is not None) and (
                projected_completion_date is not None
            ):
                max_length_days = (
                    projected_completion_date
                    - sentence_serving_start_date.status_update_datetime.date()
                ).days
        return (
            charge_classification_subtype_priority,
            sentence_type_priority,
            life_sentence_priority,
            assert_type(max_length_days or min_length_days or -1, int),
            violent_or_sex_offense_priority,
            sentence.sentence_id,
        )

    return list(sorted(sentences, key=_sort_key, reverse=True))


class UsMoSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_MO implementation of the StateSpecificSentenceNormalizationDelegate."""

    @property
    def correct_early_completed_statuses(self) -> bool:
        """
        MO often has StateSentence entities with multiple COMPLETED StateSentenceStatusSnapshot
        We change any non-final COMPLETED status snapshot to SERVING.
        """
        return True

    @staticmethod
    def get_most_severe_charge(
        sentences: list[NormalizedStateSentence],
    ) -> NormalizedStateChargeV2:
        """
        Returns the most severe charge for this group of sentences.
        """
        # A misdemeanor will never be more severe than a felony, and other non-felony classes (e.g. civil)
        # will never be more severe than a misdemeanor, so we'll only need to pass a subset of sentences
        # into _sort_sentences_by_severity.
        candidate_sentences = _get_candidates_by_classification_type(sentences)

        most_severe_sentence = first(_sort_sentences_by_severity(candidate_sentences))

        return most_severe_sentence.charges[0]
