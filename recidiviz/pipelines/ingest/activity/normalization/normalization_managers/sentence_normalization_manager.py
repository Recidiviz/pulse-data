# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""State-specific delegate for sentence normalization in the calculation pipelines."""
from more_itertools import first

from recidiviz.common.constants.state.state_sentence import StateSentenceType
from recidiviz.persistence.entity.activity.entities import (
    StateSentence,
    StateSentenceStatusSnapshot,
)
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.ingest.activity.normalization.utils import get_min_max_fields
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.utils.types import assert_type


def _sort_sentences_by_severity(
    sentences: list[NormalizedStateSentence],
) -> list[NormalizedStateSentence]:
    # TODO(#44111): consider either deprecating or refactoring this logic so it can replace the SQL logic in `person_charge_severity_ranking`
    """
    Sorts the list of sentences by their severity (in priority order):
        - A state prison sentence is more severe than other types
        - A life sentence is more severe than non-life sentences
        - A sentence with the most controlling charges (when hydrated) is more severe than other sentences
        - The sentence with the longest initial sentence length (at imposition) is more severe than other sentences,
          with sentence length determined by the first non-null date field in this order:
              - sentence length days max
              - sentence length days min
              - projected completion date max external (calculated from sentence serving start date)
              - projected completion date min external (calculated from sentence serving start date)
        - A sentence linked to at least 1 charge classified as a violent offense OR a sex offense is more severe
        - If all prior attributes are equal (or null) than the sentence with the highest sentence_id is the most severe
    """

    def _sort_key(
        sentence: NormalizedStateSentence,
    ) -> tuple[int, int, int, int, int, int]:
        # TODO(#39589): add more granular priority levels here to cover more sentence types
        sentence_type_priority = (
            0 if sentence.sentence_type == StateSentenceType.STATE_PRISON else -1
        )
        life_sentence_priority = 0 if sentence.is_life else -1
        controlling_charges_count = sum(
            (charge.is_controlling or False) for charge in sentence.charges
        )
        violent_or_sex_offense_charges = [
            charge
            for charge in sentence.charges
            if charge.is_violent or charge.is_sex_offense
        ]
        violent_or_sex_offense_priority = (
            0 if len(violent_or_sex_offense_charges) > 0 else -1
        )
        first_sentence_length = first(
            sorted(sentence.sentence_lengths, key=lambda sl: sl.length_update_datetime),
            default=None,
        )
        # Return -1 for the length priority value if there is no sentence length data for this sentence
        if not first_sentence_length:
            return (
                sentence_type_priority,
                life_sentence_priority,
                controlling_charges_count,
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
            sentence_type_priority,
            life_sentence_priority,
            controlling_charges_count,
            assert_type(max_length_days or min_length_days or -1, int),
            violent_or_sex_offense_priority,
            sentence.sentence_id,
        )

    return list(sorted(sentences, key=_sort_key, reverse=True))


def sentences_have_contiguous_or_overlapping_serving(
    s1: NormalizedStateSentence, s2: NormalizedStateSentence
) -> bool:
    """
    Returns True if the two given sentences either:
      - Have an overlapping span of time where they are both serving.
      - Have contiguous serving spans, meaning they are served one after the other
        within a day

    This is for the purpose of identifying sentences we intend to infer together
    into a NormalizedStateSentenceInferredGroup.
    """
    span1 = s1.first_serving_or_pending_status_to_terminating_status_dt_range
    span2 = s2.first_serving_or_pending_status_to_terminating_status_dt_range
    if not (span1 and span2):
        return False
    if span1.lower_bound_inclusive in span2 or span2.lower_bound_inclusive in span1:
        return True
    if span1.upper_bound_exclusive == span2.lower_bound_inclusive:
        return True
    if span2.upper_bound_exclusive == span1.lower_bound_inclusive:
        return True
    return False


# pylint: disable=unused-argument
class StateSpecificSentenceNormalizationDelegate(StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalizing sentences
    for calculations."""

    def __init__(self) -> None:
        if (
            self.allow_non_credit_serving
            and self.correct_imposed_pending_serving_statuses
        ):
            raise ValueError(
                "Both allow_non_credit_serving and correct_imposed_pending_serving_statuses return True. "
                "We can only have one of these properties return True (if a state is providing "
                "explicit serving start dates, then we should be using them). "
                "If this is too strict, please ping #platform-team"
            )

    @property
    def override_projected_completion_dates_using_sentence_length_days(self) -> bool:
        """
        If True, we'll calculate projected_completion_date_min/max_external
        from <the serving start date> + <sentence_length_days_min/max>

        Note if your state provides completion dates and this is True,
        we will override that data!
        """
        return False

    @property
    def correct_imposed_pending_serving_statuses(self) -> bool:
        """
        If True, SERVING statuses for a sentence that are before it's
        parents' final terminating statuses will be changed to IMPOSED_PENDING_SERVING.
        We will also create an initial SERVING status if one does not exist at
        that datetime.

        The delegate will fail if this property is True and allow_non_credit_serving
        is also True (if a state is providing explicit serving start dates, then
        we should be using them). If this is too strict, please ping #platform-team
        """
        return False

    def update_sentence_status_snapshot(
        self,
        sentence: "StateSentence",
        snapshot: "StateSentenceStatusSnapshot",
    ) -> "StateSentenceStatusSnapshot | None":
        """Contains state-specific logic for filtering or updating sentence status snapshots.

        Called during status snapshot normalization for each snapshot.

        Args:
            sentence: The sentence this snapshot belongs to
            snapshot: The specific status snapshot being processed

        Returns:
            - The snapshot (unchanged) to keep it
            - A modified snapshot to update it
            - None to filter it out

        By default, returns the snapshot unchanged."""
        return snapshot

    @property
    def correct_early_completed_statuses(self) -> bool:
        """
        If True, if we see a StateSentenceStatusSnapshot that is not the last status for a sentence which
        has status COMPLETED, correct that status to SERVING. Otherwise, we'll throw if we see a COMPLETED
        status that is followed by other statuses.
        """
        return False

    @property
    def infer_imposed_pending_serving_from_imposed_date(self) -> bool:
        """
        If True, when a sentence has an imposed_date and either:
          - no current_state_provided_start_date, or
          - an imposed_date that is before the current_state_provided_start_date,
        we will create an IMPOSED_PENDING_SERVING status snapshot at the imposed_date,
        unless the sentence already has a serving or terminating status at or before
        that date.

        This captures the period between when a sentence was imposed and when it
        began being actively served. For sentences without an effective date, this
        ensures they are still represented in status snapshots.
        """
        return False

    @property
    def correct_early_terminating_statuses(self) -> bool:
        """
        If True, any non-DEATH terminating status (VACATED, DISCHARGED, MERGED,
        COMMUTED, etc.) that appears before the final status in a sentence's
        status sequence will be corrected to SERVING. This handles states where
        legitimate sequences like DISCHARGED -> VACATED occur. DEATH in a
        non-final position is always an error regardless of this setting.
        """
        return False

    @property
    def allow_non_credit_serving(self) -> bool:
        """
        If True, statuses that count towards serving that are before their sentence's
        current_state_provided_start_date will be switched to NON_CREDIT_SERVING.
        Only return True in a state where we know that serving credit can be removed.
        """
        return False

    def should_carry_forward_projected_dates(self, sentence: "StateSentence") -> bool:
        """If True, when normalizing this sentence's StateSentenceLength snapshots,
        NULL projected_completion_date_min/max_external and
        parole_eligibility_date_external values are replaced with the most recent
        prior non-null value (carry-forward).

        Default returns False (no carry-forward). State delegates can override to
        enable carry-forward for specific sentences whose raw data is known to drop
        projected dates after the date passes but where the sentence is still
        relevant for the person's current legal status.
        """
        return False

    def close_serving_period_at_passed_projected_date(
        self, sentence: "StateSentence"
    ) -> bool:
        """If True for this sentence, the framework will inject a synthetic
        COMPLETED sentence_status_snapshot at the latest
        projected_completion_date_max_external when that date is in the past
        and the sentence has no existing terminating status snapshot at or
        before that date.

        Default returns False. State delegates can override per-sentence
        based on data quality patterns (e.g. raw projected dates that aren't
        updated after the date passes, leaving the sentence's serving_period
        open indefinitely without a COMPLETED snapshot).
        """
        return False

    @staticmethod
    def sentences_are_in_same_imposed_group(
        s1: NormalizedStateSentence, s2: NormalizedStateSentence
    ) -> bool:
        """Returns True if the two given sentences are imposed together."""
        return (s1.imposed_date == s2.imposed_date) and (
            s1.sentencing_authority == s2.sentencing_authority
        )

    @staticmethod
    def get_most_severe_charge(
        sentences: list[NormalizedStateSentence],
    ) -> NormalizedStateChargeV2:
        """
        Returns the most severe charge for this group of sentences.
        By default, this will return the controlling charge on the most severe sentence.
        """
        most_severe_sentence = first(_sort_sentences_by_severity(sentences))
        # Pick a controlling charge first, then a violent or sex offense charge,
        # and then if all else is the same arbitrarily pick the highest charge ID
        charges = list(
            sorted(
                most_severe_sentence.charges,
                key=lambda c: (
                    c.is_controlling or False,
                    c.is_violent or c.is_sex_offense or False,
                    c.charge_v2_id,
                ),
                reverse=True,
            )
        )
        return charges[0]

    @staticmethod
    def sentences_are_in_same_inferred_group(
        s1: NormalizedStateSentence, s2: NormalizedStateSentence
    ) -> bool:
        """
        Returns True if the two given sentences belong in the same
        NormalizedStateSentenceInferredGroup.
        An inferred group is created when two sentences:
            - Have the same NormalizedStateSentenceGroup
            - Have the same imposed_date
            - Have a common charge
            - Have charges with a common offense_date
            - Have an overlapping span of time between the first SERVING
              status and terminating status.
        """
        # Sentences have the same state provided sentence group
        if s1.sentence_group_external_id == s2.sentence_group_external_id and (
            s1.sentence_group_external_id and s2.sentence_group_external_id
        ):
            return True
        # Sentences have the same imposed date
        if (s1.imposed_date == s2.imposed_date) and (
            s1.imposed_date and s2.imposed_date
        ):
            return True
        # Sentences have a common charge
        if {c.charge_v2_id for c in s1.charges}.intersection(
            {c.charge_v2_id for c in s2.charges}
        ):
            return True
        # Sentences have a common offense date
        if {c.offense_date for c in s1.charges if c.offense_date}.intersection(
            {c.offense_date for c in s2.charges if c.offense_date}
        ):
            return True
        return sentences_have_contiguous_or_overlapping_serving(s1, s2)

    def get_sentence_type_override(
        self,
        sentence_type: StateSentenceType,
        normalized_snapshots: list[NormalizedStateSentenceStatusSnapshot],
    ) -> StateSentenceType | None:
        """If necessary, returns a sentence_type override for this sentence which should
        be set on the normalized version of this sentence. This may be useful in cases
        where we learn info about the sentence type based on the types of statuses we
        see for this sentence.
        """
        return None
