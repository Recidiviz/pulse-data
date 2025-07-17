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
"""Contains US_TN implementation of the StateSpecificSentenceNormalizationDelegate."""
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class UsTnSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_TN implementation of the StateSpecificSentenceNormalizationDelegate."""

    @property
    def allow_non_credit_serving(self) -> bool:
        """TN has an 'oversight board' that revokes credit for time served, so we allow non-credit serving sentences."""
        return True

    # TODO(#28869) understand why TN gives us data like this (a sentence really changes,
    # there's an acute data issue, or the originating process is flawed)
    @property
    def correct_early_completed_statuses(self) -> bool:
        """
        If True, if we see a StateSentenceStatusSnapshot that is not the last status for a sentence which
        has status COMPLETED, correct that status to SERVING. Otherwise, we'll throw if we see a COMPLETED
        status that is followed by other statuses.
        """
        return True
