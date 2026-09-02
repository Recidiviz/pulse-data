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
"""Contains US_ND implementation of the StateSpecificSentenceNormalizationDelegate."""
from recidiviz.pipelines.ingest.activity.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class UsNdSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_ND implementation of the StateSpecificSentenceNormalizationDelegate.

    Handles sentences that have been imposed but are not yet being served. ND
    imposes a collection of sentences at once that are to be served
    consecutively - every sentence shares an imposed date, and each one's
    serving start is the previous one's projected end. The Elite status
    snapshot ingest view only emits a snapshot once a sentence's serving start
    has arrived, so the ones not yet being served have no status snapshots at
    all, no serving period, and their projected dates never reach person-level
    views.

    Injecting IMPOSED_PENDING_SERVING at the imposed date makes those sentences
    visible from the date they were imposed, so their projected dates count
    toward the person's projected release while `is_imposed_pending_serving`
    keeps them distinguishable from time actually being served.
    """

    @property
    def infer_imposed_pending_serving_from_imposed_date(self) -> bool:
        return True
