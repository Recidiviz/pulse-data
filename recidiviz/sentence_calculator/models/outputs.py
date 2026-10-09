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
"""Output data models for sentence calculator."""

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.models.tracked import TrackedValue


@attr.define(frozen=True, kw_only=True)
class GraphEdge:
    """An edge in the sentence DAG."""

    source: str = attr.ib(validator=attr_validators.is_non_empty_str)
    target: str = attr.ib(validator=attr_validators.is_non_empty_str)


@attr.define(frozen=True, kw_only=True)
class DisplayGraph:
    """DAG structure showing sentence relationships.

    This is used for visualizing the sentence graph in the UI."""

    nodes: list[str] = attr.ib(validator=attr_validators.is_list_of(str))
    edges: list[GraphEdge] = attr.ib(validator=attr_validators.is_list_of(GraphEdge))


# ==============================================================================
# TRACKED OUTPUT MODELS (with derivation trees)
# ==============================================================================


@attr.define(frozen=True, kw_only=True)
class TrackedSentenceAdjustment:
    """Sentence adjustment with full derivation trees."""

    sentence_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # MIN date (time to parole eligibility) in days, as ordered by the court
    original_min_days: "TrackedValue[int] | None" = attr.ib(
        validator=attr_validators.is_opt(TrackedValue)
    )
    # MAX date (time to release) in days, as ordered by the court
    original_max_days: "TrackedValue[int] | None" = attr.ib(
        validator=attr_validators.is_opt(TrackedValue)
    )
    # MIN date (time to parole eligibility) in days, adjusted for earned or projected
    # credit, jail time, or other adjustments.
    adjusted_min_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # MAX date (time to release) in days, adjusted for earned or projected credit, jail
    # time, or other adjustments.
    adjusted_max_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # A flat multipler by which sentences might be adjusted (e.g. .65 to represent the
    # 35% off in SB413)
    multiplier: "TrackedValue[float]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # Jail credit to apply to root sentences (those consecutive to no other sentences)
    jail_credit: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )


@attr.define(frozen=True, kw_only=True)
class TrackedLongestSentencePath:
    """Aggregated result with derivation."""

    # Sentence IDs representing the path of 'controlling' sentences
    path_sentence_ids: list[str] = attr.ib(validator=attr_validators.is_list_of(str))
    # Total days to be served for this sentence. Tracked value shows how many days each
    # sentence adds to the total.
    # TODO(NV-9): Validate that we actually need this value to be tracked.
    total_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
