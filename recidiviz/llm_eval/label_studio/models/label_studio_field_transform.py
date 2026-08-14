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
"""How to pull a scalar value out of one Label Studio annotation result object."""
from enum import Enum


class LabelStudioFieldTransform(Enum):
    """How to extract a scalar value from a Label Studio result object."""

    CHOICES_TO_BOOL = "choices_to_bool"
    """choices[0] == 'Yes' → TRUE, anything else → FALSE."""

    CHOICES_SINGLE_SELECT = "choices_single_select"
    """choices[0] as a string. Use for single-select (radio) fields."""

    CHOICES_MULTI_SELECT = "choices_multi_select"
    """All selected choices as a JSON array string. Use for multi-select (checkbox) fields."""

    TEXTAREA_TEXT = "textarea_text"
    """text[0] from a textarea result — the raw string the annotator typed into a
    free-text input box. Use for open-ended notes or comments fields. NULL when
    the annotator left the box empty."""
