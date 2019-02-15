# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"Utils to be shared across recidiviz project"
import string
from typing import Optional

_GENERATED_ID_SUFFIX = "_GENERATE"


def create_generated_id(obj) -> str:
    return str(id(obj)) + _GENERATED_ID_SUFFIX


def is_generated_id(id_str: Optional[str]) -> bool:
    return id_str is not None and id_str.endswith(_GENERATED_ID_SUFFIX)


def normalize(s: str, remove_punctuation: bool = False) -> str:
    """Normalizes whitespace within the provided string by converting all groups
    of whitespaces into ' ', and uppercases the string."""
    if remove_punctuation:
        translation = str.maketrans(dict.fromkeys(string.punctuation, ' '))
        label_without_punctuation = s.translate(translation)
        if not label_without_punctuation.isspace():
            s = label_without_punctuation

    if s is None or s == '' or s.isspace():
        raise ValueError('Cannot normalize None or empty/whitespace string')
    return ' '.join(s.split()).upper()
