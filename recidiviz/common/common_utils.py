# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
import uuid
from typing import Optional

import flask

GENERATED_ID_SUFFIX = '_GENERATE'


def create_generated_id() -> str:
    return str(uuid.uuid4()) + GENERATED_ID_SUFFIX


def is_generated_id(id_str: Optional[str]) -> bool:
    return id_str is not None and id_str.endswith(GENERATED_ID_SUFFIX)


def get_trace_id_from_flask():
    """Get trace_id from flask request headers.
    """
    if flask is None or not flask.request:
        return None

    header = flask.request.headers.get('X_CLOUD_TRACE_CONTEXT')

    if header is None:
        return None

    trace_id = header.split("/", 1)[0]

    return trace_id


def normalize(s: str, remove_punctuation: bool = False) -> str:
    """Normalizes whitespace within the provided string by converting all groups
    of whitespaces into ' ', and uppercases the string."""
    if remove_punctuation:
        translation = str.maketrans(dict.fromkeys(string.punctuation, ' '))
        label_without_punctuation = s.translate(translation)
        if not label_without_punctuation.isspace():
            s = label_without_punctuation

    if s is None or s == '' or s.isspace():
        raise ValueError("Cannot normalize None or empty/whitespace string")
    return ' '.join(s.split()).upper()
