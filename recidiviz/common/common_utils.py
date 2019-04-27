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
import logging
import random
import time
import uuid
from typing import Optional

import flask
from google.api_core import exceptions  # pylint: disable=no-name-in-module

from recidiviz.utils import environment

GENERATED_ID_SUFFIX = "_GENERATE"
RETRY_SLEEP = 30


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


def retry_grpc(num_retries, fn, *args, **kwargs):
    """Retries a function call some number of times"""
    time_to_sleep = random.uniform(5, RETRY_SLEEP)
    for i in range(num_retries + 1):
        try:
            return fn(*args, **kwargs)
        except exceptions.InternalServerError as e:
            if i == num_retries:
                raise
            if 'GOAWAY' in str(e) or 'Deadline Exceeded' in str(e):
                logging.exception('Received exception: ')
                if environment.in_gae():
                    logging.warning('Sleeping %.2f seconds and retrying',
                                    time_to_sleep)
                    time.sleep(time_to_sleep)
            else:
                raise
