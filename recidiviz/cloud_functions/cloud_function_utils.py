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

"""This file contains all of the relevant helpers for cloud functions.

Mostly copied from:
https://cloud.google.com/iap/docs/authentication-howto#iap_make_request-python
"""
import json

GCP_PROJECT_ID_KEY = "GCP_PROJECT"


def cloud_functions_log(severity: str, message: str) -> None:
    # TODO(https://issuetracker.google.com/issues/124403972?pli=1): Workaround until
    #  built-in logging is fixed for Python cloud functions
    # Complete a structured log entry.
    entry = {
        "severity": severity,
        "message": message,
        # Log viewer accesses 'component' as jsonPayload.component'.
        "component": "arbitrary-property",
    }

    print(json.dumps(entry))
