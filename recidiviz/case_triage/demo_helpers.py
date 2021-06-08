# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements some querying abstractions for use by demo users."""
import json
import os
from datetime import datetime
from typing import List

import pytz

from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOpportunity,
)

DEMO_FROZEN_DATETIME = datetime(
    year=2021,
    month=3,
    day=9,
    tzinfo=pytz.UTC,
)
DEMO_FROZEN_DATE = DEMO_FROZEN_DATETIME.date()

# This is what we've been using for all demo data
DEMO_STATE_CODE = "US_ID"

_FIXTURE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./fixtures/",
    )
)


def get_fixture_clients() -> List[ETLClient]:
    with open(os.path.join(_FIXTURE_PATH, "demo_clients.json")) as f:
        clients = json.load(f)
    return [ETLClient.from_json(client) for client in clients]


def get_fixture_opportunities() -> List[ETLOpportunity]:
    with open(os.path.join(_FIXTURE_PATH, "demo_opportunities.json")) as f:
        clients = json.load(f)
    return [ETLOpportunity.from_json(client) for client in clients]


def fake_officer_id_for_demo_user(user_email_address: str) -> str:
    return f"demo::{user_email_address}"


def fake_person_id_for_demo_user(
    user_email_address: str, original_person_external_id: str
) -> str:
    return f"demo::{user_email_address}::{original_person_external_id}"


def unconvert_fake_person_id_for_demo_user(demo_person_id: str) -> str:
    DELIMITER = "::"
    if DELIMITER not in demo_person_id:
        return demo_person_id
    return demo_person_id[demo_person_id.rfind(DELIMITER) + len(DELIMITER) :]
