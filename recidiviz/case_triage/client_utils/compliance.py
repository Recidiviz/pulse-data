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
"""Utilities for computing client-related compliance properties."""

import logging
from datetime import date, timedelta
from typing import Literal, Optional, Tuple

import numpy as np

# TODO(#5768): Remove some of these imports once we've figured out our preferred contact method.
# TODO(#5769): Remove the rest of these imports when we've moved nextAssessmentDate to the calc pipeline.
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS,
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
    NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
    REASSESSMENT_DEADLINE_DAYS,
    SEX_OFFENSE_LSIR_MINIMUM_SCORE,
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
    US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS,
)
from recidiviz.case_triage.exceptions import CaseTriageInvalidStateException
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient


def get_next_assessment_date(client: ETLClient) -> Optional[date]:
    """Calculates the next assessment date for the given case."""

    # TODO(#5769): Eventually move this calculation to our calculate pipeline.
    # In the meantime, we're hard-coding the relation to US_ID as a quick stop gap
    if client.state_code != "US_ID":
        raise CaseTriageInvalidStateException(client.state_code)

    if client.most_recent_assessment_date is None or client.assessment_score is None:
        if client.supervision_start_date is None:
            # We expect that supervision_start_date is filled in, but in instances where
            # our default calc pipeline look back period is shorter than the amount of time
            # someone has been on supervision, it will be empty.
            #
            # We log the warning, but still want to fail gracefully.
            logging.warning(
                "Supervision start date unexpectedly empty for client with id %s in state %s",
                client.person_external_id,
                client.state_code,
            )
            return None
        return client.supervision_start_date + timedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
    if client.case_type == StateSupervisionCaseType.SEX_OFFENSE.value:
        if client.assessment_score < SEX_OFFENSE_LSIR_MINIMUM_SCORE.get(
            Gender(client.gender), 0
        ):
            return None
    if StateSupervisionLevel(client.supervision_level) == StateSupervisionLevel.MINIMUM:
        return None
    return client.most_recent_assessment_date + timedelta(
        days=REASSESSMENT_DEADLINE_DAYS
    )


def get_next_face_to_face_date(client: ETLClient) -> Optional[date]:
    """Calculates the next face-to-face contact date. It returns None if no
    future face-to-face contact is required."""

    # TODO(#5769): Eventually move this calculation to our calculate pipeline.
    # In the meantime, we're hard-coding the relation to US_ID as a quick stop gap
    if client.state_code != "US_ID":
        raise CaseTriageInvalidStateException(client.state_code)

    case_type = StateSupervisionCaseType(client.case_type)
    supervision_level = StateSupervisionLevel(client.supervision_level)
    if (
        case_type not in SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS
        or supervision_level
        not in SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[case_type]
    ):
        logging.warning(
            "Could not find requirements for case type %s, supervision level %s",
            client.case_type,
            client.supervision_level,
        )
        return None

    if client.most_recent_face_to_face_date is None:
        return np.busday_offset(
            client.supervision_start_date,
            NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
            roll="forward",
        ).astype(date)

    face_to_face_requirements = SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[case_type][
        supervision_level
    ]
    if face_to_face_requirements[0] == 0:
        return None
    return client.most_recent_face_to_face_date + timedelta(
        days=(face_to_face_requirements[1] // face_to_face_requirements[0])
    )


def get_next_home_visit_date(client: ETLClient) -> Optional[date]:
    """Calculates the next home visit contact date. It returns None if no
    future home visit contact is required."""

    # TODO(#5769): Eventually move this calculation to our calculate pipeline.
    # In the meantime, we're hard-coding the relation to US_ID as a quick stop gap
    if client.state_code != "US_ID":
        raise CaseTriageInvalidStateException(client.state_code)

    if client.most_recent_home_visit_date is None:
        return client.supervision_start_date + timedelta(
            days=NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS
        )

    supervision_level = StateSupervisionLevel(client.supervision_level)
    if supervision_level not in US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS:
        logging.warning(
            "Could not find requirements for supervision level %s",
            client.supervision_level,
        )
        return None

    home_visit_requirements = US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS[
        supervision_level
    ]
    if home_visit_requirements[0] == 0:
        return None
    return client.most_recent_home_visit_date + timedelta(
        days=(home_visit_requirements[1] // home_visit_requirements[0])
    )


DueDateStatus = Literal["OVERDUE", "UPCOMING"]


def _get_due_date_status(
    due_date: date, upcoming_threshold_days: int
) -> Optional[DueDateStatus]:
    today = date.today()
    if due_date < today:
        return "OVERDUE"
    if due_date <= today + timedelta(days=upcoming_threshold_days):
        return "UPCOMING"

    return None


def _get_days_until_due(due_date: date) -> int:
    return (due_date - date.today()).days


def get_assessment_due_details(
    client: ETLClient,
) -> Optional[Tuple[DueDateStatus, int]]:
    """Outputs whether client's assessment is upcoming or overdue relative to the current date."""
    due_date = get_next_assessment_date(client)
    if due_date:
        status = _get_due_date_status(due_date, 30)
        if status:
            return (status, _get_days_until_due(due_date))
    return None


def get_contact_due_details(client: ETLClient) -> Optional[Tuple[DueDateStatus, int]]:
    """Outputs whether contact with client is upcoming or overdue relative to the current date."""

    # TODO(#7320): Our current `nextHomeVisitDate` determines when the next home visit
    # should be assuming that home visits must be F2F visits and not collateral visits.
    # As a result, until we do more investigation into what the appropriate application
    # of state policy is, we're not showing home visits as the next contact dates.
    due_date = get_next_face_to_face_date(client)
    if due_date:
        status = _get_due_date_status(due_date, 7)
        if status:
            return (status, _get_days_until_due(due_date))
    return None
