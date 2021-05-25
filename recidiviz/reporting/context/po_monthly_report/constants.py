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
"""Include constants for use by PO Monthly Reports."""

DEFAULT_EMAIL_SUBJECT = "Your monthly Recidiviz report"
DEFAULT_MESSAGE_BODY = (
    "We’re here to make your life a bit easier by helping you keep track of "
    + "your caseload's health. You'll receive this email once a month as a customized, personal check-in just for you."
)

OFFICER_EXTERNAL_ID = "officer_external_id"
STATE_CODE = "state_code"
DISTRICT = "district"
EMAIL_ADDRESS = "email_address"
OFFICER_GIVEN_NAME = "officer_given_name"
REVIEW_MONTH = "review_month"

"""Below are metrics constants used by the PO Monthly Reports"""
POS_DISCHARGES = "pos_discharges"
EARNED_DISCHARGES = "earned_discharges"
SUPERVISION_DOWNGRADES = "supervision_downgrades"
REVOCATIONS_CLIENTS = "revocations_clients"
TOTAL_REVOCATIONS = "total_revocations"
TECHNICAL_REVOCATIONS = "technical_revocations"
CRIME_REVOCATIONS = "crime_revocations"
ABSCONSIONS = "absconsions"
ASSESSMENTS = "assessments"
FACE_TO_FACE = "facetoface"
