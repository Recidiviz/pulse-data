# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Validation interface for Workflows-related external requests from Twilio"""
import logging
from typing import Dict

from twilio.request_validator import RequestValidator

from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.secrets import get_secret


class WorkflowsTwilioValidator:
    """Validation interface for Workflows-related external requests from Twilio"""

    def validate(self, url: str, signature: str, params: Dict[str, str]) -> None:
        validation_secret = get_secret("twilio_auth_token")

        # Initialize the request validator
        validator = RequestValidator(validation_secret)

        logging.info(
            "Workflows Twilio Validator for url: [%s], params: [%s], signature: [%s]",
            url,
            params,
            signature,
        )

        # Check if the incoming signature is valid for the application URL and the incoming parameters
        validation_result = validator.validate(url, params, signature)

        logging.info(
            "Workflows Twilio Validator validation result: [%s]", validation_result
        )

        if not validation_result:
            raise AuthorizationError(
                code="not_authorized",
                description="Twilio status request not authorized",
            )
