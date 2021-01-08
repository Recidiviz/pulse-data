# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Data class for report recipients"""
import attr

import recidiviz.reporting.email_reporting_utils as utils


@attr.s
class Recipient:
    email_address: str = attr.ib()
    state_code: str = attr.ib()
    district: str = attr.ib()

    # Includes various fields for report rendering
    data: dict = attr.ib(default=attr.Factory(dict))

    def create_derived_recipient(self, new_data: dict) -> 'Recipient':
        """ Return a new Recipient, derived from this instance """
        extended_data = {**self.data, **new_data}

        return Recipient.from_report_json(extended_data)

    @staticmethod
    def from_report_json(report_json: dict) -> 'Recipient':
        return Recipient(
            email_address=report_json[utils.KEY_EMAIL_ADDRESS],
            state_code=report_json[utils.KEY_STATE_CODE],
            district=report_json[utils.KEY_DISTRICT],
            data=report_json
        )
