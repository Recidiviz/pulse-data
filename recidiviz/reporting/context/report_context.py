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

"""Abstract base class that encapsulates report-specific context."""

import os
from abc import ABC, abstractmethod
from typing import List

from jinja2 import Template

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.reporting.recipient import Recipient


class ReportContext(ABC):
    """Defines the context for generation and delivery of a single email report to a single recipient,
    for a particular report type."""

    def __init__(self, state_code: str, recipient: Recipient):
        self.state_code = state_code
        self.recipient = recipient
        self.prepared_data: dict = {}

        self.properties: dict = {}

        self._validate_recipient_has_expected_fields(recipient)

    @abstractmethod
    def get_required_recipient_data_fields(self) -> List[str]:
        """Specifies keys that must exist within `recipient` in order for the class to be instantiated"""

    def _validate_recipient_has_expected_fields(self, recipient: Recipient) -> None:
        missing_keys = [
            expected_key
            for expected_key in self.get_required_recipient_data_fields()
            if expected_key not in recipient.data.keys()
        ]

        if missing_keys:
            raise KeyError(
                f"Missing key(s) [{missing_keys}] not found in recipient.", recipient
            )

    @abstractmethod
    def get_report_type(self) -> str:
        """Returns the report type for this report."""

    @abstractmethod
    def get_properties_filepath(self) -> str:
        """Returns the filepath to the context's properties.json file"""

    @property
    @abstractmethod
    def html_template(self) -> Template:
        """Returns the context's html template"""

    def get_batch_id(self) -> str:
        """Returns the batch_id for the report context"""
        return self.recipient.data[utils.KEY_BATCH_ID]

    def get_email_address(self) -> str:
        """Returns the email_address to use to generate the filenames for email delivery."""
        return self.recipient.email_address

    def get_prepared_data(self) -> dict:
        """Execute report-specific rules that process the recipient data before templating, returning the prepared,
        report-ready template values.
        This is guaranteed to return the prepared data at any point in the instance's lifecycle. If the data has never
        been prepared before, self.prepare_for_generation will be called and its value set on this instance. If it has
        been called before, its value will be returned straight-away.
        """
        if self.prepared_data:
            return self.prepared_data

        return self.prepare_for_generation()

    def render_html(self) -> str:
        """Interpolates the report's prepared data into the template
        Returns: Interpolated template"""
        prepared_data = self.get_prepared_data()
        return self.html_template.render(prepared_data)

    @abstractmethod
    def prepare_for_generation(self) -> dict:
        """Execute report-specific rules that process the recipient data before templating, returning the prepared,
        report-ready template values.
        This will set self.prepared_data on the instance upon completion. It can be called at any time to reset and
        rebuild this instance's prepared data, i.e. it executes regardless of whether it has been invoked previously.
        NOTE: Implementors of this function must set self.prepared_data to the final results of invocation.
        """

    def _get_context_templates_folder(self) -> str:
        return os.path.join(os.path.dirname(__file__), "templates")
