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

from abc import ABC, abstractmethod


class ReportContext(ABC):
    """Defines the context for generation and delivery of a single email report to a single recipient,
    for a particular report type."""

    def __init__(self, state_code: str, recipient_data: dict):
        self.state_code = state_code
        self.recipient_data = recipient_data
        self.prepared_data: dict = {}

        self.properties: dict = {}

    @abstractmethod
    def get_report_type(self) -> str:
        """Returns the report type for this report."""

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

    @abstractmethod
    def prepare_for_generation(self) -> dict:
        """Execute report-specific rules that process the recipient data before templating, returning the prepared,
        report-ready template values.
        This will set self.prepared_data on the instance upon completion. It can be called at any time to reset and
        rebuild this instance's prepared data, i.e. it executes regardless of whether it has been invoked previously.
        NOTE: Implementors of this function must set self.prepared_data to the final results of invocation.
        """
