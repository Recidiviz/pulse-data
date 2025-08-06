# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines an enum with all valid units of observation for metrics."""
from enum import Enum


class MetricUnitOfObservationType(Enum):
    """A unit of observation is the item (or items) that you observe, measure, or
    collect while trying to learn something about your unit of analysis.

    The MetricUnitOfObservationType is a type that tells us what each input event / span
    to a metric is about. For example, compartment_sessions rows are each about a single
    person, so the MetricUnitOfObservationType is PERSON.
    """

    INSIGHTS_PRIMARY_USER = "INSIGHTS_PRIMARY_USER"
    INSIGHTS_PROVISIONED_USER = "INSIGHTS_PROVISIONED_USER"
    SUPERVISION_OFFICER = "OFFICER"
    PERSON_ID = "PERSON"
    TASKS_PRIMARY_USER = "TASKS_PRIMARY_USER"
    TASKS_PROVISIONED_USER = "TASKS_PROVISIONED_USER"
    WORKFLOWS_PRIMARY_USER = "WORKFLOWS_PRIMARY_USER"
    WORKFLOWS_PROVISIONED_USER = "WORKFLOWS_PROVISIONED_USER"
    WORKFLOWS_SURFACEABLE_CASELOAD = "WORKFLOWS_SURFACEABLE_CASELOAD"
    GLOBAL_PROVISIONED_USER = "GLOBAL_PROVISIONED_USER"

    @property
    def short_name(self) -> str:
        """Returns lowercase enum name"""
        return self.value.lower()
