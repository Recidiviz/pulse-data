# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared type aliases and enums for the Linear issue-tracking integration."""

import enum

# A Linear team's key/prefix, e.g. "OBT" — matches LinearIssue.team_prefix.
LinearTeamKey = str


class LinkKind(enum.Enum):
    """The relationship between a PR attachment and a Linear issue.

    Linear stores this as a free-form string in attachment metadata (not a
    schema-enforced enum), but only two values are used by their GitHub
    integration: "closes" and "contributes".
    See https://linear.app/docs/github#link-issues-with-pull-requests
    """

    CLOSES = "closes"
    CONTRIBUTES = "contributes"

    @classmethod
    def from_raw_value(cls, raw_value: str | None) -> "LinkKind | None":
        """Returns the LinkKind for a raw Linear linkKind string, or None if the
        value is absent or is not one of the recognized kinds.

        Because Linear stores linkKind as a free-form string (see class docstring),
        an attachment can carry a value outside the two kinds set by their GitHub
        integration: attachments linked by hand carry no linkKind at all, and
        Linear has been observed to return other values (e.g. "links"). Callers
        only care about "closes"/"contributes", so any unrecognized value maps to
        None rather than raising."""
        if raw_value is None:
            return None
        try:
            return cls(raw_value)
        except ValueError:
            return None


class LinearStateType(enum.Enum):
    """The type of a Linear workflow state.

    Each team defines its own named workflow states (each with a unique UUID),
    but every state is one of these fixed types. Linear stores the type as a
    lowercase string (note "canceled" has a single 'l').
    See https://linear.app/docs/configuring-workflows
    """

    TRIAGE = "triage"
    BACKLOG = "backlog"
    UNSTARTED = "unstarted"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELED = "canceled"
