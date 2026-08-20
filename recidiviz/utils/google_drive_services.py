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
"""Builders for Google Drive, Sheets, and Docs API services from credentials
the caller already holds. Safe to import from production code.

Kept separate from recidiviz.tools.utils.google_drive_helpers, which obtains
credentials interactively for local scripts and depends on the dev-only
google-auth-oauthlib package.
"""
from google.auth.credentials import Credentials
from googleapiclient.discovery import Resource, build


def get_drive_service(creds: Credentials) -> Resource:
    """Returns a Drive API service authorized with |creds|."""
    return build("drive", "v3", credentials=creds)


def get_sheets_service(creds: Credentials) -> Resource:
    """Returns a Sheets API service authorized with |creds|."""
    return build("sheets", "v4", credentials=creds)


def get_docs_service(creds: Credentials) -> Resource:
    """Returns a Docs API service authorized with |creds|."""
    return build("docs", "v1", credentials=creds)
