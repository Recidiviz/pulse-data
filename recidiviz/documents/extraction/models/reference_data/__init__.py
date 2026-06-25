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
"""Typed models for the curated reference data under
`recidiviz/documents/config/reference_data/`.

Reference data is curated domain knowledge injected into extractor prompts —
known organizations and acronym glossaries, classified using the
organization-type vocabulary (the `OrganizationType` enum). It lives in typed
YAML files, a `shared/` (state-agnostic) and a `{state_code}/` (state-specific)
file per category. These models represent that data independently of any
collection or extractor that consumes it.
"""
