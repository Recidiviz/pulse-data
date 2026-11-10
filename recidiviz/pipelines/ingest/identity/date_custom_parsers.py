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
"""Custom parsers for identity mappings that validate date fields.

Identity mappings can only reference functions defined in *_custom_parsers
modules under recidiviz.pipelines.ingest.identity, so this module re-exports
reasonable_birthdate_or_null from its home next to the birthdate validator it
applies. Every tenant's birthdate mapping should route through it; see the
function's docstring for why.
"""
from recidiviz.persistence.entity.reasonable_date_validators import (  # pylint: disable=unused-import
    reasonable_birthdate_or_null,
)
