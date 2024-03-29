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
"""Types used by the asset-generation-service integration."""
from typing import Any, Dict

import attr
import cattrs


@attr.s
class PayloadBase:
    """Includes common fields for asset generation payloads.
    It is expected, though not required, that instances will add their own
    data property, which will necessarily be specific to the asset type."""

    stateCode: str = attr.ib()
    id: str = attr.ib()
    width: int = attr.ib()

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s
class AssetResponseBase:
    """The standard response for a generated asset, which simply provides
    the path by which the asset can be retrieved from the asset server."""

    url: str = attr.ib()
