# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Factory class for building DirectIngestControllers of various types."""
import importlib
from typing import Optional

from gcsfs import GCSFileSystem

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import (
    GcsfsDirectIngestController,
)


class DirectIngestControllerFactory:
    """Factory class for building DirectIngestControllers of various types."""

    _DIRECT_INGEST_REGIONS_MODULE_NAME = "recidiviz.ingest.direct.regions"

    @classmethod
    def build_gcsfs_ingest_controller(
        cls,
        region_code: str,
        fs: GCSFileSystem,
    ) -> Optional[GcsfsDirectIngestController]:
        """Retrieve a direct ingest GcsfsDirectIngestController for a particular
        region.

        Returns:
            An instance of the region's direct ingest controller class (e.g.,
             UsNdController)
        """
        controller_module_name = (
            f"{cls._DIRECT_INGEST_REGIONS_MODULE_NAME}."
            f"{region_code}.{region_code}_controller"
        )

        try:
            controller_module = importlib.import_module(controller_module_name)
        except ModuleNotFoundError:
            return None

        controller_class = getattr(
            controller_module, cls.gcsfs_controller_class_name(region_code), None
        )

        if not controller_class:
            return None

        controller = controller_class(fs)
        if not isinstance(controller, GcsfsDirectIngestController):
            return None

        return controller

    @classmethod
    def gcsfs_controller_class_name(cls, region_code: str) -> str:
        """Returns the GcsfsDirectIngestController class name for a given
        region_code.
        """
        return "".join(s.title() for s in region_code.split("_")) + "Controller"
