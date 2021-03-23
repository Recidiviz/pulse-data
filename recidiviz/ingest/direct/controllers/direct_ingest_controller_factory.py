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
from typing import Type

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import (
    GcsfsDirectIngestController,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    gcsfs_direct_ingest_directory_path_for_region,
    gcsfs_direct_ingest_storage_directory_path_for_region,
)
from recidiviz.utils.regions import Region


class DirectIngestControllerFactory:
    """Factory class for building DirectIngestControllers of various types."""

    @classmethod
    def build(cls, region: Region) -> GcsfsDirectIngestController:
        """Retrieve a direct ingest GcsfsDirectIngestController for a particular
        region.

        Returns:
            An instance of the region's direct ingest controller class (e.g.,
             UsNdController)
        """
        controller_class = cls.get_controller_class(region)
        # TODO(#6077): Allow controllers to be instantiated with specific DB-specific
        #  state (e.g. database name, queue name etc).
        ingest_directory_path = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_directory_path_for_region(
                region.region_code, controller_class.system_level()
            )
        )

        storage_directory_path = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region.region_code, controller_class.system_level()
            )
        )

        controller = controller_class(
            ingest_directory_path=ingest_directory_path,
            storage_directory_path=storage_directory_path,
        )
        if not isinstance(controller, GcsfsDirectIngestController):
            raise ValueError(f"Unexpected controller class type [{type(controller)}]")

        return controller

    @classmethod
    def get_controller_class(cls, region: Region) -> Type[GcsfsDirectIngestController]:
        region_code = region.region_code.lower()
        controller_module_name = (
            f"{region.region_module.__name__}.{region_code}.{region_code}_controller"
        )

        controller_module = importlib.import_module(controller_module_name)

        controller_class_name = cls.get_controller_class_name(region_code)
        controller_class = getattr(controller_module, controller_class_name, None)

        if not controller_class:
            raise ValueError(
                f"Could not find controller class with name [{controller_class_name}]."
            )

        if not issubclass(controller_class, GcsfsDirectIngestController):
            raise ValueError(f"Unexpected controller class type [{controller_class}]")

        return controller_class

    @classmethod
    def get_controller_class_name(cls, region_code: str) -> str:
        """Returns the GcsfsDirectIngestController class name for a given
        region_code.
        """
        return "".join(s.title() for s in region_code.split("_")) + "Controller"
