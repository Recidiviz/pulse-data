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

from recidiviz.cloud_functions.direct_ingest_bucket_name_utils import (
    get_region_code_from_direct_ingest_bucket,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    check_is_region_launched_in_env,
)
from recidiviz.ingest.direct.errors import DirectIngestError, DirectIngestErrorType
from recidiviz.utils import metadata, regions
from recidiviz.utils.regions import (
    Region,
    get_supported_direct_ingest_region_codes,
)


class DirectIngestControllerFactory:
    """Factory class for building DirectIngestControllers of various types."""

    @classmethod
    def build(
        cls, *, ingest_bucket_path: GcsfsBucketPath, allow_unlaunched: bool
    ) -> BaseDirectIngestController:
        """Retrieve a direct ingest GcsfsDirectIngestController associated with a
        particular ingest bucket.

        Returns:
            An instance of the region's direct ingest controller class (e.g.,
             UsNdController) that can run ingest operations for the ingest instance
             associated with the input bucket.
        """
        region_code = get_region_code_from_direct_ingest_bucket(
            ingest_bucket_path.bucket_name
        )

        if (
            region_code is None
            or region_code not in get_supported_direct_ingest_region_codes()
        ):
            raise DirectIngestError(
                msg=f"Unsupported direct ingest region [{region_code}] in "
                f"project [{metadata.project_id()}]",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        region = cls._region_for_bucket(ingest_bucket_path)
        if not allow_unlaunched and not region.is_ingest_launched_in_env():
            check_is_region_launched_in_env(region)

        controller_class = cls.get_controller_class(region)
        controller = controller_class(ingest_bucket_path=ingest_bucket_path)
        if not isinstance(controller, BaseDirectIngestController):
            raise ValueError(f"Unexpected controller class type [{type(controller)}]")

        return controller

    @staticmethod
    def _region_for_bucket(ingest_bucket_path: GcsfsBucketPath) -> Region:
        region_code = get_region_code_from_direct_ingest_bucket(
            ingest_bucket_path.bucket_name
        )
        if not region_code:
            raise ValueError(
                f"Found no region code for bucket [{ingest_bucket_path.uri()}]"
            )
        return regions.get_region(
            region_code=region_code.lower(),
            is_direct_ingest=True,
        )

    @classmethod
    def get_controller_class(cls, region: Region) -> Type[BaseDirectIngestController]:
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

        if not issubclass(controller_class, BaseDirectIngestController):
            raise ValueError(f"Unexpected controller class type [{controller_class}]")

        return controller_class

    @classmethod
    def get_controller_class_name(cls, region_code: str) -> str:
        """Returns the BaseDirectIngestController class name for a given
        region_code.
        """
        return "".join(s.title() for s in region_code.split("_")) + "Controller"
