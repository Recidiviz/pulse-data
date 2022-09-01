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
from types import ModuleType
from typing import Optional, Type

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
    check_is_region_launched_in_env,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.utils import metadata


class DirectIngestControllerFactory:
    """Factory class for building DirectIngestControllers of various types."""

    @classmethod
    def build(
        cls,
        *,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        allow_unlaunched: bool,
        region_module_override: Optional[ModuleType] = None,
    ) -> BaseDirectIngestController:
        """Retrieve a direct ingest BaseDirectIngestController associated with a
        particular ingest instance.

        Returns:
            An instance of the region's direct ingest controller class (e.g.,
             UsNdController) that can run ingest operations for the ingest instance
             specified.
        """
        if (
            region_code is None
            or not StateCode.is_state_code(region_code)
            or StateCode.get(region_code)
            not in get_direct_ingest_states_existing_in_env()
        ):
            raise DirectIngestError(
                msg=f"Unsupported direct ingest region [{region_code}] in "
                f"project [{metadata.project_id()}]",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=region_code.lower(),
            region_module_override=region_module_override,
        )
        if not allow_unlaunched:
            check_is_region_launched_in_env(region)

        controller_class = cls.get_controller_class(region)
        controller = controller_class(ingest_instance=ingest_instance)
        if not isinstance(controller, BaseDirectIngestController):
            raise ValueError(f"Unexpected controller class type [{type(controller)}]")

        return controller

    @classmethod
    def get_controller_class(
        cls, region: DirectIngestRegion
    ) -> Type[BaseDirectIngestController]:
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
