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
"""Factory class for building IngestRawFileImportControllers."""
from types import ModuleType
from typing import Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.controllers.ingest_raw_file_import_controller import (
    IngestRawFileImportController,
    check_is_region_launched_in_env,
)
from recidiviz.ingest.direct.gating import is_raw_data_import_dag_enabled
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
    DirectIngestGatingError,
)
from recidiviz.utils import metadata


class IngestRawFileImportControllerFactory:
    """Factory class for building IngestRawFileImportControllers."""

    @classmethod
    def build(
        cls,
        *,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        allow_unlaunched: bool,
        region_module_override: Optional[ModuleType] = None,
    ) -> IngestRawFileImportController:
        """Retrieve a IngestRawFileImportController associated with a particular region and ingest instance."""
        if (
            not StateCode.is_state_code(region_code.upper())
            or (state_code := StateCode(region_code.upper()))
            not in get_direct_ingest_states_existing_in_env()
        ):
            raise DirectIngestError(
                msg=f"Unsupported direct ingest region [{region_code.upper()}] in "
                f"project [{metadata.project_id()}]",
                error_type=DirectIngestErrorType.INPUT_ERROR,
            )

        if is_raw_data_import_dag_enabled(state_code, ingest_instance):
            raise DirectIngestGatingError(
                f"IngestRawFileImportControllerFactory for region [{state_code.value}] and instance [{ingest_instance.value}] should not need to be called once raw data import DAG is active"
            )

        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=region_code.lower(),
            region_module_override=region_module_override,
        )
        if not allow_unlaunched:
            check_is_region_launched_in_env(region)

        return IngestRawFileImportController(
            state_code=state_code,
            ingest_instance=ingest_instance,
            region_module_override=region_module_override,
        )
