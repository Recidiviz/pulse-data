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
"""Class for identity ingest pipeline parameters"""
import json
from typing import Dict, List, Set

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.identity.dataset_config import (
    IDENTITY_INGEST_RESULTS_DATASET,
)
from recidiviz.pipelines.pipeline_parameters import PipelineParameters


@attr.define(kw_only=True)
class IdentityIngestPipelineParameters(PipelineParameters):
    """Class for identity ingest pipeline parameters."""

    raw_data_source_instance: str = attr.ib(
        default=DirectIngestInstance.PRIMARY.value, validator=attr_validators.is_str
    )

    raw_data_upper_bound_dates_json: str = attr.ib(
        validator=attr_validators.is_non_empty_str
    )

    # This maps a raw data table to the max update_datetime from the most recent
    # completed raw data import of that table. This allows us to correctly filter
    # out partial data if the identity ingest pipeline and raw data import are
    # running concurrently.
    raw_data_upper_bound_dates: Dict[str, str] = attr.ib()

    @raw_data_upper_bound_dates.default
    def _raw_data_upper_bound_dates(self) -> Dict[str, str]:
        raw_json = json.loads(self.raw_data_upper_bound_dates_json)
        return dict(raw_json.items())

    @property
    def raw_data_input_dataset(self) -> str:
        try:
            state_code = StateCode(self.tenant)
        except ValueError as e:
            raise ValueError(
                "No support for identity ingest pipeline with non-state tenants. If you "
                "are trying to run this pipeline with a non-state tenant like NYC, you "
                "will need to first add support for non-state raw data datasets."
            ) from e
        return self.get_input_dataset(
            raw_tables_dataset_for_region(
                state_code=state_code,
                instance=DirectIngestInstance(self.raw_data_source_instance.upper()),
            )
        )

    @property
    def clustering_output_dataset(self) -> str:
        return self.get_output_dataset(
            default_dataset_id=IDENTITY_INGEST_RESULTS_DATASET
        )

    @property
    def flex_template_name(self) -> str:
        return "identity_ingest"

    def _get_base_job_name(self) -> str:
        return self._to_job_name_friendly(f"{self.tenant}-identity-ingest")

    @classmethod
    def custom_sandbox_indicator_parameters(cls) -> Set[str]:
        return {"raw_data_source_instance"}

    @classmethod
    def get_input_dataset_property_names(cls) -> List[str]:
        return ["raw_data_input_dataset"]

    @classmethod
    def get_output_dataset_property_names(cls) -> List[str]:
        return ["clustering_output_dataset"]
